package delayd

import (
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/armon/consul-api"
	"github.com/hashicorp/raft"
	"github.com/streadway/amqp"
)

// a generous 60 seconds to apply raft commands
const raftMaxTime = time.Duration(60) * time.Second

// RoutingKey is used by AMQP consumer when binding a queue to an exchange.
const RoutingKey = "delayd"

// delaydService is used by consulapi to watch delayd service on Consul
const delaydService = "delayd"

const (
	delaydEvent      = "delayd"
	delaydLeaveEvent = delaydEvent + ":" + "leave:"
)

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	sender       Sender
	receiver     Receiver
	raft         *Raft
	timer        *Timer
	consul       *consulapi.Client
	config       Config
	registration *consulapi.AgentServiceRegistration
	bootstrapped bool

	shutdownCh chan bool
	serviceCh  chan []*consulapi.ServiceEntry
	eventCh    chan *consulapi.UserEvent
	stopCh     chan struct{}

	mu     sync.Mutex
	leader bool
}

// NewServer initialize Server instance.
func NewServer(c Config, sender Sender, receiver Receiver) (*Server, error) {
	if len(c.LogDir) != 0 {
		logFile := filepath.Join(c.LogDir, "delayd.log")
		logOutput, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		log.SetOutput(logOutput)
	}

	consul, err := NewConsul(c.Consul)
	if err != nil {
		return nil, err
	}

	raft, err := NewRaft(c.Raft, c.DataDir, c.LogDir)
	if err != nil {
		return nil, err
	}

	return &Server{
		sender:     sender,
		receiver:   receiver,
		raft:       raft,
		consul:     consul,
		config:     c,
		shutdownCh: make(chan bool),
		serviceCh:  make(chan []*consulapi.ServiceEntry),
		eventCh:    make(chan *consulapi.UserEvent),
		stopCh:     make(chan struct{}),
	}, nil
}

// Run starts server and begins its main loop.
func (s *Server) Run() {
	Info("server: starting delayd")

	s.timer = NewTimer(s.timerSend)

	go s.observeLeaderChanges()
	go s.observeNextTime()

	if s.config.UseConsul {
		go s.startConsulBackend()
	}

	for {
		select {
		case <-s.stopCh:
			Info("server/run: shutting down gracefully.")
			return
		case msg, ok := <-s.receiver.MessageCh():
			entry := msg.Entry
			// XXX cleanup needed here before exit
			if !ok {
				continue
			}

			Debug("server: got new request entry:", entry)
			b, err := entry.ToBytes()
			if err != nil {
				Error("server: error encoding entry:", err)
				continue
			}

			if err := s.raft.Add(b, raftMaxTime); err != nil {
				Error("server: failed to add:", err)
				if nerr := msg.Nack(); nerr != nil {
					Error("server: failed to nack:", err)
				}
				continue
			}

			msg.Ack()
		}
	}
}

// Stop shuts down the Server cleanly. Order of the Close calls is important.
func (s *Server) Stop() {
	Info("server: shutting down gracefully.")

	// stop triggering new changes to the FSM
	s.receiver.Close()
	s.timer.Stop()

	if s.config.UseConsul {
		s.deregisterService()
	}

	close(s.shutdownCh)

	// with no FSM stimulus, we don't need to send.
	s.sender.Close()

	s.raft.Close()

	// stop mainloop
	close(s.stopCh)
	Info("server: terminated.")
}

func (s *Server) resetTimer() {
	ok, t, err := s.raft.fsm.store.NextTime()
	if err != nil {
		Fatal("server: could not read initial send time from storage:", err)
	}
	if ok {
		s.timer.Reset(t, true)
	}
}

func (s *Server) registerService() error {
	Info("server: registering delayd service on Consul")
	agent := s.consul.Agent()

	_, port, err := net.SplitHostPort(s.config.Raft.Listen)
	if err != nil {
		return err
	}
	p, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return nil
	}

	s.registration = &consulapi.AgentServiceRegistration{
		Name: delaydService,
		ID:   "delayd-" + port,
		Port: int(p),
	}
	err = agent.ServiceRegister(s.registration)
	if err != nil {
		Warn("server: failed to register delayd service on Consul.", err)
	}
	return err
}

func (s *Server) deregisterService() error {
	if s.registration == nil {
		Warn("server: can not deregister since this server is not registered")
		return errors.New("delayd: register before deregister")
	}

	Info("server: deregistering delayd service from Consul")
	agent := s.consul.Agent()
	err := agent.ServiceDeregister(s.registration.ID)
	if err != nil {
		Warn("server: failed to deregister delayd service from Consul")
	}
	return err
}

func (s *Server) startConsulBackend() {
	Info("server: starting consul backend goroutine")
	go s.observeMembership()
	go s.observeShutdownSignal()

	Infof("server: waiting for joining %d nodes...", s.config.BootstrapExpect)
	// Do our best for registering delayd service to Consul
	for {
		if err := s.registerService(); err != nil {
			Warn("failed to register service:", err)
			// TODO: exponential backoff
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
}

func (s *Server) observeShutdownSignal() {
	graceful := make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGUSR2)
	select {
	case <-s.shutdownCh:
		return
	case <-graceful:
		Info("server: received signal to leave gracefully")
		if s.leader {
			Infof("server: removing myself %v from peerset", s.localAddr())
			if err := s.raft.raft.RemovePeer(s.localAddr()).Error(); err != nil {
				Error("failed to remove myself from peerset:", err)
			}
		} else {
			// fire an leave-event
			event := s.consul.Event()
			ue := &consulapi.UserEvent{
				Name:          delaydEvent,
				Payload:       []byte(delaydLeaveEvent + s.localAddr().String()),
				ServiceFilter: delaydService,
			}
			if _, _, err := event.Fire(ue, nil); err != nil {
				Error("failed to fire leave-event:", err)
			}
		}
		s.Stop()
	}
}

func (s *Server) observeMembership() {
	go s.observeServiceChanges()
	go s.observeEvent()

	Debug("server: starting monitoring membership")
	for {
		select {
		case <-s.shutdownCh:
			return
		case e := <-s.eventCh:
			if !s.leader {
				continue
			}

			Infof("server: receiving %s event", e.Name)

			p := string(e.Payload)
			if strings.HasPrefix(p, delaydLeaveEvent) {
				peer, err := net.ResolveTCPAddr("tcp", strings.TrimPrefix(p, delaydLeaveEvent))
				if err != nil {
					Errorf("server: leave-event has bad peer %s: %s", p, err)
					continue
				}

				// reject if peer is pointing to self
				if peer.String() == s.localAddr().String() {
					Error("server: reject leave-event because it asks the leader to remove itself")
					continue
				}

				if err := s.raft.raft.RemovePeer(peer).Error(); err != nil {
					Errorf("server: failed to remove %s from peerset: %s", peer, err)
					continue
				}

				Infof("server: %s was removed from peerset", peer)
			} else {
				Error("server: unknown event received:", p)
			}
		case services := <-s.serviceCh:
			if !s.bootstrapped {
				// wait until the number of registered services is equal or larger than expected
				if len(services) >= s.config.BootstrapExpect && s.registeredSelf(services) {
					s.bootstrap(services)
				}
				continue
			}

			// leader needs to maintain peers in response to service changes after bootstraped.
			if !s.leader {
				continue
			}

			newPeers := convertPeersFromService(services)
			curPeers, err := s.raft.peerStore.Peers()
			if err != nil {
				Error("failed to get peers:", err)
				continue
			}

			for _, p := range newPeers {
				if !raft.PeerContained(curPeers, p) {
					// new peer is found..
					Infof("server: new peer %v is found on consul. adding...", p)
					if err := s.raft.raft.AddPeer(p).Error(); err != nil {
						Error("server: failed to add peer", p)
					}
				}
			}
		}
	}
}

func (s *Server) registeredSelf(services []*consulapi.ServiceEntry) bool {
	if s.registration == nil {
		return false
	}

	for _, service := range services {
		if s.registration.Name == service.Service.Service &&
			s.registration.ID == service.Service.ID {
			return true
		}
	}
	return false
}

func (s *Server) bootstrap(services []*consulapi.ServiceEntry) {
	peers := raft.ExcludePeer(convertPeersFromService(services), s.localAddr())
	if err := s.raft.raft.SetPeers(peers).Error(); err != nil {
		Error("server: failed to setpeers:", err)
	}
	s.bootstrapped = true
	Infof("server: bootstrapped with %v", peers)
}

func (s *Server) observeEvent() {
	query := consulapi.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}
	event := s.consul.Event()

	Info("server: waiting for new delayd events...")
	for {
		entries, meta, err := event.List(delaydEvent, &query)
		if err != nil {
			Error(err)
			continue
		}

		// See https://github.com/hashicorp/consul/blob/master/watch/funcs.go#L185
		for i := 0; i < len(entries); i++ {
			if event.IDToIndex(entries[i].ID) == query.WaitIndex {
				entries = entries[i+1:]
				break
			}
		}
		for _, e := range entries {
			s.eventCh <- e
		}
		query.WaitIndex = meta.LastIndex
	}
}

func (s *Server) observeServiceChanges() {
	query := consulapi.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}
	health := s.consul.Health()
	Info("server: waiting for node changes...")
	for {
		entries, meta, err := health.Service(delaydService, "", false, &query)
		if err != nil {
			Error(err)
			continue
		}
		s.serviceCh <- entries
		query.WaitIndex = meta.LastIndex
	}
}

// Listen for changes to raft for when we transition to and from the leader state
// and react accordingly.
func (s *Server) observeLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		s.mu.Lock()
		s.leader = isLeader
		s.mu.Unlock()

		if isLeader {
			Debug("server: became raft leader")
			s.resetTimer()
			if err := s.receiver.Start(); err != nil {
				Fatal("server: error while starting receiver:", err)
			}
		} else {
			Debug("server: lost raft leadership")
			s.timer.Pause()
			if err := s.receiver.Pause(); err != nil {
				Fatal("server: error while starting receiver:", err)
			}
		}
	}
}

// Listen to storage for next time changes on entry commits
func (s *Server) observeNextTime() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case t := <-s.raft.fsm.store.C:
			s.mu.Lock()
			if s.leader {
				Debug("server: got time: ", t)
				s.timer.Reset(t, false)
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) timerSend(t time.Time) {
	// FIXME: In the case of error, we don't remove a log from raft log.
	uuids, entries, err := s.raft.fsm.store.Get(t)
	if err != nil {
		Fatal("server: could not read entries from db:", err)
	}

	if len(entries) > 1 {
		Infof("server: sending %d entries", len(entries))
	}

	if len(entries) == 0 {
		return
	}

	for i, e := range entries {
		if err := s.sender.Send(e); err != nil {
			if aerr, ok := err.(*amqp.Error); ok && aerr.Code == 504 {
				// error 504 code means that the exchange we were trying
				// to send on didnt exist.  In the case of delayd this usually
				// means that a consumer didn't set up the exchange they wish
				// to be notified on. We do not attempt to make this for them,
				// as we don't know what exchange options they would want, we
				// simply drop this message, other errors are fatal
				Warnf("server: channel/connection not set up for exchange `%s`, message will be deleted", e.Target, aerr)
			}

			// FIXME: I don't think Fatal here is right way.
			// If a reason that node is failed to send is node specific,
			// Fatal causes leader election then a problem may be resolved.
			// If the reason is not node specific, all instance may be down....
			Fatal("server: could not send entry:", err)
		}

		if err := s.raft.Remove(uuids[i], raftMaxTime); err != nil {
			// This node is no longer the leader. give up on other amqp sends,
			// and scheduling the next emission
			Warnf("server: lost raft leadership during remove. AMQP send will be a duplicate. uuid=%x", uuids[i])
			break
		}
	}

	// ensure everyone is up to date
	Debug("server: syncing raft after send.")
	err = s.raft.SyncAll()
	if err != nil {
		Warn("server: lost raft leadership during sync after send.")
		return
	}

	Debug("server: synced raft after send.")
}

func (s *Server) localAddr() net.Addr {
	return s.raft.transport.LocalAddr()
}

func convertPeersFromService(services []*consulapi.ServiceEntry) []net.Addr {
	peers := []net.Addr{}
	for _, service := range services {
		port := strconv.FormatInt(int64(service.Service.Port), 10)
		// Currently, IP address in consul's service catalog can not be set in registration.
		// This limitation is so bad for testing multiple delayd instance with 1 agent.
		// See https://github.com/hashicorp/consul/issues/229 for discussion
		address := service.Node.Address
		if tweak := os.Getenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK"); tweak != "" {
			address = tweak
		}
		peer, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(address, port))
		if err != nil {
			Fatal("server: bad peer:", err)
		}
		peers = append(peers, peer)
	}
	return peers
}
