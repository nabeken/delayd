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
	"github.com/cenkalti/backoff"
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

// DefaultTickDuration is used by default configuration for ticker
const DefaultTickDuration = 500 * time.Millisecond

const numConcurrentSender = 100

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	bootstrapped bool
	config       Config
	consul       *consulapi.Client
	raft         *Raft
	receiver     Receiver
	registration *consulapi.AgentServiceRegistration
	sender       Sender

	eventCh    chan *consulapi.UserEvent
	serviceCh  chan []*consulapi.ServiceEntry
	shutdownCh chan bool
	stopCh     chan struct{}
	tickCh     <-chan time.Time

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

	if c.TickDuration > 0 {
		c.TickDuration *= time.Millisecond
	} else {
		c.TickDuration = DefaultTickDuration
	}

	consul, err := NewConsul(c.Consul)
	if err != nil {
		return nil, err
	}

	// Retrieve advertise
	if c.UseConsul && c.Raft.Advertise == "" {
		_, port, err := net.SplitHostPort(c.Raft.Listen)
		if err != nil {
			return nil, err
		}

		b := backoff.NewExponentialBackOff()
		operation := func() error {
			self, err := consul.Agent().Self()
			if err != nil {
				Warnf("failed to get advertise: %s. will retry after %s", err, b.NextBackOff())
				return err
			}

			addr, ok := self["Config"]["AdvertiseAddr"].(string)
			if !ok {
				return err
			}

			c.Raft.Advertise = net.JoinHostPort(addr, port)
			return nil
		}

		if err := backoff.Retry(operation, b); err != nil {
			return nil, err
		}
	}

	raft, err := NewRaft(c.Raft, c.DataDir, c.LogDir)
	if err != nil {
		return nil, err
	}

	return &Server{
		sender:   sender,
		receiver: receiver,
		raft:     raft,
		consul:   consul,
		config:   c,

		eventCh:    make(chan *consulapi.UserEvent),
		serviceCh:  make(chan []*consulapi.ServiceEntry),
		shutdownCh: make(chan bool),
		stopCh:     make(chan struct{}),
		tickCh:     time.Tick(c.TickDuration),
	}, nil
}

// Run starts server and begins its main loop.
func (s *Server) Run() {
	Infof("server: starting delayd with ticking every %s. Listen: %s, Adv: %s",
		s.config.TickDuration,
		s.config.Raft.Listen,
		s.config.Raft.Advertise,
	)

	go s.tickerLoop()
	go s.observeLeaderChanges()

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
	b := backoff.NewExponentialBackOff()
	// never stop
	b.MaxElapsedTime = 0

	ticker := backoff.NewTicker(b)
	for _ = range ticker.C {
		if err := s.registerService(); err != nil {
			Warnf("failed to register service: %s. will retry after %s", err, b.NextBackOff())
			continue
		}
		ticker.Stop()
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
				Payload:       []byte(delaydLeaveEvent + s.localAddr()),
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
				if peer.String() == s.localAddr() {
					Error("server: reject leave-event because it asks the leader to remove itself")
					continue
				}

				if err := s.raft.raft.RemovePeer(peer.String()).Error(); err != nil {
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

	b := backoff.NewExponentialBackOff()
	operation := func() error {
		entries, meta, err := event.List(delaydEvent, &query)
		if err != nil {
			Warn("server: failed to retrieve events from consul. will retry after", b.NextBackOff())
			return err
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

		return nil
	}

	for {
		if err := backoff.Retry(operation, b); err != nil {
			Warnf("failed to register service: %s. will retry...", err)
		}
	}
}

func (s *Server) observeServiceChanges() {
	query := consulapi.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}
	health := s.consul.Health()

	Info("server: waiting for node changes...")

	b := backoff.NewExponentialBackOff()
	operation := func() error {
		entries, meta, err := health.Service(delaydService, "", false, &query)
		if err != nil {
			Warn("server: failed to retrieve services from consul. will retry after", b.NextBackOff())
			return err
		}
		s.serviceCh <- entries
		query.WaitIndex = meta.LastIndex

		return nil
	}

	for {
		if err := backoff.Retry(operation, b); err != nil {
			Warnf("failed to monitor service changes: %s. will retry...", err)
		}
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
			if err := s.receiver.Start(); err != nil {
				Fatal("server: error while starting receiver:", err)
			}
		} else {
			Debug("server: lost raft leadership")
			if err := s.receiver.Pause(); err != nil {
				Fatal("server: error while starting receiver:", err)
			}
		}
	}
}

func (s *Server) tickerLoop() {
	for {
		select {
		case <-s.shutdownCh:
			Info("server: ticker: receiving shutdown signal. existing.")
			return
		case sendTime := <-s.tickCh:
			if s.leader {
				s.timerSend(sendTime)
			}
		}
	}
}

func (s *Server) timerSend(t time.Time) {
	// FIXME: In the case of error, we don't remove a log from raft log.
	uuids, entries, err := s.raft.fsm.store.Get(t)
	if err != nil {
		Fatal("server: could not read entries from db:", err)
	}

	if len(entries) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(entries))

	sem := make(chan struct{}, numConcurrentSender)
	for i, e := range entries {
		sem <- struct{}{}
		go func(uuid []byte, e *Entry) {
			s.handleSend(uuid, e)
			<-sem
			wg.Done()
		}(uuids[i], e)
	}

	wg.Wait()

	// ensure everyone is up to date
	err = s.raft.SyncAll()
	if err != nil {
		Warn("server: lost raft leadership during sync after send.")
		return
	}
}

func (s *Server) handleSend(uuid []byte, e *Entry) {
	if err := s.sender.Send(e); err != nil {
		if aerr, ok := err.(*amqp.Error); ok && aerr.Code == 504 {
			// error 504 code means that the exchange we were trying
			// to send on didnt exist.  In the case of delayd this usually
			// means that a consumer didn't set up the exchange they wish
			// to be notified on. We do not attempt to make this for them,
			// as we don't know what exchange options they would want, we
			// simply drop this message, other errors are fatal
			Warnf("server: channel/connection not set up for exchange `%s`, message will be deleted: %s", e.Target, aerr)
		}

		// FIXME: I don't think Fatal here is right way.
		// If a reason that node is failed to send is node specific,
		// Fatal causes leader election then a problem may be resolved.
		// If the reason is not node specific, all instance may be down....
		Fatal("server: could not send entry:", err)
	}

	if err := s.raft.Remove(uuid, raftMaxTime); err != nil {
		// This node is no longer the leader. give up on other amqp sends,
		// and scheduling the next emission
		Warnf("server: lost raft leadership during remove. AMQP send will be a duplicate. uuid=%x", uuid)
	}
}

func (s *Server) localAddr() string {
	return s.raft.transport.LocalAddr()
}

func convertPeersFromService(services []*consulapi.ServiceEntry) []string {
	peers := []string{}
	for _, service := range services {
		port := strconv.FormatInt(int64(service.Service.Port), 10)
		address := service.Node.Address
		peers = append(peers, net.JoinHostPort(address, port))
	}
	return peers
}
