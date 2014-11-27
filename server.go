package delayd

import (
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/armon/consul-api"
	"github.com/streadway/amqp"
)

// a generous 60 seconds to apply raft commands
const raftMaxTime = time.Duration(60) * time.Second

// RoutingKey is used by AMQP consumer when binding a queue to an exchange.
const RoutingKey = "delayd"

// delaydService is used by consulapi to watch delayd service on Consul
const delaydService = "delayd"

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	sender       Sender
	receiver     Receiver
	raft         *Raft
	timer        *Timer
	shutdownCh   chan bool
	serviceCh    chan []*consulapi.ServiceEntry
	leader       bool
	mu           sync.Mutex
	consul       *consulapi.Client
	config       Config
	registration *consulapi.AgentServiceRegistration
	bootstrapped bool
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
		msg, ok := <-s.receiver.MessageCh()
		entry := msg.Entry
		// XXX cleanup needed here before exit
		if !ok {
			continue
		}

		Debug("server: got new request entry:", entry)
		b, err := entry.ToBytes()
		if err != nil {
			Error("server: error encoding entry: ", err)
			continue
		}

		if err := s.raft.Add(b, raftMaxTime); err != nil {
			Error("server: failed to add: ", err)
			msg.Nack()
			continue
		}

		msg.Ack()
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
	Info("server: deregistering delayd service from Consul")
	agent := s.consul.Agent()
	err := agent.ServiceDeregister(s.registration.ID)
	if err != nil {
		Warn("server: failed to deregister delayd service from Consul")
	}
	return err
}

func (s *Server) startConsulBackend() {
	go s.observeService()

	if !s.bootstrapped {
		go s.maybeBootstrap()
	}

	// Do our best for registering delayd service to Consul
	for {
		if err := s.registerService(); err != nil {
			// TODO: exponential backoff
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
}

func (s *Server) observeService() {
	query := consulapi.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}
	health := s.consul.Health()

	Debug("server: starting service monitoring")
	for {
		select {
		case <-s.shutdownCh:
			close(s.serviceCh)
			return
		default:
		}

		Info("server: Waiting for node changes...")
		entries, meta, err := health.Service(delaydService, "", false, &query)
		if err != nil {
			Error(err)
			continue
		}
		s.serviceCh <- entries
		query.WaitIndex = meta.LastIndex
	}
}

func (s *Server) bootstrap(services []*consulapi.ServiceEntry) {
	peers := []net.Addr{}
	for _, s := range services {
		port := strconv.FormatInt(int64(s.Service.Port), 10)
		// Currently, IP address in consul's service catalog can not be set in registration.
		// This limitation is so bad for testing multiple delayd instance with 1 agent.
		// See https://github.com/hashicorp/consul/issues/229 for discussion
		address := s.Node.Address
		if tweak := os.Getenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK"); tweak != "" {
			address = tweak
		}
		peer, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(address, port))
		if err != nil {
			Fatal("server: bad peer:", err)
		}
		peers = append(peers, peer)
	}
	s.raft.raft.SetPeers(peers)
	s.bootstrapped = true
	Infof("server: bootstrapped with %v", peers)
}

func (s *Server) maybeBootstrap() {
	Infof("server: Waiting for joining %d nodes...", s.config.BootstrapExpect)
	for {
		select {
		case <-s.shutdownCh:
			return
		case services := <-s.serviceCh:
			if len(services) >= s.config.BootstrapExpect {
				s.bootstrap(services)
				return
			}
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
			if err, ok := err.(*amqp.Error); ok && err.Code == 504 {
				// error 504 code means that the exchange we were trying
				// to send on didnt exist.  In the case of delayd this usually
				// means that a consumer didn't set up the exchange they wish
				// to be notified on. We do not attempt to make this for them,
				// as we don't know what exchange options they would want, we
				// simply drop this message, other errors are fatal
				Warnf("server: channel/connection not set up for exchange `%s`, message will be deleted", e.Target, err)
			} else {
				Fatal("server: could not send entry:", err)
			}
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
