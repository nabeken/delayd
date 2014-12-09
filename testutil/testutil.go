package testutil

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/crowdmob/goamz/sqs"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	"github.com/nabeken/delayd"
)

// Message holds a message to send to delayd server for testing.
type Message struct {
	Value string
	Key   string
	Delay int64
}

type Client interface {
	SendMessages(msg []Message) error
	RecvLoop() (done <-chan struct{})
	delayd.Closer
}

type SQSClient struct {
	*delayd.SQSConsumer

	delaydQueue *sqs.Queue
	target      delayd.SQSConfig
	config      delayd.SQSConfig
	out         io.Writer
}

// NewSQSClient creates and returns a SQSClient instance for testing.
func NewSQSClient(target, config delayd.SQSConfig, s *sqs.SQS, out io.Writer) (*SQSClient, error) {
	consumer, err := delayd.NewSQSConsumer(target, s)
	if err != nil {
		return nil, err
	}

	delaydQueue, err := s.GetQueue(config.Queue)
	if err != nil {
		return nil, err
	}

	c := &SQSClient{
		SQSConsumer: consumer,

		delaydQueue: delaydQueue,
		config:      config,
		target:      target,
		out:         out,
	}
	c.Start()
	return c, nil
}

// SendMessages sends test messages to delayd queue.
func (c *SQSClient) SendMessages(msgs []Message) error {
	for _, msg := range msgs {
		attrs := map[string]string{
			"delayd-delay":  strconv.FormatInt(msg.Delay, 10),
			"delayd-target": c.Config.Queue,
		}
		if msg.Key != "" {
			attrs["delayd-key"] = msg.Key
		}
		if _, err := c.delaydQueue.SendMessageWithAttributes(msg.Value, attrs); err != nil {
			return err
		}
	}
	return nil
}

// RecvLoop processes messages reading from deliveryCh until quit is closed.
// it also send a notification via done channel when processing is done.
func (c *SQSClient) RecvLoop() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for msg := range c.Messages {
			fmt.Fprintf(c.out, "%s\n", msg.Body)
			done <- struct{}{}
			c.Queue.DeleteMessage(msg)
		}
		delayd.Info("sqs_client: MessageCh is closed. existing.")
	}()
	return done
}

// AMQPClient is the delayd client.
// It can relay messages to the server for easy testing.
type AMQPClient struct {
	*delayd.AMQPConsumer

	delaydEx   delayd.AMQPConfig
	deliveryCh <-chan amqp.Delivery
	out        io.Writer
}

// NewAMQPClient creates and returns a AMQPClient instance.
func NewAMQPClient(targetEx, delaydEx delayd.AMQPConfig, out io.Writer) (*AMQPClient, error) {
	a, err := delayd.NewAMQPConsumer(targetEx, "")
	if err != nil {
		return nil, err
	}

	q := targetEx.Queue
	deliveryCh, err := a.Channel.Consume(
		a.Queue.Name,
		"delayd", // consumer
		true,     // autoAck
		q.Exclusive,
		q.NoLocal,
		q.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &AMQPClient{
		AMQPConsumer: a,

		delaydEx:   delaydEx,
		deliveryCh: deliveryCh,
		out:        out,
	}, nil
}

// SendMessages sends test messages to delayd exchange.
func (c *AMQPClient) SendMessages(msgs []Message) error {
	for _, msg := range msgs {
		pm := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Headers: amqp.Table{
				"delayd-delay":  msg.Delay,
				"delayd-target": c.Config.Exchange.Name,
				"delayd-key":    msg.Key,
			},
			Body: []byte(msg.Value),
		}
		if err := c.Channel.Publish(
			c.delaydEx.Exchange.Name,
			c.delaydEx.Queue.Name,
			true,  // mandatory
			false, // immediate
			pm,
		); err != nil {
			return err
		}
	}
	return nil
}

// RecvLoop processes messages reading from deliveryCh until quit is closed.
// it also send a notification via done channel when processing is done.
func (c *AMQPClient) RecvLoop() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for msg := range c.deliveryCh {
			fmt.Fprintf(c.out, "%s\n", msg.Body)
			done <- struct{}{}
			// No need to ack here because autoack is enabled
		}
		delayd.Info("amqp_client: deliveryCh is closed. existing.")
	}()
	return done
}

type Server struct {
	*delayd.Server

	config delayd.Config
}

func NewServer(config delayd.Config, sender delayd.Sender, receiver delayd.Receiver) (*Server, error) {
	s, err := delayd.NewServer(config, sender, receiver)
	if err != nil {
		return nil, err
	}
	return &Server{
		Server: s,
		config: config,
	}, nil
}

func (s *Server) Stop() {
	s.Server.Stop()
	if err := os.RemoveAll(s.config.DataDir); err != nil {
		delayd.Error(err)
	}
}

type (
	ClientFunc  func(config delayd.Config, out io.Writer) (Client, error)
	ServerFunc  func(config delayd.Config) (*Server, error)
	ServersFunc func(config delayd.Config) ([]*Server, error)
)

// ServersFunc converts ServerFunc type to ServersFunc.
func (f ServerFunc) ServersFunc() ServersFunc {
	return func(config delayd.Config) ([]*Server, error) {
		s, err := f(config)
		if err != nil {
			return nil, err
		}
		return []*Server{s}, nil
	}
}

// SQSServerFunc is a factory that returns Server for testing with SQS.
var SQSServerFunc = func(config delayd.Config) (*Server, error) {
	// create an ephemeral location for data storage during tests
	dataDir, err := ioutil.TempDir("", "delayd-integ-test-sqs")
	if err != nil {
		return nil, err
	}
	config.DataDir = dataDir

	s, err := delayd.NewSQS(config)
	if err != nil {
		return nil, err
	}

	receiver, err := delayd.NewSQSReceiver(config.SQS, s)
	if err != nil {
		return nil, err
	}

	return NewServer(config, delayd.NewSQSSender(s), receiver)
}

// SQSClientFunc is a factory that returns Client for testing with SQS.
var SQSClientFunc = func(config delayd.Config, out io.Writer) (Client, error) {
	s, err := delayd.NewSQS(config)
	if err != nil {
		return nil, err
	}

	targetQueue := os.Getenv("DELAYD_TARGET_SQS")
	if targetQueue == "" {
		return nil, errors.New("DELAYD_TARGET_SQS must be set")
	}

	target := config.SQS
	target.Queue = targetQueue
	c, err := NewSQSClient(target, config.SQS, s, out)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// AMQPServerFunc is a factory that returns Server for testing with AMQP.
var AMQPServerFunc = func(config delayd.Config) (*Server, error) {
	// create an ephemeral location for data storage during tests
	dataDir, err := ioutil.TempDir("", "delayd-integ-test-amqp")
	if err != nil {
		return nil, err
	}
	config.DataDir = dataDir

	sender, err := delayd.NewAMQPSender(config.AMQP.URL)
	if err != nil {
		return nil, err
	}

	receiver, err := delayd.NewAMQPReceiver(config.AMQP, delayd.RoutingKey)
	if err != nil {
		return nil, err
	}

	return NewServer(config, sender, receiver)
}

// AMQPClientFunc is a factory that returns Client for testing with AMQP.
var AMQPClientFunc = func(config delayd.Config, out io.Writer) (Client, error) {
	target := config.AMQP
	target.Exchange.Name = "delayd-test"
	target.Queue.Name = ""
	target.Queue.Bind = []string{target.Exchange.Name}

	return NewAMQPClient(target, config.AMQP, out)
}

func loadMessages(path string) (msgs []Message, err error) {
	messages := struct {
		Message []Message
	}{}
	delayd.Debug("reading", path)
	_, err = toml.DecodeFile(path, &messages)
	return messages.Message, err
}

func getFile(path string) string {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(dat)
}

func RaftServers(bootstrapExpect int, f ServerFunc) ServersFunc {
	return func(config delayd.Config) ([]*Server, error) {
		servers := []*Server{}

		config.UseConsul = true
		config.Raft.Single = false
		config.BootstrapExpect = bootstrapExpect

		for i := 0; i < bootstrapExpect; i++ {
			sc := config
			sc.Raft.Listen = net.JoinHostPort("127.0.0.1", strconv.FormatInt(7999-int64(i), 10))
			sc.Raft.Advertise = net.JoinHostPort("127.0.0.1", strconv.FormatInt(7999-int64(i), 10))
			s, err := f(sc)
			if err != nil {
				return nil, err
			}
			servers = append(servers, s)
		}
		return servers, nil
	}
}

func DoIntegrationMultiple(t *testing.T, bootstrapExpect int, c ClientFunc, f ServerFunc) {
	sf := RaftServers(bootstrapExpect, f)
	DoIntegration(t, c, sf)
}

func DoIntegration(t *testing.T, cf ClientFunc, sf ServersFunc) {
	if testing.Short() {
		t.Skip("Skipping test")
	}

	assert := assert.New(t)

	config, err := delayd.LoadConfig("delayd.toml")
	if err != nil {
		t.Fatal(err)
	}

	// Use stdout instead of file
	config.LogDir = ""

	out, err := os.OpenFile("testdata/out.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	client, err := cf(config, out)
	if err != nil {
		t.Fatal(err)
	}

	servers, err := sf(config)
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range servers {
		go s.Run()
		defer s.Stop()
	}

	// Send messages to delayd exchange
	msgs, err := loadMessages("testdata/in.toml")
	if err != nil {
		t.Error(err)
		return
	}
	if err := client.SendMessages(msgs); err != nil {
		t.Error(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(msgs))
	go func() {
		for _ = range client.RecvLoop() {
			wg.Done()
		}
	}()

	// Wait for messages to be processed
	wg.Wait()

	// shutdown consumer
	client.Close()

	// remove all whitespace for a more reliable compare
	f1 := strings.Trim(getFile("testdata/expected.txt"), "\n ")
	f2 := strings.Trim(getFile("testdata/out.txt"), "\n ")

	assert.Equal(f1, f2)
	if err := os.Remove("testdata/out.txt"); err != nil {
		t.Error(t)
		return
	}
}
