package main

import (
	"errors"
	"flag"
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
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	"github.com/nabeken/delayd"
)

var (
	flagAMQP   = flag.Bool("amqp", false, "Enable integration tests against AMQP")
	flagSQS    = flag.Bool("sqs", false, "Enable integration tests against SQS")
	flagConsul = flag.Bool("consul", false, "Enable integraton tests with Consul")
)

const bootstrapExpect = 3

func getFileAsString(path string) string {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(dat[:])
}

type testClient interface {
	SendMessages(msg []TestMessage) error
	RecvLoop() (done <-chan struct{})
	delayd.Closer
}

type TestSQSClient struct {
	*delayd.SQSConsumer

	delaydQueue *sqs.Queue
	target      delayd.SQSConfig
	config      delayd.SQSConfig
	out         io.Writer
}

// NewTestSQSClient creates and returns a TestSQSClient instance.
func NewTestSQSClient(target, config delayd.SQSConfig, s *sqs.SQS, out io.Writer) (*TestSQSClient, error) {
	consumer, err := delayd.NewSQSConsumer(target, s)
	if err != nil {
		return nil, err
	}

	delaydQueue, err := s.GetQueue(config.Queue)
	if err != nil {
		return nil, err
	}

	c := &TestSQSClient{
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
func (c *TestSQSClient) SendMessages(msgs []TestMessage) error {
	for _, msg := range msgs {
		attrs := map[string]string{
			"delayd-delay":  strconv.FormatInt(msg.Delay, 10),
			"delayd-target": c.Config.Queue,
			"delayd-key":    msg.Key,
		}
		if _, err := c.delaydQueue.SendMessageWithAttributes(msg.Value, attrs); err != nil {
			return err
		}
	}
	return nil
}

// RecvLoop processes messages reading from deliveryCh until quit is closed.
// it also send a notification via done channel when processing is done.
func (c *TestSQSClient) RecvLoop() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for msg := range c.Messages {
			fmt.Fprintf(c.out, "%s\n", msg.Body)
			delayd.Infof("sqs_client: written %s", string(msg.Body))
			done <- struct{}{}
			c.Queue.DeleteMessage(msg)
		}
		delayd.Info("sqs_client: MessageCh is closed. existing.")
	}()
	return done
}

// TestAMQPClient is the delayd client.
// It can relay messages to the server for easy testing.
type TestAMQPClient struct {
	*delayd.AMQPConsumer

	delaydEx   delayd.AMQPConfig
	deliveryCh <-chan amqp.Delivery
	out        io.Writer
}

// NewTestAMQPClient creates and returns a TestAMQPClient instance.
func NewTestAMQPClient(targetEx, delaydEx delayd.AMQPConfig, out io.Writer) (*TestAMQPClient, error) {
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

	return &TestAMQPClient{
		AMQPConsumer: a,

		delaydEx:   delaydEx,
		deliveryCh: deliveryCh,
		out:        out,
	}, nil
}

// SendMessages sends test messages to delayd exchange.
func (c *TestAMQPClient) SendMessages(msgs []TestMessage) error {
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
func (c *TestAMQPClient) RecvLoop() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for msg := range c.deliveryCh {
			fmt.Fprintf(c.out, "%s\n", msg.Body)
			delayd.Infof("amqp_client: written %s", string(msg.Body))
			done <- struct{}{}
			// No need to ack here because autoack is enabled
		}
		delayd.Info("amqp_client: deliveryCh is closed. existing.")
	}()
	return done
}

func loadTestMessages(path string) (msgs []TestMessage, err error) {
	messages := struct {
		Message []TestMessage
	}{}
	delayd.Debug("reading", path)
	_, err = toml.DecodeFile(path, &messages)
	return messages.Message, err
}

// TestMessage holds a message to send to delayd server for testing.
type TestMessage struct {
	Value string
	Key   string
	Delay int64
}

type testServer struct {
	*delayd.Server

	config delayd.Config
}

func newTestServer(config delayd.Config, sender delayd.Sender, receiver delayd.Receiver) (*testServer, error) {
	s, err := delayd.NewServer(config, sender, receiver)
	if err != nil {
		return nil, err
	}
	return &testServer{
		Server: s,
		config: config,
	}, nil
}

func (s *testServer) Stop() {
	s.Server.Stop()
	if err := os.RemoveAll(s.config.DataDir); err != nil {
		delayd.Error(err)
	}
}

type (
	testClientFunc  func(config delayd.Config, out io.Writer) (testClient, error)
	testServerFunc  func(config delayd.Config) (*testServer, error)
	testServersFunc func(config delayd.Config) ([]*testServer, error)
)

func (f testServerFunc) ServersFunc() testServersFunc {
	return func(config delayd.Config) ([]*testServer, error) {
		s, err := f(config)
		if err != nil {
			return nil, err
		}
		return []*testServer{s}, nil
	}
}

func newSQS(config delayd.Config) (*sqs.SQS, error) {
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		return nil, err
	}

	region, found := aws.Regions[config.SQS.Region]
	if !found {
		return nil, fmt.Errorf("region %s is not valid", config.SQS.Region)
	}
	return sqs.New(auth, region), nil
}

func sqsTestServer(config delayd.Config) (*testServer, error) {
	// create an ephemeral location for data storage during tests
	dataDir, err := ioutil.TempDir("", "delayd-integ-test-sqs")
	if err != nil {
		return nil, err
	}
	config.DataDir = dataDir

	s, err := newSQS(config)
	if err != nil {
		return nil, err
	}

	receiver, err := delayd.NewSQSReceiver(config.SQS, s)
	if err != nil {
		return nil, err
	}

	return newTestServer(config, delayd.NewSQSSender(s), receiver)
}

func sqsTestClient(config delayd.Config, out io.Writer) (testClient, error) {
	s, err := newSQS(config)
	if err != nil {
		return nil, err
	}

	targetQueue := os.Getenv("DELAYD_TARGET_SQS")
	if targetQueue == "" {
		return nil, errors.New("DELAYD_TARGET_SQS must be set")
	}

	target := config.SQS
	target.Queue = targetQueue
	c, err := NewTestSQSClient(target, config.SQS, s, out)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func TestSQS(t *testing.T) {
	if !*flagSQS {
		t.Skip("Integration tests for SQS is disabled")
	}

	doIntegration(t, sqsTestClient, testServerFunc(sqsTestServer).ServersFunc())
}

func TestSQS_Multiple(t *testing.T) {
	if !*flagSQS || !*flagConsul {
		t.Skip("Integration tests for SQS with multiple delayd is disabled")
	}

	doIntegration_Multiple(t, sqsTestClient, sqsTestServer)
}

func amqpTestServer(config delayd.Config) (*testServer, error) {
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

	return newTestServer(config, sender, receiver)
}

func amqpTestClient(config delayd.Config, out io.Writer) (testClient, error) {
	target := config.AMQP
	target.Exchange.Name = "delayd-test"
	target.Queue.Name = ""
	target.Queue.Bind = []string{target.Exchange.Name}

	return NewTestAMQPClient(target, config.AMQP, out)
}

func TestAMQP(t *testing.T) {
	if !*flagAMQP {
		t.Skip("Integration tests for AMQP is disabled")
	}
	doIntegration(t, amqpTestClient, testServerFunc(amqpTestServer).ServersFunc())
}

func TestAMQP_Multiple(t *testing.T) {
	if !*flagAMQP || !*flagConsul {
		t.Skip("Integration tests for AMQP with multiple delayd is disabled")
	}

	doIntegration_Multiple(t, amqpTestClient, amqpTestServer)
}

func doIntegration_Multiple(t *testing.T, c testClientFunc, f testServerFunc) {
	os.Setenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK", "127.0.0.1")
	defer os.Setenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK", "")

	sf := func(config delayd.Config) ([]*testServer, error) {
		servers := []*testServer{}

		config.UseConsul = *flagConsul
		if config.UseConsul {
			config.Raft.Single = false
			config.BootstrapExpect = bootstrapExpect
		}

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

	doIntegration(t, c, sf)
}

func doIntegration(t *testing.T, cf testClientFunc, sf testServersFunc) {
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
	msgs, err := loadTestMessages("testdata/in.toml")
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
	f1 := strings.Trim(getFileAsString("testdata/expected.txt"), "\n ")
	f2 := strings.Trim(getFileAsString("testdata/out.txt"), "\n ")

	assert.Equal(f1, f2)
	if err := os.Remove("testdata/out.txt"); err != nil {
		t.Error(t)
		return
	}
}
