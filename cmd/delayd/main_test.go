package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
	integAMQP = flag.Bool("amqp", false, "Enable integration tests against AMQP")
	integSQS  = flag.Bool("sqs", false, "Enable integration tests against SQS")
)

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

type testCase struct {
	Sender   delayd.Sender
	Receiver delayd.Receiver
	Client   testClient
}

type testCaseFunc func(config delayd.Config, out io.Writer) testCase

func TestSQS(t *testing.T) {
	if !*integSQS {
		t.Skip("Integration tests for SQS is disabled")
	}

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	doIntegration(t, func(config delayd.Config, out io.Writer) testCase {
		region, found := aws.Regions[config.SQS.Region]
		if !found {
			t.Fatalf("region %s is not valid", config.SQS.Region)
		}
		s := sqs.New(auth, region)

		receiver, err := delayd.NewSQSReceiver(config.SQS, s)
		if err != nil {
			t.Fatal(err)
		}

		targetQueue := os.Getenv("DELAYD_TARGET_SQS")
		if targetQueue == "" {
			t.Fatal("DELAYD_SQS must be set")
		}

		target := config.SQS
		target.Queue = targetQueue
		c, err := NewTestSQSClient(target, config.SQS, s, out)
		if err != nil {
			t.Fatal(err)
		}

		return testCase{
			Sender:   delayd.NewSQSSender(s),
			Receiver: receiver,
			Client:   c,
		}
	})
}

func TestAMQP(t *testing.T) {
	if !*integAMQP {
		t.Skip("Integration tests for AMQP is disabled")
	}

	doIntegration(t, func(config delayd.Config, out io.Writer) testCase {
		sender, err := delayd.NewAMQPSender(config.AMQP.URL)
		if err != nil {
			t.Fatal(err)
		}

		receiver, err := delayd.NewAMQPReceiver(config.AMQP, delayd.RoutingKey)
		if err != nil {
			t.Fatal(err)
		}

		target := config.AMQP
		target.Exchange.Name = "delayd-test"
		target.Queue.Name = ""
		target.Queue.Bind = []string{target.Exchange.Name}
		c, err := NewTestAMQPClient(target, config.AMQP, out)
		if err != nil {
			t.Fatal(err)
		}

		return testCase{
			Sender:   sender,
			Receiver: receiver,
			Client:   c,
		}
	})
}

func doIntegration(t *testing.T, f testCaseFunc) {
	if testing.Short() {
		t.Skip("Skipping test")
	}

	assert := assert.New(t)

	config, err := loadConfig("delayd.toml")

	// create an ephemeral location for data storage during tests
	config.DataDir, err = ioutil.TempDir("", "delayd-testint")
	assert.NoError(err)
	defer os.Remove(config.DataDir)

	// Use stdout instead of file
	config.LogDir = ""

	out, err := os.OpenFile("testdata/out.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	testCase := f(config, out)
	s, err := delayd.NewServer(config, testCase.Sender, testCase.Receiver)
	if err != nil {
		t.Fatal(err)
	}
	go s.Run()
	defer s.Stop()

	// Send messages to delayd exchange
	msgs, err := loadTestMessages("testdata/in.toml")
	if err != nil {
		t.Fatal(err)
	}
	if err := testCase.Client.SendMessages(msgs); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(msgs))
	go func() {
		for _ = range testCase.Client.RecvLoop() {
			wg.Done()
		}
	}()

	// Wait for messages to be processed
	wg.Wait()

	// shutdown consumer
	testCase.Client.Close()

	// remove all whitespace for a more reliable compare
	f1 := strings.Trim(getFileAsString("testdata/expected.txt"), "\n ")
	f2 := strings.Trim(getFileAsString("testdata/out.txt"), "\n ")

	assert.Equal(f1, f2)
	if err := os.Remove("testdata/out.txt"); err != nil {
		t.Fatal(t)
	}
}
