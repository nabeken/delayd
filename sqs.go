package delayd

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/crowdmob/goamz/sqs"
)

// SQSConsumer represents general SQS consumer.
type SQSConsumer struct {
	Config   SQSConfig
	Queue    *sqs.Queue
	Messages chan *sqs.Message

	shutdown chan struct{}
	paused   bool
	mu       sync.Mutex
}

// NewSQSConsumer creates a consumer for SQS and returns SQSConsumer instance.
func NewSQSConsumer(sc SQSConfig, s *sqs.SQS) (*SQSConsumer, error) {
	q, err := s.GetQueue(sc.Queue)
	if err != nil {
		return nil, err
	}
	c := &SQSConsumer{
		Config: sc,
		Queue:  q,

		paused:   true,
		Messages: make(chan *sqs.Message),
		shutdown: make(chan struct{}),
	}

	return c, nil
}

func (c *SQSConsumer) poll() {
	Infof("sqs: start consuming queue %s", c.Config.Queue)
	for {
		if c.paused {
			Infof("sqs: consuming queue %s is now stopped", c.Config.Queue)
			return
		}

		resp, err := c.Queue.ReceiveMessage(c.Config.MaxNumberOfMessages)
		if err != nil {
			Warn("sqs: failed to receive messages:", err)
			continue
		}

		if len(resp.Messages) < 1 {
			continue
		}

		for i := range resp.Messages {
			c.Messages <- &resp.Messages[i]
		}
	}
}

// Start or restart poller on the queue
func (c *SQSConsumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.paused {
		return nil
	}

	c.paused = false

	go c.poll()
	return nil
}

// Pause pauses polling on the queue
func (c *SQSConsumer) Pause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused {
		return nil
	}
	c.paused = true

	return nil
}

// Close closes the shutdown channel to notify shutting down.
func (c *SQSConsumer) Close() {
	c.Pause()
	close(c.shutdown)
}

// SQSReceiver receives delayd messages over SQS.
type SQSReceiver struct {
	*SQSConsumer

	c chan Message
}

// NewSQSReceiver returns new SQSReceiver instances.
func NewSQSReceiver(sc SQSConfig, s *sqs.SQS) (*SQSReceiver, error) {
	c, err := NewSQSConsumer(sc, s)
	if err != nil {
		return nil, err
	}

	r := &SQSReceiver{
		SQSConsumer: c,

		c: make(chan Message),
	}

	go r.messageLoop()

	return r, nil
}

// MessageCh returns a receiving-only channel returns Message
func (r *SQSReceiver) MessageCh() <-chan Message {
	return r.c
}

func (r *SQSReceiver) messageLoop() {
	for {
		select {
		case <-r.shutdown:
			Debug("sqs: received signal to quit reading sqs, exiting goroutine")
			return
		case msg := <-r.Messages:
			deliverer := &SQSDeliverer{
				queue: r.Queue,
				msg:   msg,
			}

			delayAttr := findMessageAttribute(msg.MessageAttribute, "delayd-delay")
			delay, err := strconv.ParseInt(delayAttr.Value.StringValue, 10, 64)
			if err != nil {
				Warn(delayAttr)
				Warn("sqs: bad/missing delay. discarding message:", err)
				deliverer.Ack()
				continue
			}
			entry := &Entry{
				SendAt: time.Now().Add(time.Duration(delay) * time.Millisecond),
			}

			targetAttr := findMessageAttribute(msg.MessageAttribute, "delayd-target")
			if targetAttr.Value.DataType != "String" || targetAttr.Value.StringValue == "" {
				Warn("sqs: bad/missing target. discarding message")
				deliverer.Ack()
				continue
			}
			entry.Target = targetAttr.Value.StringValue

			keyAttr := findMessageAttribute(msg.MessageAttribute, "delayd-key")
			if keyAttr.Value.DataType == "String" && keyAttr.Value.StringValue != "" {
				entry.Key = keyAttr.Value.StringValue
			}

			entry.SQS = &SQSMessage{
				MessageID: msg.MessageId,
			}
			entry.Body = []byte(msg.Body)

			r.c <- Message{
				Entry:            entry,
				MessageDeliverer: deliverer,
			}
		}
	}
}

// Close closes a message ch and calls SQSConsumer's Close().
func (r *SQSReceiver) Close() {
	close(r.c)
	r.SQSConsumer.Close()
}

// SQSSender manages *sqs.SQS instance and a mapping of queue name to *sqs.Queue.
type SQSSender struct {
	sqs    *sqs.SQS
	queues map[string]*sqs.Queue
	mu     sync.Mutex
}

// NewSQSSender returns a SQSSender instance.
func NewSQSSender(s *sqs.SQS) *SQSSender {
	return &SQSSender{
		sqs:    s,
		queues: make(map[string]*sqs.Queue),
	}
}

func (s *SQSSender) addQueue(n string, q *sqs.Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[n] = q
}

func (s *SQSSender) queueExists(n string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.queues[n]
	return found
}

// Send sends a delayd entry over SQS, using the entry's Target as the queue name.
func (s *SQSSender) Send(e *Entry) error {
	if e.SQS == nil {
		return errors.New("sqs: invalid entry")
	}

	if !s.queueExists(e.Target) {
		q, err := s.sqs.GetQueue(e.Target)
		if err != nil {
			return err
		}
		s.addQueue(e.Target, q)
	}

	_, err := s.queues[e.Target].SendMessage(string(e.Body))
	return err
}

// Close is nop for SQS.
func (s *SQSSender) Close() {
}

func findMessageAttribute(attrs []sqs.MessageAttribute, name string) sqs.MessageAttribute {
	for _, ma := range attrs {
		if ma.Name == name {
			return ma
		}
	}
	return sqs.MessageAttribute{}
}
