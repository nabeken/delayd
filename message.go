package delayd

import (
	"time"

	"github.com/crowdmob/goamz/sqs"
	"github.com/streadway/amqp"
	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

// Message represents a message with a broker-agnostic way.
// It holds Entry object and also a broker-specific message manager.
type Message struct {
	*Entry
	MessageDeliverer
}

// MessageDeliverer is an interface for a broker-specific message
// acknowledgment.
type MessageDeliverer interface {
	Ack() error
	Nack() error
}

// AMQPDeliverer implements MessageDeliverer for AMQP.
type AMQPDeliverer struct {
	amqp.Delivery
}

// Ack acknowledgs a message via AMQP.
func (d *AMQPDeliverer) Ack() error {
	return d.Delivery.Ack(false)
}

// Nack returns a negative acknowledgement via AMQP.
func (d *AMQPDeliverer) Nack() error {
	return d.Delivery.Nack(false, false)
}

// SQSDeliverer implements MessageDeliverer for SQS.
type SQSDeliverer struct {
	queue *sqs.Queue
	msg   *sqs.Message
}

// Ack deletes a message in the queue.
func (d *SQSDeliverer) Ack() error {
	_, err := d.queue.DeleteMessage(d.msg)
	return err
}

// Nack does nothing because no need to return a negative acknowledgement in SQS.
// Message will be available after visibility timeout expires.
func (d *SQSDeliverer) Nack() error {
	return nil
}

// Entry represents a delayed message.
type Entry struct {
	// Required
	SendAt time.Time
	Target string
	Body   []byte

	// AMQP specific message
	AMQP *AMQPMessage

	// SQS specific message
	SQS *SQSMessage
}

// AMQPMessage represents AMQP specific message.
type AMQPMessage struct {
	ContentEncoding string
	ContentType     string
	CorrelationID   string
}

// SQSMessage represents SQS specific message.
type SQSMessage struct {
	MessageID string
}

// entryFromBytes creates a new Entry based on the MessagePack encoded byte slice b.
func entryFromBytes(b []byte) (*Entry, error) {
	e := &Entry{}
	dec := codec.NewDecoderBytes(b, &mh)
	if err := dec.Decode(e); err != nil {
		return nil, err
	}
	return e, nil
}

// ToBytes encodes an Entry to a byte slice, encoding with MessagePack
func (e *Entry) ToBytes() (b []byte, err error) {
	enc := codec.NewEncoderBytes(&b, &mh)
	err = enc.Encode(e)
	return
}
