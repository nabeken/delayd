package delayd

import (
	"github.com/BurntSushi/toml"
	"time"
)

// AMQPQueue holds configuration for the queue used by the AMQPReceiver
type AMQPQueue struct {
	Name       string   `toml:"name"`
	Bind       []string `toml:"bind"`
	Durable    bool     `toml:"durable"`
	AutoDelete bool     `toml:"auto_delete"`
	AutoAck    bool     `toml:"auto_ack"`
	Exclusive  bool     `toml:"exclusive"`
	NoLocal    bool     `toml:"no_local"`
	NoWait     bool     `toml:"no_wait"`
}

// AMQPExchange holds configuration for the exchange used by the AMQPReceiver
type AMQPExchange struct {
	Name       string `toml:"name"`
	Kind       string `toml:"kind"`
	AutoDelete bool   `toml:"auto_delete"`
	Durable    bool   `toml:"durable"`
	Internal   bool   `toml:"internal"`
	NoWait     bool   `toml:"no_wait"`
}

// AMQPConfig holds configuration for AMQP senders and receivers.
type AMQPConfig struct {
	URL      string       `toml:"url"`
	Qos      int          `toml:"qos"`
	Exchange AMQPExchange `toml:"exchange"`
	Queue    AMQPQueue    `toml:"queue"`
}

// SQSConfig holds configuration for SQS.
type SQSConfig struct {
	Queue               string `toml:"queue"`
	Region              string `toml:"region"`
	MaxNumberOfMessages int    `toml:"max_number_messages"`
	VisibilityTimeout   int    `toml:"visibility_timeout"`
}

// RaftConfig holds configuration for Raft concensus
type RaftConfig struct {
	Single    bool     `toml:"single_node"`
	Peers     []string `toml:"peers"`
	Listen    string   `toml:"listen"`
	Advertise string   `toml:"advertise"`
}

// ConsulConfig holds configuration for Consul Agent
type ConsulConfig struct {
	Address string `toml:"address"`
}

// Config holds delayd configuration
type Config struct {
	AMQP   AMQPConfig   `toml:"amqp"`
	Raft   RaftConfig   `toml:"raft"`
	SQS    SQSConfig    `toml:"sqs"`
	Consul ConsulConfig `toml:"consul"`

	DataDir string `toml:"data_dir"`
	LogDir  string `toml:"log_dir"`

	TickDuration time.Duration `toml:"tick_duration"`

	BootstrapExpect int `toml:"bootstrap_expect"`

	UseConsul bool `toml:"use_consul"`
}

// LoadConfig loads delayd's toml configuration
func LoadConfig(path string) (config Config, err error) {
	_, err = toml.DecodeFile(path, &config)
	return
}
