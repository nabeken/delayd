package delayd

import (
	"github.com/armon/consul-api"
)

// NewConsul returns consul client.
func NewConsul(c ConsulConfig) (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()

	// Use default endpoint if address is not set explicitly
	if c.Address != "" {
		config.Address = c.Address
	}
	return consulapi.NewClient(config)
}
