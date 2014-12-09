package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/codegangsta/cli"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"

	"github.com/nabeken/delayd"
)

// this is filled in at build time
var version string

type Stopper interface {
	Stop()
}

// installSigHandler installs a signal handler to shutdown gracefully for ^C and kill
func installSigHandler(s Stopper) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			// we should wait until s.Stop returns for 10 seconds.
			time.AfterFunc(10*time.Second, func() {
				delayd.Error("Server#Stop() does not return for 10 seconds. existing...")
				os.Exit(0)
			})
			s.Stop()
		}
	}()
}

func mergeConfig(c *cli.Context) delayd.Config {
	// load toml
	config, err := delayd.LoadConfig(c.String("config"))
	if err != nil {
		delayd.Fatal("cli: unable to read config file:", err)
	}

	// merge envvars
	if url := os.Getenv("AMQP_URL"); url != "" {
		config.AMQP.URL = url
	}

	if consulHost := os.Getenv("CONSUL_HOST"); consulHost != "" {
		config.Consul.Address = consulHost
	}

	if raftHost := os.Getenv("RAFT_HOST"); raftHost != "" {
		config.Raft.Listen = net.JoinHostPort(raftHost, c.String("port"))
	} else if configRaftHost := config.Raft.Listen; configRaftHost != "" {
		config.Raft.Listen = net.JoinHostPort(configRaftHost, c.String("port"))
	}

	if peers := os.Getenv("RAFT_PEERS"); peers != "" {
		config.Raft.Peers = strings.Split(peers, ",")
	}

	if sqsQueue := os.Getenv("SQS_QUEUE_NAME"); sqsQueue != "" {
		config.SQS.Queue = sqsQueue
	}

	// merge flags
	if advertise := c.String("advertise"); advertise == "" {
		advIP, err := delayd.GetPrivateIP()
		if err != nil {
			delayd.Fatal("failed to get advertise address:", err)
		}

		config.Raft.Advertise = net.JoinHostPort(advIP.String(), c.String("port"))
	} else {
		config.Raft.Advertise = net.JoinHostPort(advertise, c.String("port"))
	}

	config.Raft.Single = c.Bool("single")
	if !config.Raft.Single {
		config.UseConsul = c.Bool("consul")
	}

	if config.BootstrapExpect == 0 {
		config.BootstrapExpect = c.Int("bootstrap-expect")
	}

	if config.TickDuration > 0 {
		config.TickDuration *= time.Millisecond
	} else {
		config.TickDuration = delayd.DefaultTickDuration
	}

	return config
}

func execute(c *cli.Context) {
	config := mergeConfig(c)
	sender, receiver, err := getBroker(c.String("broker"), config)
	if err != nil {
		delayd.Fatal(err)
	}

	s, err := delayd.NewServer(config, sender, receiver)
	if err != nil {
		delayd.Fatal(err)
	}
	installSigHandler(s)

	delayd.Infof("cli: starting delayd with %s. Listen: %s, Adv: %s, StaticPeers: %v, Bootstrap: %v",
		c.String("broker"),
		config.Raft.Listen,
		config.Raft.Advertise,
		config.Raft.Peers,
		c.Bool("single"),
	)

	s.Run()
}

func main() {
	app := cli.NewApp()
	app.Name = "delayd"
	app.Usage = "available setTimeout()"
	app.Version = version

	flags := []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/delayd.toml",
			Usage: "config file",
		},
		cli.StringFlag{
			Name:  "broker, b",
			Value: "amqp",
			Usage: "specify a broker for queue. 'amqp' and 'sqs' is available.",
		},
		cli.StringFlag{
			Name:  "port, p",
			Value: "7999",
			Usage: "specify a port number for Raft RPC. Default: '7999'",
		},
		cli.StringFlag{
			Name:  "advertise",
			Value: "",
			Usage: "specify a advertise address for Raft RPC.",
		},
		cli.IntFlag{
			Name:  "bootstrap-expect",
			Value: 0,
			Usage: "specify a number of expected delayd instances.",
		},
		cli.BoolFlag{
			Name:  "single",
			Usage: "run raft single mode",
		},
		cli.BoolFlag{
			Name:  "consul",
			Usage: "use consul backend as peer management",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:        "server",
			ShortName:   "serv",
			Usage:       "Spawn a Delayd Server",
			Description: "Delay Daemon for replicated `setTimeout()`",
			Action:      execute,
			Flags:       flags,
		},
	}

	app.Run(os.Args)
}

func getBroker(b string, config delayd.Config) (delayd.Sender, delayd.Receiver, error) {
	switch b {
	case "amqp":
		sender, err := delayd.NewAMQPSender(config.AMQP.URL)
		if err != nil {
			return nil, nil, err
		}

		receiver, err := delayd.NewAMQPReceiver(config.AMQP, delayd.RoutingKey)
		if err != nil {
			return nil, nil, err
		}

		return sender, receiver, nil

	case "sqs":
		region, found := aws.Regions[config.SQS.Region]
		if !found {
			return nil, nil, fmt.Errorf("region %s is not valid", config.SQS.Region)
		}

		auth, err := aws.GetAuth("", "", "", time.Now())
		if err != nil {
			return nil, nil, err
		}
		s := sqs.New(auth, region)

		receiver, err := delayd.NewSQSReceiver(config.SQS, s)
		if err != nil {
			return nil, nil, err
		}

		sender := delayd.NewSQSSender(s)

		return sender, receiver, nil
	}

	return nil, nil, errors.New("delayd: unknown broker is specified")
}
