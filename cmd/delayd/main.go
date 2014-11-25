package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
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
			s.Stop()
			os.Exit(0)
		}
	}()
}

func execute(c *cli.Context) {
	config, err := loadConfig(c.String("config"))
	if err != nil {
		delayd.Fatal("cli: unable to read config file:", err)
	}

	// override configuration by envvars
	if url := os.Getenv("AMQP_URL"); url != "" {
		config.AMQP.URL = url
	}
	if sqsQueue := os.Getenv("SQS_QUEUE_NAME"); sqsQueue != "" {
		config.SQS.Queue = sqsQueue
	}
	if raftHost := os.Getenv("RAFT_HOST"); raftHost != "" {
		config.Raft.Listen = raftHost
	}
	if peers := os.Getenv("RAFT_PEERS"); peers != "" {
		config.Raft.Peers = strings.Split(peers, ",")
	}

	// override raft single mode settings by flag
	config.Raft.Single = c.Bool("single")

	sender, receiver, err := getBroker(c.String("broker"), config)
	if err != nil {
		panic(err)
	}

	s, err := delayd.NewServer(config, sender, receiver)
	if err != nil {
		panic(err)
	}
	installSigHandler(s)

	delayd.Infof("cli: starting delayd with %s", c.String("broker"))
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
		cli.BoolFlag{
			Name:  "single",
			Usage: "run raft single mode",
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

// loadConfig loads delayd's toml configuration
func loadConfig(path string) (config delayd.Config, err error) {
	_, err = toml.DecodeFile(path, &config)
	return
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
