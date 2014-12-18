package main

import (
	"fmt"
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"

	"github.com/nabeken/delayd"
)

func execute(c *cli.Context) {
	config, err := delayd.LoadConfig(c.String("config"))
	if err != nil {
		delayd.Fatal("cli: unable to read config file:", err)
	}

	// override configuration by envvars
	if sqsQueue := os.Getenv("DELAYD_SQS_QUEUE_NAME"); sqsQueue != "" {
		config.SQS.Queue = sqsQueue
	}

	region, found := aws.Regions[config.SQS.Region]
	if !found {
		panic(fmt.Errorf("region %s is not valid", config.SQS.Region))
	}

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		panic(err)
	}
	s := sqs.New(auth, region)

	delaydQueue, err := s.GetQueue(config.SQS.Queue)
	if err != nil {
		delayd.Fatal(err)
	}

	attrs := map[string]string{
		"delayd-delay":  c.String("delay"),
		"delayd-target": c.String("target"),
	}
	if _, err := delaydQueue.SendMessageWithAttributes(c.String("message"), attrs); err != nil {
		delayd.Fatal("failed to send a message to delayd", err)
	}

	delayd.Info("message is delivered")
}

func main() {
	app := cli.NewApp()
	app.Name = "delayd-sqsctl"
	app.Usage = "command-line tool for delayd with SQS"

	flags := []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/delayd.toml",
			Usage: "config file",
		},
		cli.StringFlag{
			Name:  "delay, d",
			Value: "10",
			Usage: "specify a delay is ms.",
		},
		cli.StringFlag{
			Name:  "target, t",
			Value: "",
			Usage: "specify a target",
		},
		cli.StringFlag{
			Name:  "message, m",
			Value: "",
			Usage: "specify a message to be sent",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "send",
			Usage:  "send a message to Delayd Server",
			Action: execute,
			Flags:  flags,
		},
	}

	app.Run(os.Args)
}
