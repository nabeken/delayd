package main

import (
	"flag"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"

	"github.com/nabeken/delayd"
	"github.com/nabeken/delayd/testutil"
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

func loadTestMessages(path string) (msgs []testutil.Message, err error) {
	messages := struct {
		Message []testutil.Message
	}{}
	delayd.Debug("reading", path)
	_, err = toml.DecodeFile(path, &messages)
	return messages.Message, err
}

func TestSQS(t *testing.T) {
	if !*flagSQS {
		t.Skip("Integration tests for SQS is disabled")
	}

	doIntegration(t, testutil.SQSClientFunc, testutil.ServerFunc(testutil.SQSServerFunc).ServersFunc())
}

func TestSQS_Multiple(t *testing.T) {
	if !*flagSQS || !*flagConsul {
		t.Skip("Integration tests for SQS with multiple delayd is disabled")
	}

	doIntegration_Multiple(t, testutil.SQSClientFunc, testutil.SQSServerFunc)
}

func TestAMQP(t *testing.T) {
	if !*flagAMQP {
		t.Skip("Integration tests for AMQP is disabled")
	}

	doIntegration(t, testutil.AMQPClientFunc, testutil.ServerFunc(testutil.AMQPServerFunc).ServersFunc())
}

func TestAMQP_Multiple(t *testing.T) {
	if !*flagAMQP || !*flagConsul {
		t.Skip("Integration tests for AMQP with multiple delayd is disabled")
	}

	doIntegration_Multiple(t, testutil.AMQPClientFunc, testutil.AMQPServerFunc)
}

func doIntegration_Multiple(t *testing.T, c testutil.ClientFunc, f testutil.ServerFunc) {
	os.Setenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK", "127.0.0.1")
	defer os.Setenv("DELAYD_CONSUL_AGENT_SERVICE_ADDRESS_TWEAK", "")

	sf := func(config delayd.Config) ([]*testutil.Server, error) {
		servers := []*testutil.Server{}

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

func doIntegration(t *testing.T, cf testutil.ClientFunc, sf testutil.ServersFunc) {
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
