package main

import (
	"flag"
	"testing"

	"github.com/nabeken/delayd/testutil"
)

var (
	flagAMQP   = flag.Bool("amqp", false, "Enable integration tests against AMQP")
	flagSQS    = flag.Bool("sqs", false, "Enable integration tests against SQS")
	flagConsul = flag.Bool("consul", false, "Enable integraton tests with Consul")
)

const bootstrapExpect = 3

func TestSQS(t *testing.T) {
	if !*flagSQS {
		t.Skip("Integration tests for SQS is disabled")
	}

	testutil.DoIntegration(t, testutil.SQSClientFunc, testutil.ServerFunc(testutil.SQSServerFunc).ServersFunc())
}

func TestSQS_Multiple(t *testing.T) {
	if !*flagSQS || !*flagConsul {
		t.Skip("Integration tests for SQS with multiple delayd is disabled")
	}

	testutil.DoIntegrationMultiple(t, bootstrapExpect, testutil.SQSClientFunc, testutil.SQSServerFunc)
}

func TestAMQP(t *testing.T) {
	if !*flagAMQP {
		t.Skip("Integration tests for AMQP is disabled")
	}

	testutil.DoIntegration(t, testutil.AMQPClientFunc, testutil.ServerFunc(testutil.AMQPServerFunc).ServersFunc())
}

func TestAMQP_Multiple(t *testing.T) {
	if !*flagAMQP || !*flagConsul {
		t.Skip("Integration tests for AMQP with multiple delayd is disabled")
	}

	testutil.DoIntegrationMultiple(t, bootstrapExpect, testutil.AMQPClientFunc, testutil.AMQPServerFunc)
}
