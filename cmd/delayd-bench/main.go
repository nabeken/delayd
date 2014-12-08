package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/nabeken/delayd"
	"github.com/nabeken/delayd/testutil"
)

var (
	flagNumMsg   = flag.Int("n", 10, "Specify a number of messages to send")
	flagDuration = flag.Duration("d", 1*time.Second, "Specify a delay. Default is 1s.")
	flagConfig   = flag.String("c", "/etc/delayd.toml", "Specify a config. Default is /etc/delayd.toml")
	flagProfile  = flag.String("p", "", "Specify a output file for cpu profiling.")
)

// mps returns message per seconds.
func mps(n int, d time.Duration) float64 {
	return float64(n) / d.Seconds()
}

func generateMessages(n int, d time.Duration) []testutil.Message {
	msgs := make([]testutil.Message, n)
	for i := 0; i < n; i++ {
		msgs[i] = testutil.Message{
			Value: strconv.FormatInt(int64(i+1), 10),
			Delay: int64(d / time.Millisecond),
		}
	}
	return msgs
}

func main() {
	flag.Parse()

	config, err := delayd.LoadConfig(*flagConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Use stdout instead of file
	config.LogDir = ""

	client, err := testutil.AMQPClientFunc(config, ioutil.Discard)
	if err != nil {
		log.Fatal(err)
	}

	s, err := testutil.AMQPServerFunc(config)
	if err != nil {
		log.Fatal(err)
	}

	if *flagProfile != "" {
		f, err := os.Create(*flagProfile)
		if err != nil {
			delayd.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	go s.Run()
	defer s.Stop()

	// Send messages to delayd exchange
	msgs := generateMessages(*flagNumMsg, *flagDuration)
	n := len(msgs)

	start := time.Now()
	if err := client.SendMessages(msgs); err != nil {
		log.Println(err)
		return
	}
	sent := time.Now()

	durationSent := sent.Sub(start)
	delayd.Infof("sent %d messages for %s, %f msg/s", n, durationSent, mps(n, durationSent))

	var wg sync.WaitGroup
	wg.Add(len(msgs))
	go func() {
		for _ = range client.RecvLoop() {
			wg.Done()
		}
	}()

	// Wait for messages to be processed
	wg.Wait()
	done := time.Now()

	durationProcessed := done.Sub(sent)
	durationAll := done.Sub(start)

	delayd.Infof("processed %d messages for %s, %f msg/s", n, durationProcessed, mps(n, durationProcessed))
	delayd.Infof("total: processed %d messages for %s, %f msg/s", n, durationAll, mps(n, durationAll))

	// shutdown consumer
	client.Close()
}