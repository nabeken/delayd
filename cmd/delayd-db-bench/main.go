package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/armon/gomdb"

	"github.com/nabeken/delayd"
)

var (
	flagNumMsg = flag.Int("n", 10, "Specify a number of messages to write")
	flagSync   = flag.Bool("s", true, "Use fsync for LMDB")
)

// eps returns entry per seconds.
func eps(n int, d time.Duration) float64 {
	return float64(n) / d.Seconds()
}

func generateUUIDs(n int) [][]byte {
	uuids := make([][]byte, n)
	for i := 0; i < n; i++ {
		uuid, err := delayd.NewUUID()
		if err != nil {
			panic(err)
		}
		uuids[i] = uuid
	}
	return uuids
}

func generateEntries(n int) []*delayd.Entry {
	now := time.Now()
	target := "delayd-target"
	body := []byte("ABC")
	entries := make([]*delayd.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = &delayd.Entry{
			SendAt: now,
			Target: target,
			Body:   body,
		}
	}
	return entries
}

func main() {
	flag.Parse()
	n := *flagNumMsg

	s := &delayd.Storage{}
	var flags uint = mdb.NOTLS

	if !*flagSync {
		flags |= mdb.NOMETASYNC | mdb.NOSYNC
	}

	if err := s.InitDB(flags); err != nil {
		log.Fatal(err)
	}

	entries := generateEntries(n)
	uuids := generateUUIDs(n)

	var wg sync.WaitGroup
	wg.Add(n)

	begin := time.Now()
	go func() {
		for i := 0; i < *flagNumMsg; i++ {
			s.Get(time.Now())
			if err := s.Add(uuids[i], entries[i]); err != nil {
				log.Fatal(err)
			}
			if err := s.Remove(uuids[i]); err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}
	}()

	wg.Wait()
	done := time.Now()
	durationProcessed := done.Sub(begin)

	delayd.Infof("processed %d entries for %s, %f entry/s",
		n, durationProcessed, eps(n, durationProcessed))
}
