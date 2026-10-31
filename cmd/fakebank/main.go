// fakebank: a pretend bank for testing payengine.
// Run it with some chaos to see how payengine copes:
//
//	go run ./cmd/fakebank -error-rate 0.2 -hang-rate 0.05 -lost-rate 0.2
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"payengine/internal/bank"
)

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	errRate := flag.Float64("error-rate", 0, "chance (0-1) the bank returns 500 without processing")
	hangRate := flag.Float64("hang-rate", 0, "chance (0-1) the bank doesn't answer, without processing")
	lostRate := flag.Float64("lost-rate", 0, "chance (0-1) the bank charges but the answer is lost")
	hangFor := flag.Duration("hang-for", 10*time.Second, "how long a hang lasts")
	flag.Parse()

	fake := bank.NewFake(bank.Chaos{
		ErrorRate: *errRate, HangRate: *hangRate, LostRate: *lostRate, HangFor: *hangFor,
	})
	log.Printf("fakebank on %s  (error=%.2f hang=%.2f lost=%.2f)  stats: GET /stats", *addr, *errRate, *hangRate, *lostRate)
	srv := &http.Server{Addr: *addr, Handler: fake.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
