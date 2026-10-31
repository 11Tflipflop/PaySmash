package bank

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Chaos makes the fake bank misbehave, like real networks do.
// Rates are between 0 and 1.
type Chaos struct {
	ErrorRate float64       // bank returns 500 and did NOT process the charge
	HangRate  float64       // bank doesn't answer (client times out) and did NOT process the charge
	LostRate  float64       // bank DID process the charge, but the answer is lost (500 or no reply)
	HangFor   time.Duration // how long a hang lasts (should be longer than the client timeout)
	Latency   time.Duration // every answer is delayed by this much (a slow bank)
}

type Stats struct {
	Calls         int `json:"calls"`          // every request that arrived
	UniqueCharges int `json:"unique_charges"` // distinct idempotency keys the bank processed
	Approved      int `json:"approved"`       // of those, how many were approved
}

// Fake is a pretend bank. It remembers the result for every idempotency key,
// like a real one does, so repeating a request never charges twice.
//
// Test card tokens (the "source"):
//
//	tok_visa, tok_mastercard   approved
//	tok_declined               declined: card_declined
//	tok_insufficient_funds     declined: insufficient_funds
//	anything else              declined: invalid_source
type Fake struct {
	mu      sync.Mutex
	results map[string]Result
	calls   int
	chaos   Chaos
	rng     *rand.Rand
}

func NewFake(c Chaos) *Fake {
	if c.HangFor == 0 {
		c.HangFor = 10 * time.Second
	}
	return &Fake{
		results: map[string]Result{},
		chaos:   c,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (f *Fake) SetChaos(c Chaos) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.HangFor == 0 {
		c.HangFor = 10 * time.Second
	}
	f.chaos = c
}

func (f *Fake) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := Stats{Calls: f.calls, UniqueCharges: len(f.results)}
	for _, r := range f.results {
		if r.Approved {
			s.Approved++
		}
	}
	return s
}

func (f *Fake) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /charge", f.handleCharge)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f.Stats())
	})
	return mux
}

func (f *Fake) decide(key string, req ChargeRequest) Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	if res, ok := f.results[key]; ok {
		return res // same key as before -> same answer, no new charge
	}
	var res Result
	switch req.Source {
	case "tok_visa", "tok_mastercard":
		res = Result{Approved: true}
	case "tok_declined":
		res = Result{Reason: "card_declined"}
	case "tok_insufficient_funds":
		res = Result{Reason: "insufficient_funds"}
	default:
		res = Result{Reason: "invalid_source"}
	}
	f.results[key] = res
	return res
}

func (f *Fake) hang(r *http.Request, d time.Duration) {
	select {
	case <-time.After(d):
	case <-r.Context().Done():
	}
}

func (f *Fake) handleCharge(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}
	var req ChargeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.calls++
	c := f.chaos
	roll1, roll2 := f.rng.Float64(), f.rng.Float64()
	f.mu.Unlock()

	// failures where the bank never processed anything
	if roll1 < c.ErrorRate {
		http.Error(w, "bank is having a bad day", http.StatusInternalServerError)
		return
	}
	if roll1 < c.ErrorRate+c.HangRate {
		f.hang(r, c.HangFor)
		return
	}

	res := f.decide(key, req)

	// the nasty one: charge went through, but we never hear about it
	if roll2 < c.LostRate {
		if roll2 < c.LostRate/2 {
			http.Error(w, "connection reset", http.StatusInternalServerError)
		} else {
			f.hang(r, c.HangFor)
		}
		return
	}

	if c.Latency > 0 {
		f.hang(r, c.Latency)
	}

	w.Header().Set("Content-Type", "application/json")
	if res.Approved {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusPaymentRequired)
	}
	json.NewEncoder(w).Encode(res)
}
