package e2e

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"payengine/internal/bank"
	"payengine/internal/ledger"
	"payengine/internal/worker"
)

// 100 requests with the same Idempotency-Key at the same moment -> exactly ONE payment.
func TestIdempotency_ConcurrentSameKey(t *testing.T) {
	e := newEnv(t, bank.Chaos{})

	const n = 100
	codes := make([]int, n)
	ids := make([]string, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, p := e.pay("order-1", 5000, "tok_visa")
			codes[i], ids[i] = code, p.ID
		}(i)
	}
	close(start)
	wg.Wait()

	created := 0
	for i := 0; i < n; i++ {
		switch codes[i] {
		case 201:
			created++
		case 200: // replayed
		default:
			t.Fatalf("request %d: unexpected status %d", i, codes[i])
		}
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("request %d got payment id %q, want %q", i, ids[i], ids[0])
		}
	}
	if created != 1 {
		t.Fatalf("%d requests created a payment, want exactly 1", created)
	}
	if got := e.count(`SELECT count(*) FROM payments`); got != 1 {
		t.Fatalf("payments rows = %d, want 1", got)
	}
	if got := e.count(`SELECT count(*) FROM jobs`); got != 1 {
		t.Fatalf("jobs rows = %d, want 1", got)
	}
}

func TestIdempotency_ReplayAndMismatch(t *testing.T) {
	e := newEnv(t, bank.Chaos{})

	code, first := e.pay("order-7", 1000, "tok_visa")
	if code != 201 {
		t.Fatalf("first call: %d", code)
	}

	// same key, same body -> 200 + the same payment, marked as replayed
	h := e.auth()
	h["Idempotency-Key"] = "order-7"
	status, hdr, raw := e.request("POST", "/v1/payments", h,
		map[string]any{"amount": 1000, "currency": "usd", "source": "tok_visa"})
	if status != 200 || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: status %d, replayed header %q", status, hdr.Get("Idempotent-Replayed"))
	}
	if !strings.Contains(string(raw), first.ID) {
		t.Fatalf("replay returned a different payment: %s", raw)
	}

	// same key, different amount -> 422
	status, _, raw = e.request("POST", "/v1/payments", h,
		map[string]any{"amount": 2000, "currency": "usd", "source": "tok_visa"})
	if status != 422 || !strings.Contains(string(raw), "idempotency_key_reused") {
		t.Fatalf("mismatch: status %d body %s", status, raw)
	}
	if got := e.count(`SELECT count(*) FROM payments`); got != 1 {
		t.Fatalf("payments rows = %d, want 1", got)
	}
}

func TestAPI_ValidationAndAuth(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	h := e.auth()
	h["Idempotency-Key"] = "k"

	cases := []struct {
		name string
		body map[string]any
	}{
		{"amount zero", map[string]any{"amount": 0, "currency": "usd", "source": "tok_visa"}},
		{"negative amount", map[string]any{"amount": -5, "currency": "usd", "source": "tok_visa"}},
		{"bad currency", map[string]any{"amount": 5, "currency": "doge", "source": "tok_visa"}},
		{"unknown field", map[string]any{"amount": 5, "currency": "usd", "source": "tok_visa", "extra": 1}},
	}
	for _, c := range cases {
		if status, _, raw := e.request("POST", "/v1/payments", h, c.body); status != 400 {
			t.Errorf("%s: status %d, want 400 (%s)", c.name, status, raw)
		}
	}

	// no Idempotency-Key
	if status, _, _ := e.request("POST", "/v1/payments", e.auth(), map[string]any{"amount": 5, "currency": "usd", "source": "tok_visa"}); status != 400 {
		t.Errorf("missing idempotency key: status %d, want 400", status)
	}
	// no api key / wrong api key
	if status, _, _ := e.request("POST", "/v1/payments", map[string]string{"Idempotency-Key": "x"}, nil); status != 401 {
		t.Errorf("no auth: status %d, want 401", status)
	}
	if status, _, _ := e.request("GET", "/v1/balance", map[string]string{"Authorization": "Bearer sk_test_wrong"}, nil); status != 401 {
		t.Errorf("wrong key: status %d, want 401", status)
	}
	if got := e.count(`SELECT count(*) FROM payments`); got != 0 {
		t.Errorf("invalid requests created %d payments", got)
	}
}

func TestAPI_MerchantsCantSeeEachOther(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("k1", 1000, "tok_visa")

	// a second merchant
	var otherKey string
	_, otherKey = createMerchant(t, e, "Other Shop")

	status, _, _ := e.request("GET", "/v1/payments/"+p.ID, map[string]string{"Authorization": "Bearer " + otherKey}, nil)
	if status != 404 {
		t.Fatalf("other merchant got status %d, want 404", status)
	}
	// garbage id is a 404 too, not a 500
	if status, _, _ := e.request("GET", "/v1/payments/not-a-uuid", e.auth(), nil); status != 404 {
		t.Fatalf("bad id: status %d, want 404", status)
	}
}

func TestHappyPath_LedgerAndBalance(t *testing.T) {
	e := newEnv(t, bank.Chaos{})

	code, p := e.pay("happy-1", 10000, "tok_visa")
	if code != 201 || p.Status != "created" {
		t.Fatalf("create: %d %+v", code, p)
	}
	e.drain(e.workerCfg("w1"), 2)

	got := e.getPayment(p.ID)
	if got.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}

	// 10000 charged, fee 2.9% + 30 = 320
	if v := e.accountBalance(ledger.AccountCardNetwork); v != -10000 {
		t.Errorf("card network = %d, want -10000", v)
	}
	if v := e.accountBalance(ledger.MerchantAccount(e.merchantID)); v != 9680 {
		t.Errorf("merchant = %d, want 9680", v)
	}
	if v := e.accountBalance(ledger.AccountFees); v != 320 {
		t.Errorf("fees = %d, want 320", v)
	}

	status, _, raw := e.request("GET", "/v1/balance", e.auth(), nil)
	var bal struct {
		Balances map[string]int64 `json:"balances"`
	}
	json.Unmarshal(raw, &bal)
	if status != 200 || bal.Balances["usd"] != 9680 {
		t.Errorf("GET /v1/balance: %d %s", status, raw)
	}
	if s := e.bank.Stats(); s.UniqueCharges != 1 {
		t.Errorf("bank charges = %d, want 1", s.UniqueCharges)
	}
}

func TestDeclinedCard_FailsWithNoLedgerEntries(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("declined-1", 3000, "tok_declined")
	e.drain(e.workerCfg("w1"), 1)

	got := e.getPayment(p.ID)
	if got.Status != "failed" || got.FailureReason != "card_declined" {
		t.Fatalf("got %s / %q, want failed / card_declined", got.Status, got.FailureReason)
	}
	if n := e.count(`SELECT count(*) FROM ledger_entries`); n != 0 {
		t.Fatalf("a declined payment left %d ledger entries", n)
	}
}

// A worker takes a job and dies. Nobody else may touch the job until the lease runs out,
// and after that another worker must finish it.
func TestCrashRecovery_LeaseExpires(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("crash-1", 2500, "tok_visa")

	// pretend "dead-worker" claimed it and then died, lease ends in 700ms
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE jobs SET locked_by = 'dead-worker', attempts = 1,
		        lease_expires_at = now() + interval '700 milliseconds' WHERE payment_id = $1::uuid`, p.ID); err != nil {
		t.Fatal(err)
	}

	w := worker.New(e.workerCfg("rescuer"))
	ctx := context.Background()

	did, err := w.RunOnce(ctx)
	if err != nil || did {
		t.Fatalf("worker took a job that is still leased (did=%v err=%v)", did, err)
	}
	if st := e.getPayment(p.ID).Status; st != "created" {
		t.Fatalf("status = %s, want created", st)
	}

	time.Sleep(900 * time.Millisecond) // lease runs out

	did, err = w.RunOnce(ctx)
	if err != nil || !did {
		t.Fatalf("worker did not pick up the expired job (did=%v err=%v)", did, err)
	}
	if st := e.getPayment(p.ID).Status; st != "succeeded" {
		t.Fatalf("status = %s, want succeeded", st)
	}
	if n := e.count(`SELECT count(*) FROM ledger_transactions`); n != 1 {
		t.Fatalf("ledger transactions = %d, want 1", n)
	}
}

// The scary one: the first attempt reached the bank and the card WAS charged,
// but the worker died before saving anything. The retry must not charge again.
func TestCrashRecovery_BankAlreadyCharged(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("lost-1", 4000, "tok_visa")

	c := bank.NewClient(e.bankSrv.URL, time.Second)
	if _, err := c.Charge(context.Background(), p.ID, bank.ChargeRequest{Amount: 4000, Currency: "usd", Source: "tok_visa"}); err != nil {
		t.Fatal(err)
	}

	e.drain(e.workerCfg("w1"), 1) // worker knows nothing about the first attempt

	if st := e.getPayment(p.ID).Status; st != "succeeded" {
		t.Fatalf("status = %s, want succeeded", st)
	}
	s := e.bank.Stats()
	if s.Calls != 2 || s.UniqueCharges != 1 || s.Approved != 1 {
		t.Fatalf("bank stats %+v: want 2 calls but only 1 real charge", s)
	}
	if n := e.count(`SELECT count(*) FROM ledger_transactions`); n != 1 {
		t.Fatalf("ledger transactions = %d, want 1", n)
	}
}

// Bank is down for good: after MaxAttempts the payment must go to review,
// NOT to failed (we don't know if the card was charged) and NOT to the ledger.
func TestRetryExhaustion_GoesToReview(t *testing.T) {
	e := newEnv(t, bank.Chaos{ErrorRate: 1})
	_, p := e.pay("down-1", 1500, "tok_visa")

	cfg := e.workerCfg("w1")
	cfg.MaxAttempts = 3
	e.drain(cfg, 1)

	got := e.getPayment(p.ID)
	if got.Status != "review" || got.FailureReason != "bank_unreachable" {
		t.Fatalf("got %s / %q, want review / bank_unreachable", got.Status, got.FailureReason)
	}
	if n := e.count(`SELECT count(*) FROM ledger_entries`); n != 0 {
		t.Fatalf("review payment left %d ledger entries", n)
	}
	if a := e.count(`SELECT attempts FROM jobs WHERE payment_id = $1::uuid`, p.ID); a != 3 {
		t.Fatalf("attempts = %d, want 3", a)
	}
	if s := e.bank.Stats(); s.Calls != 3 {
		t.Fatalf("bank calls = %d, want 3", s.Calls)
	}
}

// A worker dies on its very last allowed attempt. Nobody is left to send it to review,
// so the sweep has to do it.
func TestSweep_DeadWorkerOnLastAttempt(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("sweep-1", 900, "tok_visa")

	cfg := e.workerCfg("w1")
	cfg.MaxAttempts = 3
	// attempts used up, lease already expired, job not done
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE jobs SET attempts = 3, locked_by = 'dead', lease_expires_at = now() - interval '1 second'
		 WHERE payment_id = $1::uuid`, p.ID); err != nil {
		t.Fatal(err)
	}
	e.drain(cfg, 1)

	got := e.getPayment(p.ID)
	if got.Status != "review" || got.FailureReason != "attempts_exhausted" {
		t.Fatalf("got %s / %q, want review / attempts_exhausted", got.Status, got.FailureReason)
	}
}

// Queues deliver at least once. If the same job shows up again after the payment
// is finished, nothing must happen: no second bank call, no second ledger entry.
func TestDuplicateDelivery_IsHarmless(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	_, p := e.pay("dup-1", 7000, "tok_visa")
	e.drain(e.workerCfg("w1"), 1)

	// put the finished job back in the queue
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE jobs SET done = false, run_at = now(), attempts = 0, locked_by = NULL, lease_expires_at = NULL
		 WHERE payment_id = $1::uuid`, p.ID); err != nil {
		t.Fatal(err)
	}
	e.drain(e.workerCfg("w2"), 2)

	if st := e.getPayment(p.ID).Status; st != "succeeded" {
		t.Fatalf("status = %s", st)
	}
	if n := e.count(`SELECT count(*) FROM ledger_transactions`); n != 1 {
		t.Fatalf("ledger transactions = %d, want 1", n)
	}
	if s := e.bank.Stats(); s.Calls != 1 {
		t.Fatalf("bank calls = %d, want 1 (the duplicate must not call the bank)", s.Calls)
	}
}

// Many workers, many payments, nothing double processed.
func TestManyWorkersNoDoubleProcessing(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	const n = 60
	for i := 0; i < n; i++ {
		if code, _ := e.pay(keyN("many", i), int64(1000+i), "tok_visa"); code != 201 {
			t.Fatalf("create %d: %d", i, code)
		}
	}
	e.drain(e.workerCfg("w"), 8)

	if got := e.count(`SELECT count(*) FROM payments WHERE status = 'succeeded'`); got != n {
		t.Fatalf("succeeded = %d, want %d", got, n)
	}
	if got := e.count(`SELECT count(*) FROM ledger_transactions`); got != n {
		t.Fatalf("ledger transactions = %d, want %d", got, n)
	}
	if s := e.bank.Stats(); s.Calls != n {
		t.Fatalf("bank calls = %d, want exactly %d (one per payment)", s.Calls, n)
	}
}

// A slow worker is still waiting for the bank when its lease runs out, and a second
// worker takes over the same job. Both end up trying to finish the payment at about
// the same time. Only one may post to the ledger, and neither may blow up.
func TestSlowWorker_LeaseTakenOver(t *testing.T) {
	e := newEnv(t, bank.Chaos{Latency: 500 * time.Millisecond})
	_, p := e.pay("slow-1", 6000, "tok_visa")

	mk := func(id string) *worker.Worker {
		cfg := e.workerCfg(id)
		cfg.Lease = 150 * time.Millisecond
		cfg.Bank = bank.NewClient(e.bankSrv.URL, 3*time.Second)
		return worker.New(cfg)
	}
	a, b := mk("slow-A"), mk("slow-B")

	ctx := context.Background()
	var wg sync.WaitGroup
	var errA, errB error
	var didB bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errA = a.RunOnce(ctx)
	}()
	time.Sleep(300 * time.Millisecond) // A's lease is over, but A is still waiting for the bank
	wg.Add(1)
	go func() {
		defer wg.Done()
		didB, errB = b.RunOnce(ctx)
	}()
	wg.Wait()

	if errA != nil || errB != nil {
		t.Fatalf("a worker failed: A=%v B=%v", errA, errB)
	}
	if !didB {
		t.Fatal("worker B never took over the job, so this test didn't test the race")
	}
	if st := e.getPayment(p.ID).Status; st != "succeeded" {
		t.Fatalf("status = %s, want succeeded", st)
	}
	if n := e.count(`SELECT count(*) FROM ledger_transactions`); n != 1 {
		t.Fatalf("ledger transactions = %d, want 1", n)
	}
	if got := e.sum(`SELECT COALESCE(SUM(amount), 0)::bigint FROM ledger_entries`); got != 0 {
		t.Fatalf("ledger sums to %d, want 0", got)
	}
	if n := e.count(`SELECT attempts FROM jobs WHERE payment_id = $1::uuid`, p.ID); n != 2 {
		t.Fatalf("job attempts = %d, want 2 (both workers claimed it)", n)
	}
	if s := e.bank.Stats(); s.Calls != 2 || s.UniqueCharges != 1 {
		t.Fatalf("bank stats %+v: want 2 calls, 1 charge", s)
	}
}
