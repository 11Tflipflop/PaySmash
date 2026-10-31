package e2e

import (
	"context"
	"sync"
	"testing"

	"payengine/internal/bank"
	"payengine/internal/ledger"
)

// The big one. The bank fails, hangs and loses answers at random while 8 workers
// race each other and clients retry their requests. At the end the books must be perfect.
func TestChaos_MoneyAlwaysAddsUp(t *testing.T) {
	e := newEnv(t, bank.Chaos{ErrorRate: 0.15, HangRate: 0.05, LostRate: 0.15})

	const total = 120
	type spec struct {
		key    string
		amount int64
		source string
	}
	specs := make([]spec, total)
	for i := range specs {
		amount := int64(100 + i*37)
		if i%7 == 0 {
			amount = int64(10 + i%5) // tiny amounts, fee is capped at the amount
		}
		source := "tok_visa"
		if i%5 == 0 {
			source = "tok_declined"
		}
		specs[i] = spec{keyN("chaos", i), amount, source}
	}

	// clients create payments concurrently, and every request is sent twice (a client retry)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	for _, s := range specs {
		for attempt := 0; attempt < 2; attempt++ {
			wg.Add(1)
			go func(s spec) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				code, _ := e.pay(s.key, s.amount, s.source)
				if code != 200 && code != 201 {
					t.Errorf("create %s: status %d", s.key, code)
				}
			}(s)
		}
	}
	wg.Wait()

	if got := e.count(`SELECT count(*) FROM payments`); got != total {
		t.Fatalf("payments = %d, want %d (duplicate requests must not create extra payments)", got, total)
	}

	cfg := e.workerCfg("chaos")
	cfg.MaxAttempts = 15 // enough that "never got through" is practically impossible
	e.drain(cfg, 8)

	// 1. everything settled the right way
	var wantSucceeded, wantFailed int
	var wantMerchant, wantFees, wantGross int64
	for _, s := range specs {
		if s.source == "tok_declined" {
			wantFailed++
			continue
		}
		wantSucceeded++
		fee := ledger.Fee(s.amount)
		wantGross += s.amount
		wantFees += fee
		wantMerchant += s.amount - fee
	}
	if got := e.count(`SELECT count(*) FROM payments WHERE status = 'succeeded'`); got != wantSucceeded {
		t.Errorf("succeeded = %d, want %d", got, wantSucceeded)
	}
	if got := e.count(`SELECT count(*) FROM payments WHERE status = 'failed'`); got != wantFailed {
		t.Errorf("failed = %d, want %d", got, wantFailed)
	}
	if got := e.count(`SELECT count(*) FROM payments WHERE status NOT IN ('succeeded','failed')`); got != 0 {
		t.Errorf("%d payments stuck in created/processing/review", got)
	}

	// 2. the whole ledger adds up to exactly zero
	if sum := e.sum(`SELECT COALESCE(SUM(amount), 0)::bigint FROM ledger_entries`); sum != 0 {
		t.Errorf("ledger sums to %d, want 0", sum)
	}

	// 3. each succeeded payment has exactly one ledger transaction, every other has none
	rows, err := e.pool.Query(context.Background(), `
		SELECT p.status, count(lt.id)
		FROM payments p LEFT JOIN ledger_transactions lt ON lt.payment_id = p.id
		GROUP BY p.id, p.status`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var status string
		var n int
		rows.Scan(&status, &n)
		want := 0
		if status == "succeeded" {
			want = 1
		}
		if n != want {
			t.Errorf("a %s payment has %d ledger transactions, want %d", status, n, want)
		}
	}
	rows.Close()

	// 4. balances match what we expect from the payments
	if v := e.accountBalance(ledger.MerchantAccount(e.merchantID)); v != wantMerchant {
		t.Errorf("merchant balance = %d, want %d", v, wantMerchant)
	}
	if v := e.accountBalance(ledger.AccountFees); v != wantFees {
		t.Errorf("fee balance = %d, want %d", v, wantFees)
	}
	if v := e.accountBalance(ledger.AccountCardNetwork); v != -wantGross {
		t.Errorf("card network balance = %d, want %d", v, -wantGross)
	}

	// 5. the bank charged each card once, no matter how many retries it took
	s := e.bank.Stats()
	if s.UniqueCharges != total {
		t.Errorf("bank processed %d unique charges, want %d", s.UniqueCharges, total)
	}
	if s.Approved != wantSucceeded {
		t.Errorf("bank approved %d, ledger has %d successful payments", s.Approved, wantSucceeded)
	}
	if s.Calls <= s.UniqueCharges {
		t.Errorf("bank got %d calls for %d charges: the chaos never caused a retry, test proves nothing", s.Calls, s.UniqueCharges)
	}
	t.Logf("bank calls: %d for %d charges (%d retries)", s.Calls, s.UniqueCharges, s.Calls-s.UniqueCharges)
}
