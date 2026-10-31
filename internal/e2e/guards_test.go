package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"payengine/internal/bank"
	"payengine/internal/merchants"
)

func keyN(prefix string, i int) string { return fmt.Sprintf("%s-%d", prefix, i) }

func createMerchant(t *testing.T, e *env, name string) (id, key string) {
	t.Helper()
	id, key, err := merchants.Create(context.Background(), e.pool, name)
	if err != nil {
		t.Fatal(err)
	}
	return id, key
}

// insert a payment straight into the database, bypassing the app, and walk it to a status
func (e *env) rawPayment(key, finalStatus string) string {
	e.t.Helper()
	ctx := context.Background()
	var id string
	err := e.pool.QueryRow(ctx, `
		INSERT INTO payments (merchant_id, idempotency_key, request_hash, amount, currency, source)
		VALUES ($1, $2, 'x', 100, 'usd', 'tok_visa') RETURNING id::text`, e.merchantID, key).Scan(&id)
	if err != nil {
		e.t.Fatal(err)
	}
	steps := map[string][]string{
		"created":    {},
		"processing": {"processing"},
		"succeeded":  {"processing", "succeeded"},
	}[finalStatus]
	for _, s := range steps {
		if _, err := e.pool.Exec(ctx, `UPDATE payments SET status = $2 WHERE id = $1::uuid`, id, s); err != nil {
			e.t.Fatal(err)
		}
	}
	return id
}

func mustFail(t *testing.T, err error, wantSubstring string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error containing %q, but it worked", wantSubstring)
	}
	if !strings.Contains(err.Error(), wantSubstring) {
		t.Fatalf("error %q doesn't contain %q", err, wantSubstring)
	}
}

// These try to break the rules by going around the app and talking to the database directly.
// The database itself has to say no.
func TestDatabaseGuards(t *testing.T) {
	e := newEnv(t, bank.Chaos{})
	ctx := context.Background()

	t.Run("unbalanced ledger transaction can't be committed", func(t *testing.T) {
		pid := e.rawPayment("g1", "succeeded")
		tx, _ := e.pool.Begin(ctx)
		defer tx.Rollback(ctx)
		var txID string
		if err := tx.QueryRow(ctx, `INSERT INTO ledger_transactions (payment_id, kind) VALUES ($1::uuid, 'charge') RETURNING id::text`, pid).Scan(&txID); err != nil {
			t.Fatal(err)
		}
		tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account_id, amount, currency) VALUES ($1::uuid, 'a', 100, 'usd')`, txID)
		tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account_id, amount, currency) VALUES ($1::uuid, 'b', -90, 'usd')`, txID)
		mustFail(t, tx.Commit(ctx), "does not balance")
		if n := e.count(`SELECT count(*) FROM ledger_entries`); n != 0 {
			t.Fatalf("%d entries were saved from a failed commit", n)
		}
	})

	// one good ledger transaction to attack in the next steps
	pid := e.rawPayment("g2", "succeeded")
	tx, _ := e.pool.Begin(ctx)
	var txID string
	tx.QueryRow(ctx, `INSERT INTO ledger_transactions (payment_id, kind) VALUES ($1::uuid, 'charge') RETURNING id::text`, pid).Scan(&txID)
	tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account_id, amount, currency) VALUES ($1::uuid, 'a', 100, 'usd')`, txID)
	tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account_id, amount, currency) VALUES ($1::uuid, 'b', -100, 'usd')`, txID)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("balanced transaction was rejected: %v", err)
	}

	t.Run("ledger entries can't be edited", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `UPDATE ledger_entries SET amount = 1`)
		mustFail(t, err, "append-only")
	})
	t.Run("ledger entries can't be deleted", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `DELETE FROM ledger_entries`)
		mustFail(t, err, "append-only")
	})
	t.Run("ledger transactions can't be deleted", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `DELETE FROM ledger_transactions`)
		mustFail(t, err, "append-only")
	})
	t.Run("a payment can't be posted to the ledger twice", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `INSERT INTO ledger_transactions (payment_id, kind) VALUES ($1::uuid, 'charge')`, pid)
		mustFail(t, err, "duplicate key")
	})
	t.Run("ledger needs a succeeded payment", func(t *testing.T) {
		pending := e.rawPayment("g3", "processing")
		_, err := e.pool.Exec(ctx, `INSERT INTO ledger_transactions (payment_id, kind) VALUES ($1::uuid, 'charge')`, pending)
		mustFail(t, err, "needs a succeeded payment")
	})
	t.Run("succeeded payment can't become failed", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `UPDATE payments SET status = 'failed' WHERE id = $1::uuid`, pid)
		mustFail(t, err, "illegal payment status change")
	})
	t.Run("created payment can't jump to succeeded", func(t *testing.T) {
		fresh := e.rawPayment("g4", "created")
		_, err := e.pool.Exec(ctx, `UPDATE payments SET status = 'succeeded' WHERE id = $1::uuid`, fresh)
		mustFail(t, err, "illegal payment status change")
	})
	t.Run("amount must be positive", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `UPDATE payments SET amount = -5 WHERE id = $1::uuid`, pid)
		mustFail(t, err, "amount")
	})
	t.Run("same idempotency key twice in the db is rejected", func(t *testing.T) {
		_, err := e.pool.Exec(ctx, `
			INSERT INTO payments (merchant_id, idempotency_key, request_hash, amount, currency, source)
			VALUES ($1, 'g2', 'x', 1, 'usd', 'tok_visa')`, e.merchantID)
		mustFail(t, err, "duplicate key")
	})
}
