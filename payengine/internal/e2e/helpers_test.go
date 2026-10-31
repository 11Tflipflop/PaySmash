// These are integration tests: they need a real Postgres.
//
//	DATABASE_URL=postgres://pay:pay@localhost:5432/payengine_test?sslmode=disable go test ./internal/e2e
//
// WARNING: the tests wipe the tables. They refuse to run unless the database name has "test" in it.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"payengine/internal/api"
	"payengine/internal/bank"
	"payengine/internal/db"
	"payengine/internal/merchants"
	"payengine/internal/payments"
	"payengine/internal/worker"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		fmt.Println("DATABASE_URL not set, skipping integration tests")
		os.Exit(0)
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url)
	if err != nil {
		fmt.Println("can't connect:", err)
		os.Exit(1)
	}
	var name string
	pool.QueryRow(ctx, "SELECT current_database()").Scan(&name)
	if !strings.Contains(name, "test") {
		fmt.Printf("refusing to run: database %q doesn't look like a test database (the tests delete all data)\n", name)
		os.Exit(1)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		fmt.Println("migrate failed:", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type env struct {
	t          *testing.T
	pool       *pgxpool.Pool
	bank       *bank.Fake
	bankSrv    *httptest.Server
	apiSrv     *httptest.Server
	merchantID string
	apiKey     string
}

// newEnv gives every test a clean database, a fake bank, an api server and a merchant.
func newEnv(t *testing.T, chaos bank.Chaos) *env {
	t.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx,
		`TRUNCATE jobs, ledger_entries, ledger_transactions, payments, merchants RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}

	if chaos.HangFor == 0 {
		chaos.HangFor = time.Second
	}
	fake := bank.NewFake(chaos)
	bankSrv := httptest.NewServer(fake.Handler())
	t.Cleanup(bankSrv.Close)

	apiSrv := httptest.NewServer(api.New(testPool, quiet).Routes())
	t.Cleanup(apiSrv.Close)

	id, key, err := merchants.Create(ctx, testPool, "Test Shop")
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, pool: testPool, bank: fake, bankSrv: bankSrv, apiSrv: apiSrv, merchantID: id, apiKey: key}
}

// fast settings so tests don't take forever
func (e *env) workerCfg(id string) worker.Config {
	return worker.Config{
		ID:          id,
		Pool:        e.pool,
		Bank:        bank.NewClient(e.bankSrv.URL, 300*time.Millisecond),
		Logger:      quiet,
		Lease:       2 * time.Second,
		MaxAttempts: 5,
		PollEvery:   10 * time.Millisecond,
		Backoff:     func(int) time.Duration { return 20 * time.Millisecond },
	}
}

// drain runs n workers until every job is done.
func (e *env) drain(cfg worker.Config, n int) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		worker.RunMany(ctx, n, cfg)
		close(stopped)
	}()

	deadline := time.Now().Add(40 * time.Second)
	for {
		var pending int
		e.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE NOT done`).Scan(&pending)
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-stopped
			e.t.Fatalf("timed out: %d jobs still pending", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-stopped
}

// ---------- http helpers ----------

func (e *env) request(method, path string, headers map[string]string, body any) (int, http.Header, []byte) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.apiSrv.URL+path, rd)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Errorf("request failed: %v", err)
		return 0, nil, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, raw
}

func (e *env) auth() map[string]string {
	return map[string]string{"Authorization": "Bearer " + e.apiKey}
}

// pay calls POST /v1/payments
func (e *env) pay(key string, amount int64, source string) (int, payments.Payment) {
	h := e.auth()
	h["Idempotency-Key"] = key
	status, _, raw := e.request("POST", "/v1/payments", h,
		map[string]any{"amount": amount, "currency": "usd", "source": source})
	var p payments.Payment
	json.Unmarshal(raw, &p)
	return status, p
}

func (e *env) getPayment(id string) payments.Payment {
	e.t.Helper()
	status, _, raw := e.request("GET", "/v1/payments/"+id, e.auth(), nil)
	if status != 200 {
		e.t.Fatalf("GET payment: status %d: %s", status, raw)
	}
	var p payments.Payment
	json.Unmarshal(raw, &p)
	return p
}

// ---------- db helpers ----------

func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		e.t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func (e *env) sum(query string, args ...any) int64 {
	e.t.Helper()
	var n int64
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		e.t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func (e *env) accountBalance(account string) int64 {
	return e.sum(`SELECT COALESCE(SUM(amount), 0)::bigint FROM ledger_entries WHERE account_id = $1`, account)
}
