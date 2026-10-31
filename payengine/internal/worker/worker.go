// Package worker takes jobs from the queue and talks to the bank.
//
// The queue is a Postgres table (jobs). A worker "claims" a job by setting a lease
// on it. If the worker dies, the lease runs out and another worker takes the job.
//
// Queues like this are AT-LEAST-ONCE: the same job can run twice (worker is slow,
// lease runs out, someone else picks it up). So everything here is safe to repeat:
//   - the bank call uses the payment id as idempotency key (bank dedupes)
//   - the payment status change is guarded (database trigger + row lock)
//   - the ledger insert is guarded (UNIQUE(payment_id, kind))
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"payengine/internal/bank"
	"payengine/internal/ledger"
)

// Bank is what the worker needs from a bank. bank.Client implements it.
type Bank interface {
	Charge(ctx context.Context, idempotencyKey string, req bank.ChargeRequest) (bank.Result, error)
}

type Config struct {
	ID          string // name of this worker, shows up in the jobs table
	Pool        *pgxpool.Pool
	Bank        Bank
	Logger      *slog.Logger
	Lease       time.Duration                   // how long a claim lasts. Must be longer than the bank timeout!
	MaxAttempts int                             // after this many tries -> payment goes to "review"
	PollEvery   time.Duration                   // how often to look for jobs when the queue is empty
	Backoff     func(attempt int) time.Duration // wait time before retry number "attempt"
}

type Worker struct{ Config }

func New(cfg Config) *Worker {
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("worker-%d", rand.Intn(1_000_000))
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Lease == 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 6
	}
	if cfg.PollEvery == 0 {
		cfg.PollEvery = 500 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	return &Worker{cfg}
}

// DefaultBackoff: 1s, 2s, 4s, 8s ... up to 60s, with +-25% random jitter
// (jitter so that many failed jobs don't all retry at the same moment)
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Second << min(attempt-1, 6)
	if d > time.Minute {
		d = time.Minute
	}
	return time.Duration(float64(d) * (0.75 + rand.Float64()*0.5))
}

// Run keeps taking jobs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.sweepExhausted(ctx); err != nil && ctx.Err() == nil {
			w.Logger.Error("sweep failed", "worker", w.ID, "err", err)
		}

		did, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.Logger.Error("job failed", "worker", w.ID, "err", err)
		}
		if did {
			continue // there may be more jobs waiting, don't sleep
		}
		select {
		case <-ctx.Done():
		case <-time.After(w.PollEvery + time.Duration(rand.Int63n(int64(w.PollEvery)/2+1))):
		}
	}
}

// RunMany starts n workers and waits for them all to stop.
func RunMany(ctx context.Context, n int, base Config) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		cfg := base
		cfg.ID = fmt.Sprintf("%s-%d", base.ID, i+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			New(cfg).Run(ctx)
		}()
	}
	wg.Wait()
}

type job struct {
	ID        int64
	PaymentID string
	Attempts  int
}

// RunOnce claims one job and processes it. Returns false if there was no job.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	j, ok, err := w.claim(ctx)
	if err != nil || !ok {
		return false, err
	}
	return true, w.process(ctx, j)
}

// claim takes one ready job and puts a lease on it.
//
// FOR UPDATE SKIP LOCKED: if another worker is claiming a job at the same time,
// we skip that row instead of waiting. So workers never block each other and two
// workers never get the same job.
// It uses now() from the database so all workers agree on time (no clock problems).
func (w *Worker) claim(ctx context.Context) (job, bool, error) {
	var j job
	err := w.Pool.QueryRow(ctx, `
		UPDATE jobs
		SET locked_by = $1,
		    lease_expires_at = now() + make_interval(secs => $2),
		    attempts = attempts + 1
		WHERE id = (
			SELECT id FROM jobs
			WHERE NOT done
			  AND run_at <= now()
			  AND attempts < $3
			  AND (lease_expires_at IS NULL OR lease_expires_at < now())
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, payment_id::text, attempts`,
		w.ID, w.Lease.Seconds(), w.MaxAttempts,
	).Scan(&j.ID, &j.PaymentID, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return job{}, false, nil
	}
	if err != nil {
		return job{}, false, err
	}
	return j, true, nil
}

type payment struct {
	ID         string
	MerchantID string
	Amount     int64
	Currency   string
	Source     string
}

func (w *Worker) process(ctx context.Context, j job) error {
	// If we are shutting down, still finish the job we already started.
	ctx = context.WithoutCancel(ctx)

	p, finished, err := w.start(ctx, j)
	if err != nil {
		return err // db problem. our lease will run out and the job gets retried
	}
	if finished {
		return nil // someone else already finished this payment
	}

	// NOTE: no database transaction is open during the bank call.
	// Holding a transaction (and locks) open during a slow network call is a classic mistake.
	res, err := w.Bank.Charge(ctx, p.ID, bank.ChargeRequest{
		Amount: p.Amount, Currency: p.Currency, Source: p.Source,
	})
	switch {
	case err != nil:
		return w.retryOrReview(ctx, j, err)
	case res.Approved:
		return w.settleSuccess(ctx, j, p)
	default:
		return w.settleFailure(ctx, j, p, res.Reason)
	}
}

// start moves created -> processing and loads the payment.
// If the payment is already finished (job delivered twice), it closes the job.
func (w *Worker) start(ctx context.Context, j job) (payment, bool, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return payment{}, false, err
	}
	defer tx.Rollback(ctx)

	var p payment
	var status string
	err = tx.QueryRow(ctx, `
		SELECT id::text, merchant_id, amount, currency, source, status
		FROM payments WHERE id = $1::uuid FOR UPDATE`, j.PaymentID,
	).Scan(&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.Source, &status)
	if err != nil {
		return payment{}, false, err
	}

	switch status {
	case "created":
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'processing', updated_at = now() WHERE id = $1::uuid`, p.ID); err != nil {
			return payment{}, false, err
		}
	case "processing":
		// a retry, or a job taken over from a dead worker. carry on.
	default:
		// succeeded / failed / review: nothing left to do
		if err := finishJob(ctx, tx, j.ID); err != nil {
			return payment{}, false, err
		}
		return p, true, tx.Commit(ctx)
	}
	return p, false, tx.Commit(ctx)
}

func finishJob(ctx context.Context, tx pgx.Tx, jobID int64) error {
	_, err := tx.Exec(ctx,
		`UPDATE jobs SET done = true, locked_by = NULL, lease_expires_at = NULL WHERE id = $1`, jobID)
	return err
}

// lockStatus locks the payment row and returns its current status.
func lockStatus(ctx context.Context, tx pgx.Tx, paymentID string) (string, error) {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM payments WHERE id = $1::uuid FOR UPDATE`, paymentID).Scan(&status)
	return status, err
}

// settleSuccess: payment -> succeeded, ledger entries, job done. All in ONE transaction.
// Either all three happen or none of them.
func (w *Worker) settleSuccess(ctx context.Context, j job, p payment) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	status, err := lockStatus(ctx, tx, p.ID)
	if err != nil {
		return err
	}
	// "review" is allowed too: a late bank answer can resolve a payment we gave up on
	if status == "processing" || status == "review" {
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'succeeded', failure_reason = NULL, updated_at = now() WHERE id = $1::uuid`, p.ID); err != nil {
			return err
		}
		if err := ledger.PostCharge(ctx, tx, p.ID, p.MerchantID, p.Amount, p.Currency); err != nil {
			return err
		}
	} else if status != "succeeded" {
		// we're "failed" but the bank says approved. That should never happen. Don't touch money, tell a human.
		w.Logger.Error("bank approved a payment that is already failed", "payment", p.ID, "status", status)
	}
	if err := finishJob(ctx, tx, j.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *Worker) settleFailure(ctx context.Context, j job, p payment, reason string) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	status, err := lockStatus(ctx, tx, p.ID)
	if err != nil {
		return err
	}
	if status == "processing" || status == "review" {
		if reason == "" {
			reason = "declined"
		}
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'failed', failure_reason = $2, updated_at = now() WHERE id = $1::uuid`, p.ID, reason); err != nil {
			return err
		}
	}
	if err := finishJob(ctx, tx, j.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// retryOrReview handles "the bank didn't give a clear answer".
// We can't just mark it failed: the bank may have charged the card already.
// So we retry (safe, because the bank dedupes on the idempotency key), and if we
// run out of attempts the payment goes to "review" for a human.
func (w *Worker) retryOrReview(ctx context.Context, j job, cause error) error {
	if j.Attempts >= w.MaxAttempts {
		w.Logger.Warn("out of attempts, sending to review", "payment", j.PaymentID, "err", cause)
		return w.moveToReview(ctx, j, cause.Error())
	}

	delay := w.Backoff(j.Attempts)
	w.Logger.Info("bank problem, will retry", "payment", j.PaymentID, "attempt", j.Attempts, "in", delay, "err", cause)

	// "AND locked_by = me": only reschedule if we still own the job
	_, err := w.Pool.Exec(ctx, `
		UPDATE jobs
		SET run_at = now() + make_interval(secs => $3),
		    locked_by = NULL, lease_expires_at = NULL, last_error = $4
		WHERE id = $1 AND locked_by = $2`,
		j.ID, w.ID, delay.Seconds(), cause.Error())
	return err
}

func (w *Worker) moveToReview(ctx context.Context, j job, lastErr string) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	status, err := lockStatus(ctx, tx, j.PaymentID)
	if err != nil {
		return err
	}
	if status == "processing" || status == "created" {
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'review', failure_reason = 'bank_unreachable', updated_at = now() WHERE id = $1::uuid`, j.PaymentID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET last_error = $2 WHERE id = $1`, j.ID, lastErr); err != nil {
		return err
	}
	if err := finishJob(ctx, tx, j.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// sweepExhausted handles jobs whose worker died on the LAST allowed attempt
// (so nobody is left to send the payment to review). Without this such a job
// would sit there forever: claim() ignores jobs that used all their attempts.
func (w *Worker) sweepExhausted(ctx context.Context) error {
	_, err := w.Pool.Exec(ctx, `
		WITH dead AS (
			UPDATE jobs
			SET done = true, locked_by = NULL, lease_expires_at = NULL,
			    last_error = COALESCE(last_error, '') || ' [gave up: attempts used up]'
			WHERE NOT done
			  AND attempts >= $1
			  AND (lease_expires_at IS NULL OR lease_expires_at < now())
			RETURNING payment_id
		)
		UPDATE payments
		SET status = 'review', failure_reason = 'attempts_exhausted', updated_at = now()
		WHERE id IN (SELECT payment_id FROM dead) AND status IN ('created', 'processing')`,
		w.MaxAttempts)
	return err
}
