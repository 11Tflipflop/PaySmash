// Package payments creates and reads payments.
// The main thing here is Create, which is idempotent.
package payments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Status string

const (
	StatusCreated    Status = "created"    // accepted, waiting for a worker
	StatusProcessing Status = "processing" // a worker is talking to the bank
	StatusSucceeded  Status = "succeeded"  // bank approved, money is in the ledger
	StatusFailed     Status = "failed"     // bank declined
	StatusReview     Status = "review"     // we couldn't find out what happened, needs a human
)

type Payment struct {
	ID            string    `json:"id"`
	Status        Status    `json:"status"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Source        string    `json:"source"`
	FailureReason string    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type CreateInput struct {
	MerchantID     string
	IdempotencyKey string
	Amount         int64
	Currency       string
	Source         string
}

var (
	ErrNotFound            = errors.New("payment not found")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
)

// ValidationError is for bad input from the client (400).
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

const MaxAmount = 99_999_999

var (
	sourceRe   = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	currencies = map[string]bool{"usd": true, "eur": true, "inr": true}
)

func (in CreateInput) Validate() error {
	if n := len(in.IdempotencyKey); n < 1 || n > 255 {
		return &ValidationError{"Idempotency-Key must be 1 to 255 characters"}
	}
	for i := 0; i < len(in.IdempotencyKey); i++ {
		if c := in.IdempotencyKey[i]; c < 0x21 || c > 0x7e {
			return &ValidationError{"Idempotency-Key must only have printable ascii characters, no spaces"}
		}
	}
	if in.Amount < 1 || in.Amount > MaxAmount {
		return &ValidationError{fmt.Sprintf("amount must be between 1 and %d", MaxAmount)}
	}
	if !currencies[in.Currency] {
		return &ValidationError{"currency must be one of: usd, eur, inr"}
	}
	if !sourceRe.MatchString(in.Source) {
		return &ValidationError{"source must be lowercase letters, numbers or _ (max 64)"}
	}
	return nil
}

// hash of the parts of the request that matter. If someone reuses an
// idempotency key with different values we can tell.
func (in CreateInput) hash() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%s", in.Amount, in.Currency, in.Source)))
	return hex.EncodeToString(sum[:])
}

type Store struct {
	Pool *pgxpool.Pool
}

// Create makes a payment (and its job) exactly once per (merchant, idempotency key).
//
// Returns replayed=true if this key was already used with the same request,
// in which case the existing payment is returned and nothing new is created.
//
// How it stays safe when 100 identical requests arrive at the same time:
// the INSERT ... ON CONFLICT DO NOTHING hits the UNIQUE(merchant_id, idempotency_key)
// index. Postgres makes the others wait for the first one to commit, and then they
// all get "no row inserted". Only one request can ever insert.
//
// The payment row and the job row are inserted in the SAME db transaction, so we
// can never end up with a payment that has no job (or a job with no payment).
func (s *Store) Create(ctx context.Context, in CreateInput) (p Payment, replayed bool, err error) {
	if err := in.Validate(); err != nil {
		return Payment{}, false, err
	}
	hash := in.hash()

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Payment{}, false, err
	}
	defer tx.Rollback(ctx)

	var status string
	err = tx.QueryRow(ctx, `
		INSERT INTO payments (merchant_id, idempotency_key, request_hash, amount, currency, source)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (merchant_id, idempotency_key) DO NOTHING
		RETURNING id::text, status, amount, currency, source, created_at, updated_at`,
		in.MerchantID, in.IdempotencyKey, hash, in.Amount, in.Currency, in.Source,
	).Scan(&p.ID, &status, &p.Amount, &p.Currency, &p.Source, &p.CreatedAt, &p.UpdatedAt)

	if err == nil {
		// new payment: queue the job in the same transaction
		if _, err := tx.Exec(ctx, `INSERT INTO jobs (payment_id) VALUES ($1::uuid)`, p.ID); err != nil {
			return Payment{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Payment{}, false, err
		}
		p.Status = Status(status)
		return p, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, false, err
	}

	// the key was used before: load that payment and compare the request
	var storedHash string
	var reason *string
	err = tx.QueryRow(ctx, `
		SELECT id::text, request_hash, status, amount, currency, source, failure_reason, created_at, updated_at
		FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`,
		in.MerchantID, in.IdempotencyKey,
	).Scan(&p.ID, &storedHash, &status, &p.Amount, &p.Currency, &p.Source, &reason, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return Payment{}, false, err
	}
	if storedHash != hash {
		return Payment{}, false, ErrIdempotencyMismatch
	}
	p.Status = Status(status)
	if reason != nil {
		p.FailureReason = *reason
	}
	return p, true, nil
}

// Get returns one payment. A merchant can only see their own payments.
func (s *Store) Get(ctx context.Context, merchantID, id string) (Payment, error) {
	if !uuidRe.MatchString(id) {
		return Payment{}, ErrNotFound
	}
	var p Payment
	var status string
	var reason *string
	err := s.Pool.QueryRow(ctx, `
		SELECT id::text, status, amount, currency, source, failure_reason, created_at, updated_at
		FROM payments WHERE id = $1::uuid AND merchant_id = $2`,
		id, merchantID,
	).Scan(&p.ID, &status, &p.Amount, &p.Currency, &p.Source, &reason, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, err
	}
	p.Status = Status(status)
	if reason != nil {
		p.FailureReason = *reason
	}
	return p, nil
}
