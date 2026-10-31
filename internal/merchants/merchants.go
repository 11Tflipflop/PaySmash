// Package merchants handles merchant accounts and api keys.
package merchants

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // the system random source is broken, nothing sane to do
	}
	return hex.EncodeToString(b)
}

// api keys are long random strings, so a plain sha256 is enough here
// (passwords would need bcrypt/argon2, random keys don't)
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Create makes a merchant and returns the api key. The key is only shown this
// once, we store just its hash.
func Create(ctx context.Context, pool *pgxpool.Pool, name string) (id, apiKey string, err error) {
	id = "m_" + randomHex(6)
	apiKey = "sk_test_" + randomHex(16)
	_, err = pool.Exec(ctx,
		`INSERT INTO merchants (id, name, api_key_hash) VALUES ($1, $2, $3)`,
		id, name, hashKey(apiKey))
	return id, apiKey, err
}

// Authenticate returns the merchant id for an api key.
func Authenticate(ctx context.Context, pool *pgxpool.Pool, apiKey string) (string, bool, error) {
	var id string
	err := pool.QueryRow(ctx,
		`SELECT id FROM merchants WHERE api_key_hash = $1`, hashKey(apiKey)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}
