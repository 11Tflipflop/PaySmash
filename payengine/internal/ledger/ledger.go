// Package ledger is the double-entry bookkeeping part.
//
// Every money movement is a list of entries with signed amounts that add up to 0.
// Example: customer pays 10000, fee is 320:
//
//	external:card_network   -10000   (money came in from outside)
//	merchant:m_abc           +9680
//	platform:fees             +320
//
// A balance is just the sum of all entries of an account.
package ledger

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	AccountCardNetwork = "external:card_network"
	AccountFees        = "platform:fees"
)

func MerchantAccount(merchantID string) string { return "merchant:" + merchantID }

type Entry struct {
	Account string
	Amount  int64
}

// Fee is 2.9% + 30 (smallest unit), never more than the payment itself.
// Integer math only. Never use floats for money.
func Fee(amount int64) int64 {
	fee := amount*29/1000 + 30
	if fee > amount {
		fee = amount
	}
	return fee
}

// ChargeEntries builds the entries for a successful charge. They always sum to 0.
func ChargeEntries(merchantID string, amount int64) []Entry {
	fee := Fee(amount)
	entries := []Entry{{AccountCardNetwork, -amount}}
	if net := amount - fee; net != 0 {
		entries = append(entries, Entry{MerchantAccount(merchantID), net})
	}
	if fee != 0 {
		entries = append(entries, Entry{AccountFees, fee})
	}
	return entries
}

// PostCharge writes the ledger transaction + entries inside the caller's db transaction.
// So it is saved together with the payment status change, or not at all.
func PostCharge(ctx context.Context, tx pgx.Tx, paymentID, merchantID string, amount int64, currency string) error {
	var txID string
	err := tx.QueryRow(ctx,
		`INSERT INTO ledger_transactions (payment_id, kind) VALUES ($1::uuid, 'charge') RETURNING id::text`,
		paymentID).Scan(&txID)
	if err != nil {
		return err
	}
	for _, e := range ChargeEntries(merchantID, amount) {
		_, err := tx.Exec(ctx,
			`INSERT INTO ledger_entries (transaction_id, account_id, amount, currency)
			 VALUES ($1::uuid, $2, $3, $4)`,
			txID, e.Account, e.Amount, currency)
		if err != nil {
			return err
		}
	}
	return nil
}

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Balances returns the balance of an account per currency.
func Balances(ctx context.Context, q Querier, account string) (map[string]int64, error) {
	rows, err := q.Query(ctx,
		`SELECT currency, COALESCE(SUM(amount), 0)::bigint FROM ledger_entries
		 WHERE account_id = $1 GROUP BY currency`, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var cur string
		var sum int64
		if err := rows.Scan(&cur, &sum); err != nil {
			return nil, err
		}
		out[cur] = sum
	}
	return out, rows.Err()
}
