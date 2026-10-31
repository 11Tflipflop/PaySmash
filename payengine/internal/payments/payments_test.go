package payments

import (
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := CreateInput{MerchantID: "m", IdempotencyKey: "order-1", Amount: 500, Currency: "usd", Source: "tok_visa"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}

	bad := map[string]func(in *CreateInput){
		"no key":           func(in *CreateInput) { in.IdempotencyKey = "" },
		"key too long":     func(in *CreateInput) { in.IdempotencyKey = strings.Repeat("a", 256) },
		"key has space":    func(in *CreateInput) { in.IdempotencyKey = "my key" },
		"amount zero":      func(in *CreateInput) { in.Amount = 0 },
		"amount negative":  func(in *CreateInput) { in.Amount = -5 },
		"amount too big":   func(in *CreateInput) { in.Amount = MaxAmount + 1 },
		"bad currency":     func(in *CreateInput) { in.Currency = "doge" },
		"source uppercase": func(in *CreateInput) { in.Source = "TOK_VISA" },
		"source empty":     func(in *CreateInput) { in.Source = "" },
	}
	for name, mutate := range bad {
		in := ok
		mutate(&in)
		err := in.Validate()
		var verr *ValidationError
		if !errors.As(err, &verr) {
			t.Errorf("%s: want ValidationError, got %v", name, err)
		}
	}
}

func TestHashChangesWithRequest(t *testing.T) {
	a := CreateInput{Amount: 100, Currency: "usd", Source: "tok_visa"}
	b := a
	b.Amount = 101
	if a.hash() == b.hash() {
		t.Fatal("different requests must have different hashes")
	}
	c := a
	c.IdempotencyKey = "different key, same request"
	if a.hash() != c.hash() {
		t.Fatal("the idempotency key itself must not be part of the hash")
	}
}
