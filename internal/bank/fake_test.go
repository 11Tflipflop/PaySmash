package bank

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFakeBankIsIdempotentAndUnderstandsTokens(t *testing.T) {
	fake := NewFake(Chaos{})
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	c := NewClient(srv.URL, time.Second)
	ctx := context.Background()

	req := ChargeRequest{Amount: 100, Currency: "usd", Source: "tok_visa"}
	for i := 0; i < 3; i++ {
		res, err := c.Charge(ctx, "key-1", req)
		if err != nil || !res.Approved {
			t.Fatalf("want approved, got %+v err=%v", res, err)
		}
	}
	if s := fake.Stats(); s.Calls != 3 || s.UniqueCharges != 1 {
		t.Fatalf("same key 3 times should be 3 calls but 1 charge, got %+v", s)
	}

	res, err := c.Charge(ctx, "key-2", ChargeRequest{Amount: 100, Currency: "usd", Source: "tok_declined"})
	if err != nil || res.Approved || res.Reason != "card_declined" {
		t.Fatalf("want card_declined, got %+v err=%v", res, err)
	}
}

func TestClientTreatsBankErrorsAsTransient(t *testing.T) {
	fake := NewFake(Chaos{ErrorRate: 1})
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	c := NewClient(srv.URL, time.Second)

	_, err := c.Charge(context.Background(), "k", ChargeRequest{Amount: 1, Currency: "usd", Source: "tok_visa"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient, got %v", err)
	}

	// nothing listening at all -> also transient
	c2 := NewClient("http://127.0.0.1:1", 200*time.Millisecond)
	_, err = c2.Charge(context.Background(), "k", ChargeRequest{Amount: 1, Currency: "usd", Source: "tok_visa"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient for a dead bank, got %v", err)
	}
}

func TestLostResponseStillChargesOnce(t *testing.T) {
	fake := NewFake(Chaos{LostRate: 1, HangFor: 50 * time.Millisecond})
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	c := NewClient(srv.URL, time.Second)

	_, err := c.Charge(context.Background(), "k", ChargeRequest{Amount: 5, Currency: "usd", Source: "tok_visa"})
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("answer was lost, want ErrTransient, got %v", err)
	}
	// ...but the bank did charge
	if s := fake.Stats(); s.UniqueCharges != 1 || s.Approved != 1 {
		t.Fatalf("bank should have charged once, got %+v", s)
	}
}
