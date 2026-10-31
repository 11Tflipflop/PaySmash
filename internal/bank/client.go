// Package bank has the client the worker uses to talk to the bank,
// and a fake bank (fake.go) for testing.
package bank

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type ChargeRequest struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Source   string `json:"source"`
}

type Result struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"` // why it was declined
}

// ErrTransient means "we don't know what happened, try again":
// timeout, connection error, 5xx from the bank, ...
var ErrTransient = errors.New("bank: temporary error")

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: timeout}}
}

// Charge asks the bank to charge a card.
//
// The idempotency key is how the bank knows two requests are the same charge.
// We use the payment id, so if we retry (because the first answer got lost)
// the bank returns the same result instead of charging twice.
//
// Returns:
//   - (result, nil)    the bank gave a clear answer: approved or declined
//   - (_, ErrTransient) no clear answer, the caller should retry later
func (c *Client) Charge(ctx context.Context, idempotencyKey string, req ChargeRequest) (Result, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/charge", bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrTransient, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPaymentRequired: // approved / declined
		var res Result
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return Result{}, fmt.Errorf("%w: bad response body: %v", ErrTransient, err)
		}
		return res, nil
	default:
		// 5xx, 429, or anything we don't understand: treat as "try again later"
		return Result{}, fmt.Errorf("%w: bank returned status %d", ErrTransient, resp.StatusCode)
	}
}
