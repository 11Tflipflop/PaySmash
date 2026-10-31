// Package api is the http layer: auth, validation, json in and out.
// The real logic lives in the payments and ledger packages.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"payengine/internal/ledger"
	"payengine/internal/merchants"
	"payengine/internal/payments"
)

type API struct {
	Pool   *pgxpool.Pool
	Store  *payments.Store
	Logger *slog.Logger
}

func New(pool *pgxpool.Pool, logger *slog.Logger) *API {
	if logger == nil {
		logger = slog.Default()
	}
	return &API{Pool: pool, Store: &payments.Store{Pool: pool}, Logger: logger}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.Handle("POST /v1/payments", a.auth(http.HandlerFunc(a.createPayment)))
	mux.Handle("GET /v1/payments/{id}", a.auth(http.HandlerFunc(a.getPayment)))
	mux.Handle("GET /v1/balance", a.auth(http.HandlerFunc(a.getBalance)))
	return a.recoverer(a.logRequests(mux))
}

// ---------- helpers ----------

type ctxKey struct{}

func merchantID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// ---------- middleware ----------

func (a *API) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				a.Logger.Error("panic in handler", "panic", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// logs method, path, status, time. Never logs headers or bodies (api keys live there).
func (a *API) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		a.Logger.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "ms", time.Since(start).Milliseconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// auth reads "Authorization: Bearer sk_test_..." and finds the merchant.
func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized", "send your api key as: Authorization: Bearer <key>")
			return
		}
		id, found, err := merchants.Authenticate(r.Context(), a.Pool, token)
		if err != nil {
			a.Logger.Error("auth lookup failed", "err", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong")
			return
		}
		if !found {
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid api key")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

// ---------- handlers ----------

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.Pool.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "db_down", "database not reachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createRequest struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Source   string `json:"source"`
}

// POST /v1/payments
// Needs the header Idempotency-Key. Send the same key again to safely retry.
//
//	201 = created a new payment
//	200 = this key was used before with the same request -> here is the existing payment
//	422 = this key was used before with a DIFFERENT request
func (a *API) createPayment(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "send an Idempotency-Key header")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body createRequest
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "body must be json with: amount, currency, source")
		return
	}

	p, replayed, err := a.Store.Create(r.Context(), payments.CreateInput{
		MerchantID:     merchantID(r.Context()),
		IdempotencyKey: key,
		Amount:         body.Amount,
		Currency:       strings.ToLower(body.Currency),
		Source:         body.Source,
	})

	var verr *payments.ValidationError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusBadRequest, "invalid_request", verr.Message)
	case errors.Is(err, payments.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with different amount/currency/source")
	case err != nil:
		a.Logger.Error("create payment failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong")
	case replayed:
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, p)
	default:
		writeJSON(w, http.StatusCreated, p)
	}
}

// GET /v1/payments/{id}
func (a *API) getPayment(w http.ResponseWriter, r *http.Request) {
	p, err := a.Store.Get(r.Context(), merchantID(r.Context()), r.PathValue("id"))
	switch {
	case errors.Is(err, payments.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "no such payment")
	case err != nil:
		a.Logger.Error("get payment failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong")
	default:
		writeJSON(w, http.StatusOK, p)
	}
}

// GET /v1/balance : the merchant's balance, calculated from the ledger
func (a *API) getBalance(w http.ResponseWriter, r *http.Request) {
	id := merchantID(r.Context())
	bal, err := ledger.Balances(r.Context(), a.Pool, ledger.MerchantAccount(id))
	if err != nil {
		a.Logger.Error("balance failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"merchant_id": id, "balances": bal})
}
