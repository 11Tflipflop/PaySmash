# payengine

A small payment processor written in Go + PostgreSQL. It takes payment requests over an
HTTP API, charges a (fake) bank in the background, and keeps a ledger of where the money
went. The goal of the project is **not** lots of features. It is to get the hard part right:
**never lose money and never charge twice, even when things crash or time out.**

This is a learning project. It talks to a fake bank, handles no real card data, and is not
PCI compliant. Don't use it for real money.

## What it does

```
 client                API                 Postgres                 workers              bank
   |  POST /v1/payments |                     |                       |                    |
   |  Idempotency-Key   |  one transaction:   |                       |                    |
   |------------------->|  payment + job  --->| payments, jobs        |                    |
   |<-- 201 created ----|                     |                       |                    |
   |                    |                     |<-- claim a job -------|                    |
   |                    |                     |    (lease, SKIP LOCKED)|-- charge --------->|
   |                    |                     |                       |   key = payment id |
   |                    |                     |<-- ONE transaction:   |<-- approved -------|
   |                    |                     |    status=succeeded   |                    |
   |  GET /v1/payments  |                     |    + ledger entries   |                    |
   |<------ succeeded --|                     |    + job done         |                    |
```

Payment states: `created -> processing -> succeeded | failed | review`

- `review` = we asked the bank, never got a clear answer after all the retries, so we
  genuinely don't know if the card was charged. We do NOT guess. A human checks it.

## The guarantees, and what enforces each one

| Guarantee | How |
|---|---|
| Same request twice never makes two payments | `UNIQUE (merchant_id, idempotency_key)` + `INSERT ... ON CONFLICT DO NOTHING`. Same key with a different body gets a 422. |
| A payment can never exist without its job | payment row and job row are inserted in one db transaction |
| Card is never charged twice, even if a retry happens | worker sends the payment id as the bank's idempotency key, so the bank returns the same answer |
| A worker dying doesn't lose the job | jobs are claimed with a lease. When the lease runs out, another worker takes the job |
| Two workers never take the same job (at the same time) | `FOR UPDATE SKIP LOCKED` |
| Running a job twice is harmless | row lock + status check, and `UNIQUE (payment_id, kind)` on the ledger |
| Illegal status changes are impossible | a database trigger lists the allowed transitions |
| Money can't be created or destroyed | ledger entries of a transaction must sum to 0, checked by a deferred constraint trigger at COMMIT |
| History can't be edited | triggers block UPDATE/DELETE on ledger tables |
| No ledger entries for payments that didn't succeed | trigger checks the payment status |

The point: these rules live in the **database**, not only in Go. So a bug in the app code
can't break them. (One of my tests removes a check from the Go code on purpose. The database
constraint still stops the double posting.)

## Run it

You need Go 1.22+, Docker (for Postgres) and `psql` (only for the demo).

```
go mod tidy              # first time only, downloads pgx and makes go.sum
make db                  # postgres in docker
make migrate             # create tables
make bank                # terminal 1: fake bank that fails a lot on purpose
make serve               # terminal 2: api + 4 workers
```

Create a merchant and send a payment (terminal 3):

```
export DATABASE_URL='postgres://pay:pay@localhost:5432/payengine?sslmode=disable'
go run ./cmd/payengine create-merchant -name "My Shop"      # prints an api key
KEY=sk_test_...                                              # paste it

curl -X POST localhost:8080/v1/payments \
  -H "Authorization: Bearer $KEY" \
  -H "Idempotency-Key: order-123" \
  -H "Content-Type: application/json" \
  -d '{"amount":5000,"currency":"usd","source":"tok_visa"}'

# run the exact same curl again: you get 200 and the SAME payment (Idempotent-Replayed: true)
# change the amount but keep the key: you get 422

curl localhost:8080/v1/payments/<id> -H "Authorization: Bearer $KEY"
curl localhost:8080/v1/balance       -H "Authorization: Bearer $KEY"
```

Amounts are integers in the smallest unit (cents). Fee is 2.9% + 30, so a 5000 payment
leaves the merchant 4825.

Test card tokens for the fake bank: `tok_visa`, `tok_mastercard` (approved),
`tok_declined`, `tok_insufficient_funds` (declined), anything else is declined as invalid.

Commands: `payengine migrate | create-merchant | serve | api | worker`. You can run several
`worker` processes at once, even on different machines, as long as they share the database.

### The demo

```
make demo
```

Starts a bank that fails/hangs/loses answers, an API, and two worker processes. Sends 40
payments (each request twice at the same time), then `kill -9`s one worker process while it
has jobs in flight. At the end it prints the result and checks the ledger adds up to 0.

## Tests

```
make test
```

Needs the Postgres from `make db`. They use a separate `payengine_test` database and
**wipe its tables**. They refuse to run on a database without "test" in the name.

| Test | What it proves |
|---|---|
| `TestIdempotency_ConcurrentSameKey` | 100 requests with the same key at once -> exactly 1 payment, 1 job |
| `TestIdempotency_ReplayAndMismatch` | replay gives the same payment, a different body gives 422 |
| `TestCrashRecovery_LeaseExpires` | a dead worker's job is untouchable until the lease ends, then gets finished |
| `TestCrashRecovery_BankAlreadyCharged` | bank charged but we never heard back -> retry, still only ONE real charge |
| `TestSlowWorker_LeaseTakenOver` | two workers finish the same payment at the same time -> one ledger posting, no errors |
| `TestRetryExhaustion_GoesToReview` | bank down for good -> `review`, not `failed`, nothing in the ledger |
| `TestSweep_DeadWorkerOnLastAttempt` | a job whose worker died on its last try still ends up in `review` |
| `TestDuplicateDelivery_IsHarmless` | the same job delivered again -> no new bank call, no new ledger entry |
| `TestDatabaseGuards` | going around the app straight to the database: unbalanced entries, edits, deletes, illegal status changes, double posting are all rejected |
| `TestChaos_MoneyAlwaysAddsUp` | 120 payments, each sent twice, bank failing 35% of the time, 8 racing workers. At the end: ledger sums to 0, each succeeded payment has exactly 1 ledger transaction, balances match, bank charged each card once |

I also checked the tests can actually fail: I broke the code on purpose (random bank
idempotency key, removed a status check) and made sure the suite goes red.

## Design decisions (and what I'd say if asked why)

**Postgres as the queue, not Redis.** The brief was "Redis queues", but with Redis you have
the dual-write problem: you save the payment in Postgres AND push a job to Redis, and a crash
between the two leaves you with a payment nobody will process. With the queue in Postgres,
payment + job are one transaction, so that can't happen. `FOR UPDATE SKIP LOCKED` is a
well-known pattern and handles a lot of throughput. If you want Redis anyway, the clean way
is to keep the jobs table as the source of truth and use Redis only to wake workers up faster.

**Retry is safe because the bank dedupes.** The worker uses the payment id as the bank's
idempotency key. That is what makes "the answer got lost, try again" safe.

**No database transaction during the bank call.** Holding locks open during a slow network
call would block other work. The worker does short transactions before and after.

**Lease must be longer than the bank timeout.** The program refuses to start otherwise.
(If it's shorter, two workers could work on the same payment. It would still be safe, because
of the guards above, but wasteful.)

**Time comes from the database (`now()`)**, not from each worker's clock, so workers can't
disagree about whether a lease expired.

**Out of attempts -> `review`, never `failed`.** If we say "failed" and the bank actually
charged the card, the customer paid and the merchant got nothing.

## Limits (be honest about these)

- It is one Postgres. It is **concurrent and crash-tolerant**, but not "distributed" in the
  sense of sharding or consensus. Call it a reliable payment processing service.
- `review` payments have no tool yet to resolve them (a reconciler that asks the bank what
  really happened).
- Only charges. No refunds, no webhooks, no multi-currency conversion.
- Idempotent replays return the payment's *current* state, not a stored copy of the first response.
- Only the happy bits of auth: api keys, no rate limiting, no key rotation.
- Failure is simulated by my fake bank. A real bank would surprise me in other ways.

## Where to start reading

1. `internal/db/migrations/001_init.sql` : the rules. Read the comments.
2. `internal/payments/payments.go` : `Create`, the idempotent insert.
3. `internal/worker/worker.go` : claim, lease, process, settle, retry.
4. `internal/ledger/ledger.go` : double entry.
5. `internal/e2e/chaos_test.go` : the test that ties everything together.
