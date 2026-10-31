#!/usr/bin/env bash
# Live demo: real processes, a chaotic bank, and a worker that gets killed with kill -9.
#
#   export DATABASE_URL='postgres://pay:pay@localhost:5432/payengine?sslmode=disable'
#   ./scripts/demo.sh
#
# needs: go, curl, psql
set -euo pipefail
: "${DATABASE_URL:?set DATABASE_URL first}"
cd "$(dirname "$0")/.."

API_PORT=${API_PORT:-8080}
BANK_PORT=${BANK_PORT:-9090}
N=${N:-40}                # number of payments (each one is sent twice)
AMOUNT=5000

mkdir -p bin
go build -o bin/payengine ./cmd/payengine
go build -o bin/fakebank ./cmd/fakebank

PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

./bin/payengine migrate >/dev/null
KEY=$(./bin/payengine create-merchant -name "Demo Shop $(date +%s)" | awk '/api key/ {print $3}')

echo ">> starting a bank that fails 20%, hangs 5%, and loses 20% of its answers"
./bin/fakebank -addr ":$BANK_PORT" -error-rate 0.2 -hang-rate 0.05 -lost-rate 0.2 -hang-for 8s 2>/dev/null &
PIDS+=($!)

echo ">> starting the api and 2 separate worker processes"
./bin/payengine api -addr ":$API_PORT" 2>/dev/null &
PIDS+=($!)
./bin/payengine worker -workers 3 -bank "http://localhost:$BANK_PORT" -bank-timeout 2s -lease 8s -max-attempts 12 2>/dev/null &
W1=$!; PIDS+=($W1)
./bin/payengine worker -workers 3 -bank "http://localhost:$BANK_PORT" -bank-timeout 2s -lease 8s -max-attempts 12 2>/dev/null &
PIDS+=($!)
sleep 1.5

echo ">> sending $N payments, every request twice at the same moment (same Idempotency-Key)"
send() {
  curl -s -o /dev/null -X POST "http://localhost:$API_PORT/v1/payments" \
    -H "Authorization: Bearer $KEY" -H "Idempotency-Key: demo-$RUN-$1" \
    -H "Content-Type: application/json" \
    -d "{\"amount\":$AMOUNT,\"currency\":\"usd\",\"source\":\"tok_visa\"}"
}
RUN=$(date +%s)   # same for every request of this run, so the two copies share a key
export -f send; export KEY API_PORT AMOUNT RUN
(for i in $(seq 1 "$N"); do echo "$i"; echo "$i"; done) | xargs -P 20 -I{} bash -c 'send {}'

sleep 1
echo ">> kill -9 on worker process $W1 while jobs are in flight (no goodbye, no cleanup)"
kill -9 "$W1" || true

echo ">> waiting for the other worker to finish everything (it has to wait for the dead worker's leases to run out, ~8s)"
for _ in $(seq 1 90); do
  pending=$(psql "$DATABASE_URL" -tAc "SELECT count(*) FROM jobs j JOIN payments p ON p.id=j.payment_id JOIN merchants m ON m.id=p.merchant_id WHERE NOT j.done")
  [ "$pending" = "0" ] && break
  sleep 1
done

echo
echo "================ RESULT ================"
psql "$DATABASE_URL" -c "SELECT status, count(*) FROM payments p JOIN merchants m ON m.id=p.merchant_id WHERE m.api_key_hash = encode(sha256('$KEY'::bytea),'hex') GROUP BY status"
STATS=$(curl -s "http://localhost:$BANK_PORT/stats")
echo "bank says:     $STATS"
BAL=$(curl -s "http://localhost:$API_PORT/v1/balance" -H "Authorization: Bearer $KEY")
echo "your balance:  $BAL"

CREATED=$(psql "$DATABASE_URL" -tAc "SELECT count(*) FROM payments p JOIN merchants m ON m.id=p.merchant_id WHERE m.api_key_hash = encode(sha256('$KEY'::bytea),'hex')")
LEDGER_SUM=$(psql "$DATABASE_URL" -tAc "SELECT COALESCE(SUM(amount),0) FROM ledger_entries")
echo
[ "$CREATED" = "$N" ]      && echo "PASS  $N payments exist (2N requests were sent, duplicates were ignored)" || echo "FAIL  $CREATED payments, wanted $N"
[ "$LEDGER_SUM" = "0" ]    && echo "PASS  the ledger adds up to exactly 0" || echo "FAIL  ledger sums to $LEDGER_SUM"
