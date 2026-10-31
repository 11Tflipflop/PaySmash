-- payengine schema
-- The important rules are enforced HERE in the database (constraints + triggers),
-- not only in Go code. So even a bug in the app can't break the money rules.

CREATE TABLE merchants (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    api_key_hash  TEXT NOT NULL UNIQUE,      -- we only store a hash of the api key
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id      TEXT NOT NULL REFERENCES merchants(id),
    idempotency_key  TEXT NOT NULL,
    request_hash     TEXT NOT NULL,          -- hash of the request, to catch "same key, different body"
    amount           BIGINT NOT NULL CHECK (amount > 0),   -- smallest unit (cents/paise)
    currency         TEXT NOT NULL CHECK (currency IN ('usd', 'eur', 'inr')),
    source           TEXT NOT NULL,          -- test card token like tok_visa
    status           TEXT NOT NULL DEFAULT 'created'
                     CHECK (status IN ('created', 'processing', 'succeeded', 'failed', 'review')),
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- THE idempotency rule: one merchant can't use the same key twice
    UNIQUE (merchant_id, idempotency_key)
);

-- Only these status changes are allowed. Anything else raises an error.
--   created    -> processing | review
--   processing -> succeeded | failed | review
--   review     -> succeeded | failed      (a late answer from the bank resolves it)
CREATE FUNCTION check_payment_transition() RETURNS trigger AS $$
BEGIN
    IF NEW.status = OLD.status THEN
        RETURN NEW;
    END IF;
    IF (OLD.status, NEW.status) IN (
        ('created', 'processing'),
        ('created', 'review'),
        ('processing', 'succeeded'),
        ('processing', 'failed'),
        ('processing', 'review'),
        ('review', 'succeeded'),
        ('review', 'failed')
    ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'illegal payment status change: % -> %', OLD.status, NEW.status;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER payments_status_guard
    BEFORE UPDATE OF status ON payments
    FOR EACH ROW EXECUTE FUNCTION check_payment_transition();

-- The job queue. One job per payment. Workers claim jobs with a lease.
CREATE TABLE jobs (
    id                BIGSERIAL PRIMARY KEY,
    payment_id        UUID NOT NULL UNIQUE REFERENCES payments(id),
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),   -- don't run before this time (used for backoff)
    attempts          INT NOT NULL DEFAULT 0,
    locked_by         TEXT,
    lease_expires_at  TIMESTAMPTZ,                          -- if a worker dies, the lease runs out and the job is free again
    last_error        TEXT,
    done              BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX jobs_ready_idx ON jobs (run_at) WHERE NOT done;

-- The ledger (double entry).
-- A ledger_transaction is one money movement. It has 2+ entries.
-- Entries use signed amounts and must add up to exactly 0.
CREATE TABLE ledger_transactions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id  UUID NOT NULL REFERENCES payments(id),
    kind        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- a payment can only be posted to the ledger once per kind
    UNIQUE (payment_id, kind)
);

CREATE TABLE ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  UUID NOT NULL REFERENCES ledger_transactions(id),
    account_id      TEXT NOT NULL,        -- e.g. merchant:m_abc, platform:fees, external:card_network
    amount          BIGINT NOT NULL CHECK (amount <> 0),   -- + = money in, - = money out
    currency        TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id);
CREATE INDEX ledger_entries_tx_idx ON ledger_entries (transaction_id);

-- Ledger is append-only: no editing, no deleting. Fix mistakes with a new reversing entry.
CREATE FUNCTION forbid_ledger_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();

CREATE TRIGGER ledger_transactions_append_only
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();

-- A ledger transaction is only allowed for a payment that really succeeded.
CREATE FUNCTION check_ledger_tx_payment() RETURNS trigger AS $$
DECLARE
    s TEXT;
BEGIN
    SELECT status INTO s FROM payments WHERE id = NEW.payment_id;
    IF s IS DISTINCT FROM 'succeeded' THEN
        RAISE EXCEPTION 'ledger transaction needs a succeeded payment (payment % is %)', NEW.payment_id, s;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_tx_needs_success
    BEFORE INSERT ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION check_ledger_tx_payment();

-- Entries of one transaction must sum to 0. This is a DEFERRED constraint trigger,
-- so it runs at COMMIT (after all the entries of the transaction are inserted).
-- If the sum is not 0, the whole commit fails and nothing is saved.
CREATE FUNCTION check_ledger_balanced() RETURNS trigger AS $$
DECLARE
    total BIGINT;
BEGIN
    SELECT COALESCE(SUM(amount), 0) INTO total
    FROM ledger_entries
    WHERE transaction_id = NEW.transaction_id AND currency = NEW.currency;

    IF total <> 0 THEN
        RAISE EXCEPTION 'ledger transaction % does not balance (sum is %)', NEW.transaction_id, total;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_ledger_balanced();
