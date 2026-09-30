-- 1) Ledger append-only: nenhuma edicao, exclusao ou truncate, nem pelo proprio app.
CREATE OR REPLACE FUNCTION forbid_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% blocked)', TG_OP
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER trg_ledger_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();

CREATE TRIGGER trg_ledger_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();

-- 2) balance_after = balance_before +/- amount, imposto pelo schema.
ALTER TABLE wallet_ledger_entries
    ADD CONSTRAINT ck_ledger_balance_math CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
     OR (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor)
    );

-- 3) Politica de valor por tipo: LOSS = 0, demais > 0.
ALTER TABLE wager_transactions
    ADD CONSTRAINT ck_wager_amount_by_kind CHECK (
        (kind = 'LOSS' AND amount_minor = 0)
     OR (kind <> 'LOSS' AND amount_minor > 0)
    );

-- 4) Origem interna (OPENING) x externa (demais tipos).
ALTER TABLE wager_transactions
    ADD CONSTRAINT ck_wager_origin CHECK (
        (kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
     OR (kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    );

-- 5) No maximo um credito inicial por carteira.
CREATE UNIQUE INDEX uq_wager_single_opening
    ON wager_transactions(wallet_id) WHERE kind = 'OPENING';
