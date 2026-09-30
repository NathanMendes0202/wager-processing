DROP INDEX IF EXISTS uq_wager_single_opening;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS ck_wager_origin;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS ck_wager_amount_by_kind;
ALTER TABLE wallet_ledger_entries DROP CONSTRAINT IF EXISTS ck_ledger_balance_math;
DROP TRIGGER IF EXISTS trg_ledger_no_truncate ON wallet_ledger_entries;
DROP TRIGGER IF EXISTS trg_ledger_no_update_delete ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS forbid_ledger_mutation();
