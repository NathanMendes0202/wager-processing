-- Cada transacao emite no maximo um evento de cada tipo. A versao do envelope
-- passa a ser a versao do contrato do evento (constante), nao mais a da carteira.
DROP INDEX IF EXISTS uq_outbox_aggregate_version_type;

CREATE UNIQUE INDEX uq_outbox_event_per_cause
    ON outbox_events(event_type, causation_id)
    WHERE causation_id IS NOT NULL;
