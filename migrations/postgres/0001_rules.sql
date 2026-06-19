-- S5: rules table + hot-reload NOTIFY. Transactional rule store (replaces the
-- Java Mongo+Redis rule store and Redis pub/sub).
--
-- A DB trigger emits NOTIFY 'rules_changed' on every INSERT/UPDATE/DELETE, so
-- all service instances hear rule changes (replaces Redis pub/sub). The payload
-- is the affected rule id. gen_random_uuid() is built in on Postgres 16.

CREATE TABLE IF NOT EXISTS rules (
    id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    description  TEXT        NOT NULL DEFAULT '',
    expression   TEXT        NOT NULL,
    target_topic TEXT        NOT NULL,
    enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    version      BIGINT      NOT NULL DEFAULT 1,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION notify_rules_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('rules_changed', COALESCE(NEW.id, OLD.id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rules_changed_trg ON rules;
CREATE TRIGGER rules_changed_trg
    AFTER INSERT OR UPDATE OR DELETE ON rules
    FOR EACH ROW EXECUTE FUNCTION notify_rules_changed();
