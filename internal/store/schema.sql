-- Entities: queues, topics and subscriptions. Properties are stored as
-- JSON so every Service Bus property is configurable without schema
-- changes. Paths are normalized to lower case, e.g. "orders" or
-- "events/subscriptions/audit".
CREATE TABLE IF NOT EXISTS entities (
    id          INTEGER PRIMARY KEY,
    kind        TEXT    NOT NULL,
    path        TEXT    NOT NULL UNIQUE,
    name        TEXT    NOT NULL,
    parent_id   INTEGER REFERENCES entities(id) ON DELETE CASCADE,
    props       TEXT    NOT NULL,
    next_seq    INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    accessed_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS entities_parent ON entities(parent_id);

-- Rules of subscriptions.
CREATE TABLE IF NOT EXISTS rules (
    id              INTEGER PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    props           TEXT    NOT NULL,
    created_at      INTEGER NOT NULL,
    UNIQUE (subscription_id, name)
);

-- Messages of every entity and sub-queue.
--   subqueue: 0 active, 1 dead-letter, 2 transfer dead-letter
--   state:    0 active, 1 deferred, 2 scheduled
-- Times are Unix milliseconds; expires_at 0 means never.
CREATE TABLE IF NOT EXISTS messages (
    id                 INTEGER PRIMARY KEY,
    entity_id          INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    subqueue           INTEGER NOT NULL DEFAULT 0,
    seq                INTEGER NOT NULL,
    state              INTEGER NOT NULL DEFAULT 0,
    session_id         TEXT,
    message_id         TEXT,
    enqueued_at        INTEGER NOT NULL,
    visible_at         INTEGER NOT NULL,
    expires_at         INTEGER NOT NULL DEFAULT 0,
    delivery_count     INTEGER NOT NULL DEFAULT 0,
    lock_token         BLOB,
    locked_until       INTEGER NOT NULL DEFAULT 0,
    dead_letter_source TEXT,
    payload            BLOB    NOT NULL,
    -- For a copy made by a rule with an action: the sequence number the
    -- message had on the topic, which CancelScheduledMessages names.
    origin_seq         INTEGER NOT NULL DEFAULT 0,
    -- The partition of a message of a partitioned entity, which is the high
    -- bits of the sequence number clients see.
    partition          INTEGER NOT NULL DEFAULT 0
);

-- Delivery order within a sub-queue.
CREATE INDEX IF NOT EXISTS messages_order
    ON messages(entity_id, subqueue, state, seq);
-- Session-aware delivery order.
-- Only messages of sessions are in it. It replaces messages_session, which
-- held every message.
CREATE INDEX IF NOT EXISTS messages_by_session
    ON messages(entity_id, subqueue, session_id, state, seq) WHERE session_id IS NOT NULL;
DROP INDEX IF EXISTS messages_session;
-- Messages without a lock, in delivery order. Receivers skip the locked ones
-- at the head of a queue through it, instead of walking past them in
-- messages_order, and the cost no longer grows with the messages in flight.
CREATE INDEX IF NOT EXISTS messages_ready
    ON messages(entity_id, subqueue, state, seq) WHERE lock_token IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS messages_lock
    ON messages(lock_token) WHERE lock_token IS NOT NULL;
-- By sub-queue and sequence: peek, purge and the sequence lookups. It
-- replaces messages_seq, which had no sub-queue.
CREATE INDEX IF NOT EXISTS messages_entity_subqueue_seq
    ON messages(entity_id, subqueue, seq);
DROP INDEX IF EXISTS messages_seq;
-- Timers processed by the background scheduler.
CREATE INDEX IF NOT EXISTS messages_expiry
    ON messages(expires_at) WHERE expires_at > 0 AND subqueue = 0;
CREATE INDEX IF NOT EXISTS messages_scheduled
    ON messages(visible_at) WHERE state = 2;
CREATE INDEX IF NOT EXISTS messages_locked
    ON messages(locked_until) WHERE locked_until > 0;

-- Message sessions: lock holder and state.
CREATE TABLE IF NOT EXISTS sessions (
    entity_id    INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    session_id   TEXT    NOT NULL,
    state        BLOB,
    lock_token   BLOB,
    locked_until INTEGER NOT NULL DEFAULT 0,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (entity_id, session_id)
);

-- Session lock expiry, for the timers. Sessions are never deleted, so
-- without it every timer run reads them all.
CREATE INDEX IF NOT EXISTS sessions_locked
    ON sessions(locked_until) WHERE locked_until > 0;

-- Message ids seen recently, for duplicate detection.
CREATE TABLE IF NOT EXISTS duplicates (
    entity_id  INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    message_id TEXT    NOT NULL,
    seen_at    INTEGER NOT NULL,
    PRIMARY KEY (entity_id, message_id)
);
-- Purging the records that left the detection window, oldest first, reads
-- only those.
CREATE INDEX IF NOT EXISTS duplicates_seen ON duplicates(entity_id, seen_at);

-- Bytes stored per entity, kept by triggers so quotas need no scan.
CREATE TABLE IF NOT EXISTS entity_sizes (
    entity_id  INTEGER PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
    size_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TRIGGER IF NOT EXISTS messages_size_insert AFTER INSERT ON messages BEGIN
    INSERT INTO entity_sizes (entity_id, size_bytes) VALUES (NEW.entity_id, LENGTH(NEW.payload))
        ON CONFLICT (entity_id) DO UPDATE SET size_bytes = size_bytes + excluded.size_bytes;
END;

CREATE TRIGGER IF NOT EXISTS messages_size_delete AFTER DELETE ON messages BEGIN
    UPDATE entity_sizes SET size_bytes = size_bytes - LENGTH(OLD.payload) WHERE entity_id = OLD.entity_id;
END;

-- Updates rewrite every column, so only real size changes adjust totals.
CREATE TRIGGER IF NOT EXISTS messages_size_update AFTER UPDATE OF payload, entity_id ON messages
    WHEN LENGTH(OLD.payload) <> LENGTH(NEW.payload) OR OLD.entity_id <> NEW.entity_id
BEGIN
    UPDATE entity_sizes SET size_bytes = size_bytes - LENGTH(OLD.payload) WHERE entity_id = OLD.entity_id;
    INSERT INTO entity_sizes (entity_id, size_bytes) VALUES (NEW.entity_id, LENGTH(NEW.payload))
        ON CONFLICT (entity_id) DO UPDATE SET size_bytes = size_bytes + excluded.size_bytes;
END;

-- Recomputed at every start, which repairs totals of older databases.
DELETE FROM entity_sizes;
INSERT INTO entity_sizes (entity_id, size_bytes)
    SELECT entity_id, SUM(LENGTH(payload)) FROM messages GROUP BY entity_id;

-- Messages per entity, sub-queue and state, kept by triggers so counting is a
-- read of a few rows instead of a walk over every message.
CREATE TABLE IF NOT EXISTS entity_counts (
    entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    subqueue  INTEGER NOT NULL,
    state     INTEGER NOT NULL,
    n         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (entity_id, subqueue, state)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS messages_count_insert AFTER INSERT ON messages BEGIN
    INSERT INTO entity_counts (entity_id, subqueue, state, n) VALUES (NEW.entity_id, NEW.subqueue, NEW.state, 1)
        ON CONFLICT (entity_id, subqueue, state) DO UPDATE SET n = n + 1;
END;

CREATE TRIGGER IF NOT EXISTS messages_count_delete AFTER DELETE ON messages BEGIN
    UPDATE entity_counts SET n = n - 1 WHERE entity_id = OLD.entity_id AND subqueue = OLD.subqueue AND state = OLD.state;
END;

-- Updates rewrite every column, so only a move between groups changes counts.
CREATE TRIGGER IF NOT EXISTS messages_count_update AFTER UPDATE OF state, subqueue, entity_id ON messages
    WHEN OLD.state <> NEW.state OR OLD.subqueue <> NEW.subqueue OR OLD.entity_id <> NEW.entity_id
BEGIN
    UPDATE entity_counts SET n = n - 1 WHERE entity_id = OLD.entity_id AND subqueue = OLD.subqueue AND state = OLD.state;
    INSERT INTO entity_counts (entity_id, subqueue, state, n) VALUES (NEW.entity_id, NEW.subqueue, NEW.state, 1)
        ON CONFLICT (entity_id, subqueue, state) DO UPDATE SET n = n + 1;
END;

-- Recomputed at every start, which repairs counts of older databases.
DELETE FROM entity_counts;
INSERT INTO entity_counts (entity_id, subqueue, state, n)
    SELECT entity_id, subqueue, state, COUNT(*) FROM messages GROUP BY entity_id, subqueue, state;

-- Metrics in one-minute buckets. entity is the normalized path of a queue,
-- topic or subscription, or "" for the namespace; minute is the Unix time
-- divided by 60. There is no foreign key: the history of a deleted entity
-- stays until retention removes it, as Azure Monitor keeps it.
CREATE TABLE IF NOT EXISTS metrics (
    entity TEXT    NOT NULL,
    minute INTEGER NOT NULL,
    name   TEXT    NOT NULL,
    value  INTEGER NOT NULL,
    PRIMARY KEY (entity, minute, name)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS metrics_minute ON metrics(minute);

-- Activity log: administrative operations on entities, rules and auth.
-- Times are Unix milliseconds.
CREATE TABLE IF NOT EXISTS activity_log (
    id        INTEGER PRIMARY KEY,
    time      INTEGER NOT NULL,
    actor     TEXT    NOT NULL,
    operation TEXT    NOT NULL,
    entity    TEXT    NOT NULL,
    status    TEXT    NOT NULL,
    detail    TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS activity_log_time ON activity_log(time);

-- Shared access policies. scope is the normalized path of a queue or
-- topic, or "" for the namespace. rights is a bit set: 1 Listen, 2 Send,
-- 4 Manage.
CREATE TABLE IF NOT EXISTS auth_policies (
    scope         TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    rights        INTEGER NOT NULL,
    primary_key   TEXT    NOT NULL,
    secondary_key TEXT    NOT NULL,
    PRIMARY KEY (scope, name)
) WITHOUT ROWID;

-- Role assignments of Entra ID principals: role is a Service Bus data
-- role, scope an entity path or "" for the namespace.
CREATE TABLE IF NOT EXISTS role_assignments (
    id        INTEGER PRIMARY KEY,
    principal TEXT NOT NULL,
    role      TEXT NOT NULL,
    scope     TEXT NOT NULL,
    UNIQUE (principal, role, scope)
);

-- Geo-DR aliases of the namespaces of one server, kept in the default
-- namespace's database so that a failover survives a restart.
CREATE TABLE IF NOT EXISTS geodr_aliases (
    alias     TEXT PRIMARY KEY,
    primary_ns   TEXT    NOT NULL,
    secondary_ns TEXT    NOT NULL,
    paired    INTEGER NOT NULL
) WITHOUT ROWID;

-- Namespace settings that are not entities: tags, locks and encryption,
-- each a JSON document under its key.
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
