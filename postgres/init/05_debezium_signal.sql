-- Debezium signaling table: inserting a row here sends a command to the running
-- connector through the WAL (e.g. an ad-hoc incremental snapshot of chosen tables).
CREATE TABLE IF NOT EXISTS ecommerce.debezium_signal (
    id   VARCHAR(64)   PRIMARY KEY,
    type VARCHAR(32)   NOT NULL,
    data VARCHAR(2048)
);

-- Debezium reads signals and writes snapshot watermarks into this table
GRANT SELECT, INSERT, UPDATE, DELETE ON ecommerce.debezium_signal TO debezium;

-- Signals must reach Debezium through logical replication
ALTER PUBLICATION dbz_publication ADD TABLE ecommerce.debezium_signal;
