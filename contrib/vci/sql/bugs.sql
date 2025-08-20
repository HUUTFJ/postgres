-- Bug reported by Japin Li that caused a vci_beginscan PANIC
-- See https://www.postgresql.org/message-id/ME0P300MB04457E24CA8965F008FB2CDBB648A%40ME0P300MB0445.AUSP300.PROD.OUTLOOK.COM

CREATE TABLE t1 (id int, info text);
CREATE INDEX t1_id_idx ON t1 USING vci (id);
INSERT INTO t1 SELECT id, md5(id::text) FROM generate_series(1, 1000) id;
SET enable_seqscan TO off;
SELECT * FROM t1 WHERE id = 100;
DROP TABLE t1;

-- Bug reported by Japin Li that VACUUM caused a TRAP
-- See https://www.postgresql.org/message-id/SY8P300MB0442BEC3F5CF432F0121ACC4B642A%40SY8P300MB0442.AUSP300.PROD.OUTLOOK.COM

CREATE TABLE t2 (id int, info text) WITH (autovacuum_enabled = off);
CREATE INDEX t2_id_idx ON t2 USING vci (id);
INSERT INTO t2 SELECT id, 'test' || id FROM generate_series(1, 1000) id;
DELETE FROM t2 WHERE id % 10 = 0;
VACUUM t2;
DROP TABLE t2;

-- Bug reported by Japin Li that caused a Segmentation Violation attempting to REFRESH a VCI internal relation
-- See https://www.postgresql.org/message-id/ME0P300MB0445EBA04D6947DD717074DFB65CA%40ME0P300MB0445.AUSP300.PROD.OUTLOOK.COM

CREATE TABLE t3 (id int, info text);
CREATE INDEX ON t3 USING vci (id);
SELECT relname FROM pg_class WHERE relname ~ '^pg_vci_*' LIMIT 1 \gset
SELECT * FROM :relname;
\d+ :relname
REFRESH MATERIALIZED VIEW :relname;
DROP TABLE t3;

-- Bug missing logic. Ensure VCI internal relations get removed when the TABLE is dropped.

CREATE TABLE t4 (id int, info text);
CREATE INDEX t4_idx ON t4 USING vci (id);
SELECT relname FROM pg_class WHERE relname ~ '^pg_vci_*' ORDER BY relname;
DROP TABLE t4;
SELECT relname FROM pg_class WHERE relname ~ '^pg_vci_*';

-- Bug reported by Japin Li that REINDEX forgot to restore security context
-- See https://www.postgresql.org/message-id/ME0P300MB0445827B6E9CC04E0FAEE446B624A%40ME0P300MB0445.AUSP300.PROD.OUTLOOK.COM

CREATE TABLE t5 (id int, info text);
CREATE INDEX t5_idx ON t5 USING vci (id);
REINDEX TABLE t5;
REINDEX TABLE t5;
DROP TABLE t5;

-- InstrStartNode bug:
-- Unexpected error "InstrStartNode called twice in a row"
-- NOTE -Change the EXPLAIN below to use TIMING TRUE reproduce the bug,
-- otherwise leave it FALSE so timings don't cause 'make check' to fail.

CREATE TABLE t6(id int, info text);
CREATE INDEX t6_id_idx ON t6 USING vci (id);
INSERT INTO t6 SELECT id, 'info' || id FROM generate_series(1, 500) id;
ANALYZE t6;
EXPLAIN (ANALYZE, COSTS FALSE, TIMING FALSE, SUMMARY FALSE) SELECT max(id) FROM t6;
DROP TABLE t6;
