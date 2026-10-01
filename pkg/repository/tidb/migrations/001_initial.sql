CREATE TABLE IF NOT EXISTS schema_version (
    version BIGINT NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS entity (
    tenant CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    entity_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    external_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task_id BIGINT NOT NULL,
    inserted_at DATETIME(6) NOT NULL,
    body LONGTEXT NOT NULL,
    readable_status VARCHAR(16) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.readable_status'))) STORED,
    PRIMARY KEY (tenant, kind, entity_key),
    KEY entity_external (tenant, kind, external_id),
    KEY entity_run (tenant, kind, run_id),
    KEY entity_task (tenant, kind, task_id),
    KEY entity_time (tenant, kind, inserted_at),
    KEY entity_status (tenant, kind, inserted_at, readable_status)
);
CREATE TABLE IF NOT EXISTS log_line (
    tenant CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    id BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    task_id BIGINT NOT NULL,
    task_inserted_at DATETIME(6) NOT NULL,
    workflow_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    step_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    retry_count INT NOT NULL,
    level VARCHAR(8) NOT NULL,
    message TEXT NOT NULL,
    body LONGTEXT NOT NULL,
    PRIMARY KEY (tenant, id, created_at),
    KEY log_time (tenant, created_at, id),
    KEY log_task (tenant, task_id, created_at),
    KEY log_workflow (tenant, workflow_id, created_at),
    KEY log_step (tenant, step_id, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE IF NOT EXISTS run_summary (
    tenant CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind VARCHAR(4) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    external_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    inserted_at DATETIME(6) NOT NULL,
    workflow_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    parent_external_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    idempotency_key VARCHAR(255) NULL,
    readable_status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    is_dag_child TINYINT(1) NOT NULL,
    PRIMARY KEY (tenant, kind, external_id, inserted_at),
    KEY run_summary_time (tenant, inserted_at, kind, is_dag_child, readable_status),
    KEY run_summary_workflow (tenant, workflow_id, inserted_at, readable_status),
    KEY run_summary_parent (tenant, parent_external_id, inserted_at)
) PARTITION BY RANGE COLUMNS(inserted_at) (
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
CREATE TABLE IF NOT EXISTS repository_lock (
    lock_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS repository_sequence (
    name VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    next_id BIGINT UNSIGNED NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS event_id_seq CACHE 1000;
INSERT INTO repository_sequence (name, next_id) VALUES ('entity', 1)
    ON DUPLICATE KEY UPDATE next_id = next_id;
