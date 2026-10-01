CREATE TABLE IF NOT EXISTS v1_runs_olap (
 tenant_id BINARY(16) NOT NULL,
 kind VARCHAR(4) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 external_id BINARY(16) NOT NULL,
 entity_key VARBINARY(255) NOT NULL,
 id BIGINT NOT NULL,
 inserted_at DATETIME(6) NOT NULL,
 workflow_id BINARY(16) NOT NULL,
 workflow_run_id BINARY(16) NOT NULL,
 parent_external_id BINARY(16) NULL,
 idempotency_key LONGTEXT NULL,
 readable_status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 latest_retry_count INT NOT NULL DEFAULT 0,
 latest_worker_id BINARY(16) NULL,
 is_dag_child BOOLEAN NOT NULL DEFAULT FALSE,
 is_placeholder BOOLEAN NOT NULL DEFAULT FALSE,
 queued_count BIGINT NOT NULL DEFAULT 0,
 running_count BIGINT NOT NULL DEFAULT 0,
 evicted_count BIGINT NOT NULL DEFAULT 0,
 completed_count BIGINT NOT NULL DEFAULT 0,
 cancelled_count BIGINT NOT NULL DEFAULT 0,
 failed_count BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (tenant_id,external_id,inserted_at,kind),
 KEY run_time (tenant_id,inserted_at,id),
 KEY run_workflow (tenant_id,workflow_id,inserted_at,id),
 KEY run_status (tenant_id,readable_status,inserted_at,id),
 KEY run_parent (tenant_id,parent_external_id,inserted_at),
 KEY run_members (tenant_id,workflow_run_id,kind,inserted_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY RANGE COLUMNS(inserted_at) (PARTITION pmax VALUES LESS THAN (MAXVALUE));
CREATE TABLE IF NOT EXISTS v1_lookup_table_olap (
 external_id BINARY(16) NOT NULL,
 kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 tenant_id BINARY(16) NOT NULL,
 entity_key VARBINARY(255) NOT NULL,
 inserted_at DATETIME(6) NOT NULL,
 PRIMARY KEY (external_id,kind,inserted_at,entity_key),
 KEY lookup_retention (inserted_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_olap_run_locks (
 lock_key VARBINARY(255) NOT NULL PRIMARY KEY,
 initialized BOOLEAN NOT NULL DEFAULT FALSE,
 tenant_id BINARY(16) NULL,
 run_id BINARY(16) NULL,
 touched_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY lock_retention (touched_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_olap_write_receipts (
 receipt_id BINARY(32) NOT NULL PRIMARY KEY,
 committed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY receipt_retention (committed_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_olap_pending_updates (
 tenant_id BINARY(16) NOT NULL,
 task_id BIGINT NOT NULL,
 task_inserted_at DATETIME(6) NOT NULL,
 run_id BINARY(16) NOT NULL,
 kind VARCHAR(4) NOT NULL,
 retry_after DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 retries INT NOT NULL DEFAULT 0,
 PRIMARY KEY (tenant_id,task_id,task_inserted_at,kind),
 KEY pending_retry (kind,retry_after,tenant_id),
 KEY pending_run (tenant_id,run_id),
 KEY pending_retention (task_inserted_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_olap_metadata (
 tenant_id BINARY(16) NOT NULL,
 kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 entity_key VARBINARY(255) NOT NULL,
 inserted_at DATETIME(6) NOT NULL,
 node_id INT NOT NULL,
 parent_node INT NOT NULL,
 property_name LONGBLOB NULL,
 property_hash BINARY(32) NOT NULL,
 node_type VARCHAR(8) NOT NULL,
 atom LONGBLOB NULL,
 atom_hash BINARY(32) NOT NULL,
 pg_text LONGBLOB NULL,
 PRIMARY KEY (tenant_id,kind,entity_key,inserted_at,node_id),
 KEY metadata_parent (tenant_id,kind,entity_key,inserted_at,parent_node,property_hash,node_type),
 KEY metadata_value (tenant_id,kind,property_hash,node_type,atom_hash,inserted_at),
 KEY metadata_retention (inserted_at)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_payloads_olap_cutover_job_offset (
 job_id BINARY(16) NOT NULL PRIMARY KEY,
 state VARCHAR(16) NOT NULL,
 lease_until DATETIME(6) NOT NULL,
 index_file LONGTEXT NULL,
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY job_retry (state,lease_until)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS v1_payloads_olap_offloaded_block_index (
 tenant_id BINARY(16) NOT NULL,
 external_id BINARY(16) NOT NULL,
 inserted_at DATETIME(6) NOT NULL,
 job_id BINARY(16) NOT NULL,
 PRIMARY KEY (tenant_id,external_id,inserted_at),
 KEY offload_job (job_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE SEQUENCE IF NOT EXISTS event_id_seq CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS log_id_seq CACHE 1000;
