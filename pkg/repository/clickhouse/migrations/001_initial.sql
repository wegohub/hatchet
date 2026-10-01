CREATE TABLE IF NOT EXISTS {database}.schema_version (version UInt64)
ENGINE = ReplicatedReplacingMergeTree('{keeper_root}/tables/schema_version/{shard}', '{replica}')
ORDER BY version;

CREATE TABLE IF NOT EXISTS {database}.entities (
    tenant UUID,
    kind LowCardinality(String),
    entity_key String,
    external_id UUID,
    run_id UUID,
    task_id Int64,
    inserted_at DateTime64(6, 'UTC'),
    batch_id UUID,
    ordinal UInt64,
    body String,
    INDEX external_id_index external_id TYPE bloom_filter GRANULARITY 1,
    INDEX run_id_index run_id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree('{keeper_root}/tables/entities/{shard}', '{replica}', ordinal)
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (tenant, kind, entity_key, batch_id);

CREATE TABLE IF NOT EXISTS {database}.manifests (
    batch_id UUID, row_count UInt64, digest String, created_at DateTime64(6, 'UTC')
) ENGINE = ReplicatedReplacingMergeTree('{keeper_root}/tables/manifests/{shard}', '{replica}')
ORDER BY batch_id;

CREATE TABLE IF NOT EXISTS {database}.commits (
    batch_id UUID, sequence UInt64
) ENGINE = ReplicatedReplacingMergeTree('{keeper_root}/tables/commits/{shard}', '{replica}')
ORDER BY batch_id;

CREATE TABLE IF NOT EXISTS {database}.log_lines (
    tenant UUID, id Int64, created_at DateTime64(6, 'UTC'), task_id Int64,
    task_inserted_at DateTime64(6, 'UTC'), workflow_id UUID, step_id UUID,
    retry_count Int32, level LowCardinality(String), message String,
    body String, batch_id UUID, ordinal UInt64
) ENGINE = ReplicatedReplacingMergeTree('{keeper_root}/tables/log_lines/{shard}', '{replica}', ordinal)
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant, created_at, id);

INSERT INTO {database}.schema_version SELECT toUInt64(1);
