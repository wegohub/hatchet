package clickhouse

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed migrations/*.sql
var migrations embed.FS

const schemaVersion uint64 = 1

func open(ctx context.Context, cfg Config, database string) (driver.Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	conn, err := ch.Open(cfg.options(database))
	if err != nil {
		return nil, err
	}
	conn = &boundedConn{Conn: conn, timeout: cfg.QueryTimeout}
	if err = conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect ClickHouse: %w", err)
	}
	return conn, nil
}

// Migrate creates the namespace explicitly; normal server startup only validates it.
func Migrate(ctx context.Context, cfg Config) error {
	conn, err := open(ctx, cfg, "default")
	if err != nil {
		return err
	}
	defer conn.Close()
	k, err := connectKeeper(ctx, cfg)
	if err != nil {
		return err
	}
	defer k.close()
	if err = k.initialize(ctx); err != nil {
		return err
	}
	if err = k.bindNamespace(ctx, cfg.Database); err != nil {
		return err
	}
	l, err := k.lock(ctx, "schema")
	if err != nil {
		return err
	}
	defer l.release()
	if err = conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+cfg.Database+" ENGINE = Atomic"); err != nil {
		return err
	}
	var exists uint64
	if err = conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = 'schema_version'", cfg.Database).Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		var version uint64
		if err = conn.QueryRow(ctx, "SELECT max(version) FROM "+cfg.Database+".schema_version").Scan(&version); err != nil {
			return err
		}
		if version > schemaVersion {
			return fmt.Errorf("ClickHouse schema version %d is newer than this migrator", version)
		}
	}
	sql, err := migrations.ReadFile("migrations/001_initial.sql")
	if err != nil {
		return err
	}
	text := strings.NewReplacer("{database}", cfg.Database, "{keeper_root}", cfg.KeeperRoot).Replace(string(sql))
	for _, statement := range strings.Split(text, ";") {
		if strings.TrimSpace(statement) != "" {
			if err = conn.Exec(ctx, statement); err != nil {
				return fmt.Errorf("ClickHouse schema migration: %w", err)
			}
		}
	}
	dbConn, err := open(ctx, cfg, cfg.Database)
	if err != nil {
		return err
	}
	defer dbConn.Close()
	return validateSchema(ctx, dbConn, cfg.KeeperRoot)
}

func validateSchema(ctx context.Context, conn driver.Conn, keeperRoot string) error {
	var version uint64
	if err := conn.QueryRow(ctx, "SELECT max(version) FROM schema_version").Scan(&version); err != nil {
		return fmt.Errorf("ClickHouse schema unavailable; run hatchet-migrate-clickhouse: %w", err)
	}
	if version != schemaVersion {
		return fmt.Errorf("ClickHouse schema version %d, required %d", version, schemaVersion)
	}
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table IN ('schema_version','entities','manifests','commits','log_lines') AND engine = 'ReplicatedReplacingMergeTree' AND startsWith(zookeeper_path, ?)", keeperRoot+"/tables/").Scan(&count); err != nil {
		return err
	}
	if count != 5 {
		return errors.New("ClickHouse repository tables are missing or have incompatible engines")
	}
	expected := map[string]map[string]string{
		"schema_version": {"version": "UInt64"},
		"entities":       {"tenant": "UUID", "kind": "LowCardinality(String)", "entity_key": "String", "external_id": "UUID", "run_id": "UUID", "task_id": "Int64", "inserted_at": "DateTime64(6, 'UTC')", "batch_id": "UUID", "ordinal": "UInt64", "body": "String"},
		"manifests":      {"batch_id": "UUID", "row_count": "UInt64", "digest": "String", "created_at": "DateTime64(6, 'UTC')"},
		"commits":        {"batch_id": "UUID", "sequence": "UInt64"},
		"log_lines":      {"tenant": "UUID", "id": "Int64", "created_at": "DateTime64(6, 'UTC')", "task_id": "Int64", "task_inserted_at": "DateTime64(6, 'UTC')", "workflow_id": "UUID", "step_id": "UUID", "retry_count": "Int32", "level": "LowCardinality(String)", "message": "String", "body": "String", "batch_id": "UUID", "ordinal": "UInt64"},
	}
	rows, err := conn.Query(ctx, "SELECT table,name,type FROM system.columns WHERE database=currentDatabase() AND table IN ('schema_version','entities','manifests','commits','log_lines')")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var table, column, typ string
		if err = rows.Scan(&table, &column, &typ); err != nil {
			return err
		}
		want, ok := expected[table][column]
		if !ok {
			continue
		}
		if typ != want {
			return fmt.Errorf("ClickHouse %s.%s has type %s, required %s", table, column, typ, want)
		}
		delete(expected[table], column)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for table, columns := range expected {
		for column := range columns {
			return fmt.Errorf("ClickHouse schema is missing %s.%s", table, column)
		}
	}
	return nil
}
