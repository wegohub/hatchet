package tidb

import (
	"context"
	"database/sql"
	"fmt"
)

type TiFlashReplica struct {
	Table     string
	Available bool
	Progress  float64
}

var analyticalTables = []string{"v1_runs_olap", "v1_log_line"}

func EnableTiFlash(ctx context.Context, cfg Config) error {
	db, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := validateSchema(ctx, db); err != nil {
		return err
	}
	for _, table := range analyticalTables {
		if _, err := db.ExecContext(ctx, "ALTER TABLE "+table+" SET TIFLASH REPLICA 1"); err != nil {
			return fmt.Errorf("enable TiFlash for %s: %w", table, err)
		}
	}
	return nil
}

func TiFlashStatus(ctx context.Context, cfg Config) ([]TiFlashReplica, error) {
	db, err := open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return tiFlashStatusDB(ctx, db)
}

func tiFlashStatusDB(ctx context.Context, db *sql.DB) ([]TiFlashReplica, error) {
	var schema string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return nil, err
	}
	result := make([]TiFlashReplica, 0, len(analyticalTables))
	for _, table := range analyticalTables {
		status := TiFlashReplica{Table: table}
		rows, err := db.QueryContext(ctx, "SELECT AVAILABLE,PROGRESS FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", schema, table)
		if err != nil {
			return nil, fmt.Errorf("TiFlash status for %s: %w", table, err)
		}
		status.Available = true
		status.Progress = 1
		count := 0
		for rows.Next() {
			var available int
			var progress float64
			if err := rows.Scan(&available, &progress); err != nil {
				rows.Close()
				return nil, err
			}
			count++
			status.Available = status.Available && available == 1 && progress >= 1
			if progress < status.Progress {
				status.Progress = progress
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if count == 0 {
			status.Available = false
			status.Progress = 0
		}
		result = append(result, status)
	}
	return result, nil
}
