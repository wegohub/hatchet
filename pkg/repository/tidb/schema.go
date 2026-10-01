package tidb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

//go:embed migrations/*.sql migrations/v2/*.sql
var migrationFiles embed.FS

const schemaVersion = 2

func open(ctx context.Context, cfg Config) (*sql.DB, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	parsed, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, errors.New("invalid TiDB DSN")
	}
	parsed.ParseTime = true
	// One-shot explicit SQL avoids prepare/execute/close roundtrips. Parameter
	// encoding and escaping remain the MySQL driver's responsibility.
	parsed.InterpolateParams = true
	if parsed.Timeout == 0 {
		parsed.Timeout = 5 * time.Second
	}
	parsed.Loc = time.UTC
	parsed.Params = copyParams(parsed.Params)
	parsed.Params["time_zone"] = "'+00:00'"
	parsed.Params["tidb_txn_mode"] = "'pessimistic'"
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return nil, errors.New("cannot open TiDB connection")
	}
	n := cfg.MaxOpenConns
	if n == 0 {
		n = 40
	}
	idle := cfg.MaxIdleConns
	if idle == 0 {
		idle = 10
	}
	life := cfg.ConnMaxLifetime
	if life == 0 {
		life = 15 * time.Minute
	}
	db.SetMaxOpenConns(n)
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(life)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect TiDB: %w", err)
	}
	return db, nil
}
func copyParams(src map[string]string) map[string]string {
	out := make(map[string]string, len(src)+2)
	for k, v := range src {
		out[k] = v
	}
	return out
}
func v2Files() ([]string, error) {
	files, err := fs.Glob(migrationFiles, "migrations/v2/*.sql")
	sort.Strings(files)
	return files, err
}
func Migrate(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	db, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	var legacy int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('entity','run_summary','log_line')").Scan(&legacy); err != nil {
		return err
	}
	if legacy > 0 {
		return errors.New("TiDB schema v1 cannot be upgraded in place; select a new empty OLAP database")
	}
	var tracking, tables int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(TABLE_NAME='schema_migrations'),0) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()").Scan(&tables, &tracking); err != nil {
		return err
	}
	if tables > 0 && tracking == 0 {
		return errors.New("TiDB v2 initialization requires an empty database")
	}
	if _, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (filename VARBINARY(255) PRIMARY KEY, checksum BINARY(32) NOT NULL)"); err != nil {
		return err
	}
	files, err := v2Files()
	if err != nil {
		return err
	}
	for _, file := range files {
		raw, err := migrationFiles.ReadFile(file)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		var got []byte
		err = db.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE filename=?", file).Scan(&got)
		if err == nil {
			if string(got) != string(digest[:]) {
				return fmt.Errorf("TiDB migration checksum differs: %s", file)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		for _, stmt := range strings.Split(string(raw), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			// All partitioned tables start with the complete retained window in one DDL.
			stmt = strings.ReplaceAll(stmt, "PARTITION pmax VALUES LESS THAN (MAXVALUE)", initialPartitions(time.Now(), 30*24*time.Hour))
			statementCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			_, err = db.ExecContext(statementCtx, stmt)
			cancel()
			if err != nil {
				return fmt.Errorf("TiDB migration %s: %w", file, err)
			}
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO schema_migrations(filename,checksum) VALUES (?,?)", file, digest[:]); err != nil {
			return err
		}
	}
	if _, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_version (version BIGINT PRIMARY KEY)"); err != nil {
		return err
	}
	if err = validateV2Objects(ctx, db); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "INSERT IGNORE INTO schema_version(version) VALUES (?)", schemaVersion); err != nil {
		return err
	}
	return validateSchema(ctx, db)
}
func validateSchema(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var version int
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_version").Scan(&version); err != nil {
		return fmt.Errorf("TiDB schema unavailable; initialize an empty v2 database: %w", err)
	}
	if version != schemaVersion {
		return fmt.Errorf("TiDB schema version %d, required %d; select a new OLAP database", version, schemaVersion)
	}
	files, err := v2Files()
	if err != nil {
		return err
	}
	for _, file := range files {
		raw, err := migrationFiles.ReadFile(file)
		if err != nil {
			return err
		}
		want := sha256.Sum256(raw)
		var actual []byte
		if err = db.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE filename=?", file).Scan(&actual); err != nil || string(actual) != string(want[:]) {
			return fmt.Errorf("TiDB migration missing or checksum differs: %s", file)
		}
	}
	return validateV2Objects(ctx, db)
}

type schemaColumn struct {
	typ       string
	length    int64
	notNull   bool
	precision int64
	charset   string
	collation string
}
type schemaIndex struct {
	columns []string
	unique  bool
}

func validateV2Objects(ctx context.Context, db *sql.DB) error {
	files, err := v2Files()
	if err != nil {
		return err
	}
	for _, file := range files {
		raw, _ := migrationFiles.ReadFile(file)
		for _, stmt := range strings.Split(string(raw), ";") {
			stmt = strings.TrimSpace(stmt)
			words := strings.Fields(stmt)
			if len(words) < 7 || words[0] != "CREATE" || words[1] != "TABLE" {
				continue
			}
			table := words[5]
			columns := map[string]schemaColumn{}
			indexes := map[string]schemaIndex{}
			for _, line := range strings.Split(stmt, "\n")[1:] {
				line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), ","))
				parts := strings.Fields(line)
				if len(parts) < 2 || strings.HasPrefix(line, ")") {
					continue
				}
				name := strings.Trim(parts[0], "`,")
				if name == "PRIMARY" || name == "KEY" || name == "UNIQUE" {
					index := "PRIMARY"
					unique := name != "KEY"
					if name == "KEY" {
						index = parts[1]
					}
					if name == "UNIQUE" {
						index = parts[2]
					}
					start := strings.Index(line, "(")
					end := strings.LastIndex(line, ")")
					if start < 0 || end < 0 {
						return fmt.Errorf("invalid index definition %s", table)
					}
					var keys []string
					for _, col := range strings.Split(line[start+1:end], ",") {
						keys = append(keys, strings.TrimSpace(strings.ReplaceAll(col, "`", "")))
					}
					indexes[index] = schemaIndex{columns: keys, unique: unique}
					continue
				}
				typ := strings.ToLower(parts[1])
				c := schemaColumn{notNull: strings.Contains(line, "NOT NULL") || strings.Contains(line, "PRIMARY KEY")}
				if strings.HasPrefix(typ, "datetime(") {
					c.precision = 6
				}
				if i := strings.Index(typ, "("); i >= 0 {
					n, _ := strconv.ParseInt(strings.TrimSuffix(typ[i+1:], ")"), 10, 64)
					if strings.HasPrefix(typ, "binary") || strings.HasPrefix(typ, "varbinary") || strings.HasPrefix(typ, "varchar") {
						c.length = n
					}
					typ = typ[:i]
				}
				if typ == "boolean" {
					typ = "tinyint"
				}
				c.typ = typ
				if typ == "varchar" || typ == "longtext" {
					c.charset, c.collation = "utf8mb4", "utf8mb4_bin"
					for i, token := range parts {
						if token == "SET" && i > 0 && parts[i-1] == "CHARACTER" && i+1 < len(parts) {
							c.charset = strings.TrimSuffix(parts[i+1], ",")
						}
						if token == "COLLATE" && i+1 < len(parts) {
							c.collation = strings.TrimSuffix(parts[i+1], ",")
						}
					}
				}
				columns[name] = c
				if strings.Contains(line, "PRIMARY KEY") {
					indexes["PRIMARY"] = schemaIndex{columns: []string{name}, unique: true}
				}
			}
			rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?", table)
			if err != nil {
				return err
			}
			for rows.Next() {
				var name, typ, nullable string
				var length, precision sql.NullInt64
				var charset, collation sql.NullString
				if err = rows.Scan(&name, &typ, &length, &nullable, &precision, &charset, &collation); err != nil {
					rows.Close()
					return err
				}
				if c, ok := columns[name]; ok {
					if c.typ != typ || c.length > 0 && c.length != length.Int64 || c.notNull != (nullable == "NO") || c.precision > 0 && c.precision != precision.Int64 || c.charset != "" && (c.charset != charset.String || c.collation != collation.String) {
						rows.Close()
						return fmt.Errorf("TiDB schema column differs: %s.%s", table, name)
					}
					delete(columns, name)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(columns) > 0 {
				return fmt.Errorf("TiDB schema incomplete: %s columns %v", table, columns)
			}
			rows, err = db.QueryContext(ctx, "SELECT INDEX_NAME,COLUMN_NAME,SUB_PART,NON_UNIQUE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX", table)
			if err != nil {
				return err
			}
			actual := map[string]schemaIndex{}
			for rows.Next() {
				var name, col string
				var prefix sql.NullInt64
				var nonunique int
				if err = rows.Scan(&name, &col, &prefix, &nonunique); err != nil {
					rows.Close()
					return err
				}
				if prefix.Valid {
					col += fmt.Sprintf("(%d)", prefix.Int64)
				}
				index := actual[name]
				index.columns = append(index.columns, col)
				index.unique = nonunique == 0
				actual[name] = index
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for name, want := range indexes {
				got, ok := actual[name]
				if !ok || want.unique != got.unique || strings.Join(want.columns, ",") != strings.Join(got.columns, ",") {
					return fmt.Errorf("TiDB schema index differs: %s.%s", table, name)
				}
			}
			if field, ok := partitionTables[table]; ok {
				var expression string
				if err = db.QueryRowContext(ctx, "SELECT PARTITION_EXPRESSION FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? LIMIT 1", table).Scan(&expression); err != nil {
					return err
				}
				if strings.Trim(expression, "`") != field {
					return fmt.Errorf("TiDB partition field differs: %s", table)
				}
			}
		}
	}
	for _, sequence := range []string{"event_id_seq", "log_id_seq"} {
		var n int
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA=DATABASE() AND SEQUENCE_NAME=?", sequence).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("TiDB sequence missing: %s", sequence)
		}
	}
	return nil
}
