package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

var testDatabase struct {
	sync.Mutex
	cfg     Config
	cleanup func()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if testDatabase.cleanup != nil {
		testDatabase.cleanup()
	}
	os.Exit(code)
}

// The suite shares the physical schema and clears rows between serial tests.
// Schema mutation tests use their own database so failures cannot contaminate it.
func reusableTestDatabase(t *testing.T) Config {
	t.Helper()
	testDatabase.Lock()
	defer testDatabase.Unlock()
	if testDatabase.cfg.DSN == "" {
		dsn := os.Getenv("TIDB_TEST_DSN")
		if dsn == "" {
			t.Skip("TIDB_TEST_DSN is required")
		}
		parsed, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		prefix := os.Getenv("TIDB_TEST_DATABASE_PREFIX")
		if prefix == "" {
			prefix = "hatchet_olap_test_"
		}
		name := fmt.Sprintf("%ssuite_%d", prefix, os.Getpid())
		parsed.DBName = ""
		admin, err := sql.Open("mysql", parsed.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err = admin.ExecContext(ctx, "CREATE DATABASE "+name)
		cancel()
		if err != nil {
			admin.Close()
			t.Fatal(err)
		}
		testDatabase.cleanup = func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := admin.ExecContext(ctx, "DROP DATABASE "+name); err != nil {
				fmt.Fprintln(os.Stderr, "drop isolated suite database:", err)
			}
			admin.Close()
		}
		parsed.DBName = name
		testDatabase.cfg = Config{DSN: parsed.FormatDSN(), QueryTimeout: 5 * time.Second}
	}
	cfg := testDatabase.cfg
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		db, err := open(ctx, cfg)
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()
		rows, err := db.QueryContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'")
		if err != nil {
			t.Error(err)
			return
		}
		var names []string
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				t.Error(err)
				return
			}
			if strings.HasPrefix(name, "v1_") {
				names = append(names, name)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Error(err)
			return
		}
		for _, name := range names {
			if _, err = db.ExecContext(ctx, "DELETE FROM `"+name+"`"); err != nil {
				t.Error(err)
				return
			}
		}
	})
	return cfg
}
