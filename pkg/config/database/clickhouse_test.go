package database

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/config/loader/loaderutils"
)

func TestOLAPConfiguration(t *testing.T) {
	for _, name := range []string{"DATABASE_OLAP_BACKEND", "DATABASE_CLICKHOUSE_ADDRESSES", "DATABASE_CLICKHOUSE_KEEPER_ADDRESSES", "DATABASE_CLICKHOUSE_PASSWORD", "DATABASE_CLICKHOUSE_KEEPER_AUTH"} {
		t.Setenv(name, "")
	}
	var cfg ConfigFile
	if _, err := loaderutils.LoadConfigFromViper(BindAllEnv, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.OLAPBackend != "postgres" {
		t.Fatalf("default backend %s", cfg.OLAPBackend)
	}
	if cfg.ClickHouse.DialTimeout != 5*time.Second || cfg.ClickHouse.KeeperSessionTimeout != 30*time.Second {
		t.Fatal("ClickHouse timeout defaults were not loaded")
	}
	t.Setenv("DATABASE_OLAP_BACKEND", "ck")
	t.Setenv("DATABASE_CLICKHOUSE_ADDRESSES", "127.0.0.1:9009,127.0.0.1:9010")
	t.Setenv("DATABASE_CLICKHOUSE_KEEPER_ADDRESSES", "127.0.0.1:9181")
	t.Setenv("DATABASE_CLICKHOUSE_PASSWORD", "test-password")
	t.Setenv("DATABASE_CLICKHOUSE_KEEPER_AUTH", "test-user:test-password")
	if _, err := loaderutils.LoadConfigFromViper(BindAllEnv, &cfg); err != nil {
		t.Fatal(err)
	}
	backend, err := NormalizeOLAPBackend(cfg.OLAPBackend)
	if err != nil || backend != "clickhouse" {
		t.Fatal("ClickHouse alias was not accepted")
	}
	if len(cfg.ClickHouse.Addresses) != 2 || len(cfg.ClickHouse.KeeperAddresses) != 1 {
		t.Fatal("ClickHouse address lists were not parsed")
	}
	if err = cfg.ClickHouse.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "test-password") {
		t.Fatal("ClickHouse configuration JSON exposed credentials")
	}
	if _, err = NormalizeOLAPBackend("invalid"); err == nil {
		t.Fatal("unknown backend was accepted")
	}
}
