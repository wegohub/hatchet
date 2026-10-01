package database

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/config/loader/loaderutils"
)

func TestTiDBOLAPConfiguration(t *testing.T) {
	t.Setenv("DATABASE_OLAP_BACKEND", "tidb")
	t.Setenv("DATABASE_TIDB_DSN", "user:secret@tcp(127.0.0.1:4000)/olap")
	t.Setenv("DATABASE_TIDB_QUERY_TIMEOUT", "2s")
	t.Setenv("DATABASE_TIDB_WRITE_CONCURRENCY", "8")
	t.Setenv("DATABASE_TIDB_TIFLASH_QUERY_TIMEOUT", "500ms")
	var cfg ConfigFile
	if _, err := loaderutils.LoadConfigFromViper(BindAllEnv, &cfg); err != nil {
		t.Fatal(err)
	}
	backend, err := NormalizeOLAPBackend(cfg.OLAPBackend)
	if err != nil || backend != "tidb" || cfg.TiDB.QueryTimeout != 2*time.Second || cfg.TiDB.WriteConcurrency != 8 || cfg.TiDB.TiFlashQueryTimeout != 500*time.Millisecond {
		t.Fatalf("TiDB configuration: backend=%q timeout=%s err=%v", backend, cfg.TiDB.QueryTimeout, err)
	}
	if err := cfg.TiDB.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatal("TiDB DSN exposed by configuration JSON")
	}
}
