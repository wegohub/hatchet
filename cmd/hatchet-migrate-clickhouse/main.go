package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hatchet-dev/hatchet/pkg/config/loader"
	"github.com/hatchet-dev/hatchet/pkg/repository/clickhouse"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cfg, err := loader.LoadDatabaseConfigFile()
	if err == nil {
		err = clickhouse.Migrate(ctx, cfg.ClickHouse)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ClickHouse migration failed:", err)
		os.Exit(1)
	}
	fmt.Println("ClickHouse OLAP schema is current")
}
