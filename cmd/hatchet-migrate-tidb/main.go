package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hatchet-dev/hatchet/pkg/config/loader"
	"github.com/hatchet-dev/hatchet/pkg/repository/tidb"
)

func main() {
	withTiFlash := flag.Bool("tiflash", false, "create TiFlash replicas for analytics tables")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cfg, err := loader.LoadDatabaseConfigFile()
	if err == nil {
		err = tidb.Migrate(ctx, cfg.TiDB)
	}
	if err == nil && *withTiFlash {
		err = tidb.EnableTiFlash(ctx, cfg.TiDB)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "TiDB migration failed:", err)
		os.Exit(1)
	}
	fmt.Println("TiDB OLAP schema is current")
	if *withTiFlash {
		statuses, err := tidb.TiFlashStatus(ctx, cfg.TiDB)
		if err != nil {
			fmt.Fprintln(os.Stderr, "TiFlash status unavailable:", err)
			os.Exit(1)
		}
		for _, status := range statuses {
			fmt.Printf("%s: available=%t progress=%.2f\n", status.Table, status.Available, status.Progress)
		}
	}
}
