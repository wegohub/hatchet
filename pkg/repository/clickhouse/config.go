package clickhouse

import (
	"crypto/tls"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// Config describes a single replicated shard and its coordination namespace.
type Config struct {
	Addresses              []string      `mapstructure:"addresses" json:"addresses,omitempty"`
	Database               string        `mapstructure:"database" json:"database,omitempty" default:"hatchet_olap"`
	Username               string        `mapstructure:"username" json:"username,omitempty" default:"default"`
	Password               string        `mapstructure:"password" json:"-"`
	TLS                    bool          `mapstructure:"tls" json:"tls,omitempty" default:"false"`
	KeeperAddresses        []string      `mapstructure:"keeperAddresses" json:"keeperAddresses,omitempty"`
	KeeperRoot             string        `mapstructure:"keeperRoot" json:"keeperRoot,omitempty" default:"/hatchet/olap/hatchet_olap"`
	KeeperAuth             string        `mapstructure:"keeperAuth" json:"-"`
	DialTimeout            time.Duration `mapstructure:"dialTimeout" json:"dialTimeout,omitempty" default:"5s"`
	QueryTimeout           time.Duration `mapstructure:"queryTimeout" json:"queryTimeout,omitempty" default:"30s"`
	KeeperSessionTimeout   time.Duration `mapstructure:"keeperSessionTimeout" json:"keeperSessionTimeout,omitempty" default:"30s"`
	KeeperOperationTimeout time.Duration `mapstructure:"keeperOperationTimeout" json:"keeperOperationTimeout,omitempty" default:"10s"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var keeperPath = regexp.MustCompile(`^/[a-zA-Z0-9_./-]+$`)

func (c Config) Validate() error {
	if len(c.Addresses) == 0 || len(c.KeeperAddresses) == 0 {
		return fmt.Errorf("ClickHouse and Keeper addresses are required")
	}
	for _, address := range append(append([]string{}, c.Addresses...), c.KeeperAddresses...) {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("ClickHouse and Keeper addresses cannot be empty")
		}
	}
	if !identifier.MatchString(c.Database) {
		return fmt.Errorf("invalid ClickHouse database identifier")
	}
	if c.KeeperRoot == "/" || !keeperPath.MatchString(c.KeeperRoot) || path.Clean(c.KeeperRoot) != c.KeeperRoot {
		return fmt.Errorf("invalid Keeper namespace")
	}
	if c.DialTimeout <= 0 || c.QueryTimeout <= 0 || c.KeeperSessionTimeout <= 0 || c.KeeperOperationTimeout <= 0 {
		return fmt.Errorf("ClickHouse and Keeper timeouts must be positive")
	}
	return nil
}

func (c Config) options(database string) *ch.Options {
	opts := &ch.Options{Addr: c.Addresses, Auth: ch.Auth{Database: database, Username: c.Username, Password: c.Password}, DialTimeout: c.DialTimeout, ReadTimeout: c.QueryTimeout, MaxOpenConns: 8, MaxIdleConns: 8, ConnMaxLifetime: time.Hour, Settings: ch.Settings{"async_insert": 0, "insert_quorum": 0, "insert_quorum_timeout": c.QueryTimeout.Milliseconds(), "insert_quorum_parallel": 0, "select_sequential_consistency": 1}}
	if c.TLS {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return opts
}
