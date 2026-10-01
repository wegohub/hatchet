package tidb

import (
	"errors"
	"time"
)

type Config struct {
	DSN                 string        `mapstructure:"dsn" json:"-"`
	MaxOpenConns        int           `mapstructure:"maxOpenConns" json:"maxOpenConns,omitempty" default:"40"`
	MaxIdleConns        int           `mapstructure:"maxIdleConns" json:"maxIdleConns,omitempty" default:"10"`
	ConnMaxLifetime     time.Duration `mapstructure:"connMaxLifetime" json:"connMaxLifetime,omitempty" default:"15m"`
	WriteConcurrency    int           `mapstructure:"writeConcurrency" json:"writeConcurrency,omitempty" default:"8"`
	TiFlashQueryTimeout time.Duration `mapstructure:"tiFlashQueryTimeout" json:"tiFlashQueryTimeout,omitempty" default:"500ms"`
	QueryTimeout        time.Duration `mapstructure:"queryTimeout" json:"queryTimeout,omitempty" default:"5s"`
}

func (c Config) Validate() error {
	if c.DSN == "" {
		return errors.New("TiDB DSN is required")
	}
	if c.WriteConcurrency < 0 || c.TiFlashQueryTimeout < 0 || c.MaxOpenConns < 0 || c.MaxIdleConns < 0 || c.ConnMaxLifetime < 0 || c.QueryTimeout < 0 {
		return errors.New("TiDB pool settings must be nonnegative")
	}
	return nil
}
