//go:build wego_embedded

package backend

import (
	"context"

	embed "github.com/hatchet-dev/hatchet-embedded"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// startEmbedded 仅在显式构建标签下启用；测试在独立进程运行，隔离引擎环境变量初始化。
func startEmbedded(c spec.Runtime) (spec.Runtime, func(context.Context) error, error) {
	// v 已显式提供的嵌入或驱逐配置视图，读取各字段后转换到私有后端配置。
	v := c.Embedded
	// opts 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系。
	opts := []embed.Option{embed.WithPostgres(v.DatabaseURL)}
	if v.GRPCPort != 0 {
		opts = append(opts, embed.WithGRPCPort(v.GRPCPort))
	}
	if v.APIPort != 0 {
		opts = append(opts, embed.WithAPIPort(v.APIPort))
	}
	if v.DisableAPI {
		opts = append(opts, embed.WithoutAPI())
	}
	if v.DisableMigrations {
		opts = append(opts, embed.WithoutMigrations())
	}
	if v.LogLevel != "" {
		opts = append(opts, embed.WithLogLevel(v.LogLevel))
	}
	// instance, err 接收 embed.StartServer 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	instance, err := embed.StartServer(context.Background(), opts...)
	if err != nil {
		return c, nil, err
	}

	c.Token = instance.Token()
	c.TenantID = instance.TenantID()
	c.Address = instance.GRPCAddress()
	c.ServerURL = instance.APIURL()
	c.TLS, c.TLSSet = nil, true
	return c, func(ctx context.Context) error {
		return instance.Shutdown(ctx)
	}, nil
}
