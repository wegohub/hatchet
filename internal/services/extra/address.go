package extra

import (
	"context"
	"encoding/base32"
	"strings"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// address 使用完整 namespace 编码，不能用截断摘要制造可碰撞的租户寻址空间。
type address struct {
	// names 用于 stream 和 bucket 名称，包含完整 tenant 与 namespace。
	names string
	// subjects 是不可通配的租户前缀，业务通配符只能出现在其后。
	subjects string
}

// addressFor 根据已认证租户和完整 namespace 编码生成不可碰撞的物理前缀。
func addressFor(ctx context.Context, scope *pb.Scope) (address, error) {
	id, err := tenant(ctx)
	if err != nil {
		return address{}, err
	}
	ns := scope.GetNamespace()
	if len(ns) > 64 || ns != "" && !validName(ns) {
		return address{}, invalid("invalid namespace")
	}
	n := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(ns))
	if n == "" {
		n = "0"
	}
	t := strings.ReplaceAll(id.String(), "-", "")
	return address{
		names:    "X_" + t + "_" + n + "_",
		subjects: "x." + t + "." + n + ".",
	}, nil
}

// name 校验逻辑资源名再追加租户前缀，业务不能直接指定物理资源。
func (a address) name(n string) (string, error) {
	if !validName(n) {
		return "", invalid("invalid resource name")
	}
	return a.names + n, nil
}

// subject 为业务 subject 添加固定租户前缀，按入口决定是否允许通配符。
func (a address) subject(s string, wild bool) (string, error) {
	if !validSubject(s, wild) {
		return "", invalid("invalid subject")
	}
	return a.subjects + s, nil
}

// logicalName 仅移除当前作用域前缀，保留原生资源的业务名称。
func (a address) logicalName(n string) string {
	return strings.TrimPrefix(n, a.names)
}

// logicalSubject 仅移除当前作用域的寻址前缀，不改变业务 subject 结构。
func (a address) logicalSubject(n string) string {
	return strings.TrimPrefix(n, a.subjects)
}

// stream 校验 stream 引用并生成认证租户内的物理地址。
func (s *Service) stream(ctx context.Context, r *pb.StreamRef) (address, string, error) {
	if r == nil {
		return address{}, "", invalid("stream required")
	}
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return a, "", err
	}
	n, err := a.name(r.Stream)
	return a, n, err
}

// bucket 校验 KV 桶引用并生成当前租户的物理地址。
func (s *Service) bucket(ctx context.Context, r *pb.BucketRef) (address, string, error) {
	if r == nil {
		return address{}, "", invalid("bucket required")
	}
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return a, "", err
	}
	n, err := a.name(r.Bucket)
	return a, n, err
}
