package client

import "github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"

// WithRouting 为所有任务提交显式 CEL 字段；例如 group=a、cost=3，不会自动转换成 Worker 亲和。
func WithRouting(values map[string]any) RunOption { return spec.WithRouting(values) }
