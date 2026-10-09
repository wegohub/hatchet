package spec

import (
	"maps"
	"slices"
)

// Clone 复制 Runtime 的可变配置，Worker 局部选项不能改写 Conn 或另一个 Worker
// logger、interceptor 和 exporter 是应用提供的能力对象，仅复制容器，不复制其内部状态
func (r *Runtime) Clone() Runtime {
	// out 是独立的配置视图，标量字段直接复制
	out := *r
	out.Labels = maps.Clone(r.Labels)
	out.StreamMethods = maps.Clone(r.StreamMethods)
	out.RoutingDefaults = make(map[string]map[string]any, len(r.RoutingDefaults))
	// method 和 fields 分别复制两层映射，Worker 修改嵌套 routing 默认值不会改写 Conn
	for method, fields := range r.RoutingDefaults {
		snapshot, err := SnapshotRouting(fields)
		if err != nil {
			out.routingError = err
		}
		out.RoutingDefaults[method] = snapshot
	}
	out.Middleware = slices.Clone(r.Middleware)
	out.Telemetry.Trace.Exporters = slices.Clone(r.Telemetry.Trace.Exporters)
	if r.TLS != nil {
		out.TLS = r.TLS.Clone()
	}
	if r.Embedded != nil {
		// embedded 独立持有端口及数据库配置，局部修改不影响所属实例
		embedded := *r.Embedded
		out.Embedded = &embedded
	}
	return out
}
