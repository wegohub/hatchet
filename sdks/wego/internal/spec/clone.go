package spec

import (
	"maps"
	"slices"
)

// Clone 复制 Runtime 的可变配置，Worker 局部选项不能改写 Conn 或另一个 Worker。
// logger、interceptor 和 exporter 是应用提供的能力对象，仅复制容器，不复制其内部状态。
func (r Runtime) Clone() Runtime {
	// out 是独立的配置视图，标量字段直接复制。
	out := r
	out.Labels = maps.Clone(r.Labels)
	out.Projections = make(map[string]map[string]string, len(r.Projections))
	// method 和 fields 分别复制两层映射，Worker 修改 group 投影不会改写 Conn。
	for method, fields := range r.Projections {
		out.Projections[method] = maps.Clone(fields)
	}
	out.Middleware = slices.Clone(r.Middleware)
	out.Telemetry.Trace.Exporters = slices.Clone(r.Telemetry.Trace.Exporters)
	if r.TLS != nil {
		out.TLS = r.TLS.Clone()
	}
	if r.Embedded != nil {
		// embedded 独立持有端口及数据库配置，局部修改不影响所属实例。
		embedded := *r.Embedded
		out.Embedded = &embedded
	}
	return out
}
