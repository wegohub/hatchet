package logging

import (
	"context"
	"log/slog"
	"slices"
)

// binding 是一段固定属性及其分组路径；例如 path=[request] 中的 id 与根 id 不冲突。
type binding struct {
	// path 保存 WithGroup 时的不可变路径。
	path []string
	// attrs 保存当前 WithAttrs 的独立容器，不下沉到无法再去重的底层 Handler。
	attrs []slog.Attr
}

// handler 合并固定属性、上下文和记录属性，再委托实际输出。
type handler struct {
	// base 是实际输出 Handler；所有派生对象共享其并发安全实现。
	base slog.Handler
	// bindings 按调用顺序保存固定属性及分组范围。
	bindings []binding
	// groups 是后续上下文和记录属性的分组路径。
	groups []string
	// bound 为 FromContext 保留上下文，标准无 Context 方法仍能关联身份。
	bound context.Context
}

// Handler 重复包装直接复用，避免属性重复追加。
func Handler(base slog.Handler) slog.Handler {
	if _, ok := base.(*handler); ok {
		return base
	}
	return &handler{base: base}
}

// context 将标准无 Context 方法的 Background 替换为绑定上下文。
func (h *handler) context(ctx context.Context) context.Context {
	if h.bound != nil && (ctx == nil || ctx == context.Background() || ctx == context.TODO()) {
		return h.bound
	}
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// Enabled 只委托等级判断，不解析属性或执行 LogValuer。
func (h *handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(h.context(ctx), level)
}

// Handle 构造独立记录，底层修改记录不能污染调用方或其他出口。
func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	ctx = h.context(ctx)
	return h.base.Handle(ctx, h.record(ctx, record))
}

// WithAttrs 复制派生状态；属性保留分组位置，直到输出时才合并。
func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.bindings = append(slices.Clone(h.bindings), binding{path: slices.Clone(h.groups), attrs: slices.Clone(attrs)})
	return &next
}

// WithGroup 仅改变后续属性范围，不提前提交已有字段；空组保持原 Handler。
func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.groups = append(slices.Clone(h.groups), name)
	return &next
}

// record 同层后写覆盖前写；受保护的执行身份始终追加在根层。
func (h *handler) record(ctx context.Context, record slog.Record) slog.Record {
	// attrs 按固定字段、context、调用字段依次合并，返回独立记录。
	var attrs []slog.Attr
	for _, fixed := range h.bindings {
		attrs = appendAt(attrs, fixed.path, fixed.attrs)
	}
	if h.bound != nil {
		attrs = appendAt(attrs, h.groups, Attrs(h.bound))
	}
	attrs = appendAt(attrs, h.groups, Attrs(ctx))
	// explicit 是本次调用的字段快照，优先于继承字段。
	var explicit []slog.Attr
	record.Attrs(func(attr slog.Attr) bool { explicit = append(explicit, attr); return true })
	attrs = appendAt(attrs, h.groups, explicit)
	if h.bound != nil {
		attrs = appendAt(attrs, nil, identity(h.bound))
	}
	attrs = appendAt(attrs, nil, identity(ctx))
	result := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	result.AddAttrs(attrs...)
	return result
}

// appendAt 只合并分组路径对应的字段；显式 Group 属性则整体覆盖同名 Group。
func appendAt(existing []slog.Attr, path []string, attrs []slog.Attr) []slog.Attr {
	if len(attrs) == 0 {
		return existing
	}
	if len(path) == 0 {
		return compact(append(existing, attrs...), true)
	}
	// children 保存当前路径的分组字段，根字段不能与它们合并。
	var children []slog.Attr
	for _, attr := range existing {
		if attr.Key == path[0] && attr.Value.Kind() == slog.KindGroup {
			children = slices.Clone(attr.Value.Group())
		}
	}
	children = appendAt(children, path[1:], attrs)
	return compact(append(existing, slog.Attr{Key: path[0], Value: slog.GroupValue(children...)}), true)
}

// compact 忽略空属性、展开无名组并保留同键最后一项；动态值只在实际输出时解析。
func compact(attrs []slog.Attr, resolve bool) []slog.Attr {
	// expanded 展开无名组并排除空属性，再执行后写覆盖。
	var expanded []slog.Attr
	for _, attr := range attrs {
		if resolve {
			attr.Value = attr.Value.Resolve()
		}
		if attr.Equal(slog.Attr{}) {
			continue
		}
		if attr.Value.Kind() == slog.KindGroup {
			children := compact(attr.Value.Group(), resolve)
			if len(children) == 0 {
				continue
			}
			if attr.Key == "" {
				expanded = append(expanded, children...)
				continue
			}
			attr.Value = slog.GroupValue(children...)
		}
		expanded = append(expanded, attr)
	}
	seen := make(map[string]struct{}, len(expanded))
	result := make([]slog.Attr, 0, len(expanded))
	for i := len(expanded) - 1; i >= 0; i-- {
		attr := expanded[i]
		// 未解析的无名动态组不得按空键去重，否则其不同子字段可能被提前丢弃。
		if _, ok := seen[attr.Key]; ok && (resolve || attr.Key != "") {
			continue
		}
		seen[attr.Key] = struct{}{}
		result = append(result, attr)
	}
	slices.Reverse(result)
	return result
}

// 编译期确认包装器完整实现标准 slog Handler。
var _ slog.Handler = (*handler)(nil)
