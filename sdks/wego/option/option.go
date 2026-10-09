package option

// Option 区分未设置和显式零值，Set 为 true 时 Value 才参与策略覆盖
type Option[T any] struct {
	// Value 配置或标签比较使用的值；有效性由所属类型及 Set 决定
	Value T
	// Set 是否显式提供配置；Some(0) 的 Set=true，与默认零值 Option 不同
	Set bool
}

// Some 构造显式设置的 Option；例如 Some(0) 仍应覆盖默认值
func Some[T any](value T) Option[T] {
	return Option[T]{Value: value, Set: true}
}
