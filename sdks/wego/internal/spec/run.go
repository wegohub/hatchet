package spec

import (
	"fmt"
	"maps"
	"math"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// RunOption 按传入顺序配置单次提交；不得改变实例默认值。
type RunOption func(*model.RunOptions)

// CloneRunOptions 复制可修改的容器和指针，派生调用不能改变另一个请求。
func CloneRunOptions(in model.RunOptions) model.RunOptions {
	out := in
	out.AdditionalMetadata = maps.Clone(in.AdditionalMetadata)
	out.Routing, out.ValidationError = SnapshotRouting(in.Routing)
	if in.ValidationError != nil {
		out.ValidationError = in.ValidationError
	}
	if in.Key != nil {
		value := *in.Key
		out.Key = &value
	}
	if in.Sticky != nil {
		value := *in.Sticky
		out.Sticky = &value
	}
	if in.Priority != nil {
		value := *in.Priority
		out.Priority = &value
	}
	if in.DesiredWorkerLabels != nil {
		out.DesiredWorkerLabels = make(map[string]*model.DesiredWorkerLabel, len(in.DesiredWorkerLabels))
		for key, label := range in.DesiredWorkerLabels {
			if label == nil {
				out.DesiredWorkerLabels[key] = nil
				continue
			}
			value := *label
			out.DesiredWorkerLabels[key] = &value
		}
	}
	return out
}

// ApplyRunOptions 冻结最终配置并在任何 codec I/O 前校验。
func ApplyRunOptions(options ...RunOption) (model.RunOptions, error) {
	out := model.RunOptions{}
	for _, option := range options {
		if option == nil {
			return out, fmt.Errorf("wego: nil RunOption")
		}
		option(&out)
	}
	out = CloneRunOptions(out)
	if out.ValidationError != nil {
		return out, out.ValidationError
	}
	if out.Priority != nil && (*out.Priority < 1 || *out.Priority > 3) {
		return out, fmt.Errorf("wego: run priority must be between 1 and 3")
	}
	if err := validateDesiredLabels(out.DesiredWorkerLabels); err != nil {
		return out, err
	}
	return out, nil
}

// validateDesiredLabels 拒绝服务端协议无法表示的比较值，在任何提交或 codec I/O 前报错。
func validateDesiredLabels(labels map[string]*model.DesiredWorkerLabel) error {
	for key, label := range labels {
		if key == "" || label == nil {
			return fmt.Errorf("wego: worker label requires a nonempty key and value")
		}
		// 官方标签协议仅支持字符串或 int32 范围整数，容器没有合法调度语义。
		switch value := label.Value.(type) {
		case string, int32:
		case int:
			if int64(value) < math.MinInt32 || int64(value) > math.MaxInt32 {
				return fmt.Errorf("wego: affinity integer exceeds int32 range")
			}
		case int64:
			if value < math.MinInt32 || value > math.MaxInt32 {
				return fmt.Errorf("wego: affinity integer exceeds int32 range")
			}
		default:
			return fmt.Errorf("wego: unsupported affinity value type %T", label.Value)
		}
		switch label.Comparator {
		case model.LabelEqual, model.LabelNotEqual, model.LabelGreater, model.LabelGreaterEqual, model.LabelLess, model.LabelLessEqual:
		default:
			return fmt.Errorf("wego: unsupported worker label comparator")
		}
	}
	return nil
}

// WithRouting 在构造时冻结 JSON map，后续调用按顶层键覆盖已有值。
func WithRouting(values map[string]any) RunOption {
	snapshot, err := SnapshotRouting(values)
	return func(o *model.RunOptions) {
		if err != nil {
			o.ValidationError = err
			return
		}
		if o.Routing == nil {
			o.Routing = map[string]any{}
		}
		copy, _ := SnapshotRouting(snapshot)
		for key, value := range copy {
			o.Routing[key] = value
		}
	}
}

// WithStreamIdempotencyKey 设置一次流调用的稳定提交键，不适用于普通任务。
func WithStreamIdempotencyKey(key string) RunOption {
	return func(o *model.RunOptions) { o.StreamIdempotencyKey = key }
}
