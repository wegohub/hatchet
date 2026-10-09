package backend

import (
	"context"
	"fmt"
	"maps"
	"math"
	"sync"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/client/types"
	"go.opentelemetry.io/otel/propagation"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// traceMetadata 将当前实例 trace 注入调度元数据，复制调用者 map 后再写入
func traceMetadata(ctx context.Context, values map[string]string) map[string]string {
	values = maps.Clone(values)
	if values == nil {
		values = map[string]string{}
	}
	// carrier 取得 propagation.MapCarrier{} 的结果，确认成功后才进入下一处理阶段
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for key, value := range carrier {
		values["wego."+key] = value
	}
	return values
}

// runOptions 转换本次运行的键、优先级、metadata 与亲和性选项
func runOptions(v model.RunOptions) ([]v0.RunOptFunc, error) {
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out []v0.RunOptFunc
	if v.Priority != nil {
		out = append(out, v0.WithPriority(int32(*v.Priority)))
	}
	if v.Metadata != nil {
		out = append(out, v0.WithRunMetadata(v.Metadata))
	}
	if v.Labels != nil {
		// labels 后端亲和性条件集合，例如 owner 的 Required=true 不允许回退到另一实例
		labels := map[string]*types.DesiredWorkerLabel{}
		// key 是调度标签名，label 的 Value 必须保持 string 或 int32 类型
		for key, label := range v.Labels {
			if label == nil {
				return nil, fmt.Errorf("wego: nil affinity label %s", key)
			}
			// value 验证数值范围，禁止 JSON roundtrip 把整数变成 float64
			value, err := affinityValue(label.Value)
			if err != nil {
				return nil, fmt.Errorf("wego: label %s: %w", key, err)
			}
			// converted 拷贝必需性、权重和比较器，避免引用调用方可变指针
			converted := &types.DesiredWorkerLabel{Value: value, Required: label.Required, Weight: label.Weight}
			if label.Comparator != nil {
				// comparator 与 wego 枚举保持同一数值语义
				comparator := types.WorkerLabelComparator(*label.Comparator)
				converted.Comparator = &comparator
			}
			labels[key] = converted
		}
		out = append(out, v0.WithDesiredWorkerLabels(labels))
	}
	return out, nil
}

// affinityValue 只接受协议支持的字符串和有符号整数，例如 capacity=8，拒绝 8.5 及溢出值
func affinityValue(value any) (any, error) {
	// integer 将整数统一到后端 int32；调用方整数类型不影响匹配结果
	var integer int64
	// v 是原始动态类型，不能通过 JSON 推断数值语义
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		integer = int64(v)
	case int32:
		return v, nil
	case int64:
		integer = v
	default:
		return nil, fmt.Errorf("unsupported affinity value type %T", value)
	}
	if integer < math.MinInt32 || integer > math.MaxInt32 {
		return nil, fmt.Errorf("affinity integer exceeds int32 range")
	}
	return int32(integer), nil
}

// submissionGate 串行化父执行的提交确认；排队者可取消而不影响持有者
type submissionGate struct {
	// once 只初始化一份 token 通道，允许 execution 的零值直接使用
	once sync.Once
	// token 容量为一，持有期间占用 token，不能同时修改 child 序号
	token chan struct{}
}

// Lock 等待提交权，预算结束时不消耗下一个稳定 child 序号
func (g *submissionGate) Lock(ctx context.Context) error {
	// err 优先拒绝已经取消的调用，不能随机获取空闲 token
	if err := ctx.Err(); err != nil {
		return err
	}
	g.once.Do(func() { g.token = make(chan struct{}, 1) })
	select {
	case g.token <- struct{}{}:
		// 取消和获取 token 同时就绪时，不让已取消的请求占用 child 序号
		if err := ctx.Err(); err != nil {
			g.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock 在真正提交确认后归还提交权，业务结果等待不持有此 token
func (g *submissionGate) Unlock() { <-g.token }
