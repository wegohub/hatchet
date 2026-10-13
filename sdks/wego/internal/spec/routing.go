package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
)

// SnapshotRouting 拒绝函数、循环引用、NaN 等非 JSON 值，并复制所有嵌套容器
// UseNumber 校验原始整数文本；超出 9007199254740991 的整数必须改用字符串
func SnapshotRouting(values map[string]any) (map[string]any, error) {
	if values == nil {
		return nil, nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("wego: routing requires JSON values: %w", err)
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("wego: routing exceeds 1 MiB")
	}
	// out 只在完整 JSON 校验后交付，失败不保存半个配置
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	if _, err := normalizeRoutingNumbers(out); err != nil {
		return nil, err
	}
	return out, nil
}

// cloneRouting 复制已经校验的 JSON 快照；能力对象不允许进入 routing
func cloneRouting(values map[string]any) map[string]any {
	copy, _ := SnapshotRouting(values)
	return copy
}

// SetRoutingDefaults 保存方法默认值；无效选项在实例校验时返回原始诊断
func (r *Runtime) SetRoutingDefaults(method string, values map[string]any, err error) {
	if err != nil {
		r.routingError = err
		return
	}
	if r.RoutingDefaults == nil {
		r.RoutingDefaults = map[string]map[string]any{}
	}
	r.RoutingDefaults[method] = cloneRouting(values)
}

// Routing 合并默认值与调用值；嵌套对象整体替换，例如 override.policy 取代整个默认 policy
func (r *Runtime) Routing(method string, override map[string]any) (map[string]any, error) {
	if r.routingError != nil {
		return nil, r.routingError
	}
	out, err := SnapshotRouting(r.RoutingDefaults[method])
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range override {
		out[key] = value
	}
	return SnapshotRouting(out)
}

// normalizeRoutingNumbers 先检查整数精度，再使用服务端 float64 的规范 JSON 文本。
// 例如 1e3、1000.0 均保存为 1000，避免引擎重编码后与 codec 内 routing 快照不一致。
func normalizeRoutingNumbers(value any) (any, error) {
	switch v := value.(type) {
	case json.Number:
		// float 限制指数范围，不能为校验 1e999999999 分配巨大有理数。
		float, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, fmt.Errorf("wego: routing number exceeds JSON float64 range")
		}
		number, ok := new(big.Rat).SetString(string(v))
		if !ok {
			return nil, fmt.Errorf("wego: invalid routing number")
		}
		if number.IsInt() && new(big.Int).Abs(number.Num()).Cmp(big.NewInt(9007199254740991)) > 0 {
			return nil, fmt.Errorf("wego: routing integer exceeds JSON safe range; use a string")
		}
		encoded, err := json.Marshal(float)
		if err != nil {
			return nil, err
		}
		return json.Number(encoded), nil
	case map[string]any:
		for key, item := range v {
			normalized, err := normalizeRoutingNumbers(item)
			if err != nil {
				return nil, err
			}
			v[key] = normalized
		}
	case []any:
		for index, item := range v {
			normalized, err := normalizeRoutingNumbers(item)
			if err != nil {
				return nil, err
			}
			v[index] = normalized
		}
	}
	return value, nil
}
