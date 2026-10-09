package backend

import (
	"fmt"
	"reflect"
)

// batchResults 把强类型 map 或广播结果转换为执行器要求的逐成员映射
// 例如输入 {run1:A,run2:B} 必须返回恰好 run1、run2 两个结果，不能漏报或混入第三个运行
func batchResults(input, output any, broadcast bool) (map[string]any, error) {
	// members 是已解码的批次身份，不使用工作流名称代替逐成员 RunID
	members, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("wego: batch input must be a member map")
	}
	// results 始终是 map[string]any，成员值保持原类型供执行器分别编码
	results := make(map[string]any, len(members))
	if broadcast {
		// 广播给同一批次的每个成员；如总数 2，两个调用都收到同一个汇总结果
		for id := range members {
			results[id] = output
		}
		return results, nil
	}
	// values 允许 map[string]业务结构体，不能依赖动态类型恰好为 map[string]any
	values := reflect.ValueOf(output)
	if !values.IsValid() || values.Kind() != reflect.Map || values.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("wego: per-member batch output must be a string-keyed map")
	}
	// id 沿用真实成员键，转换 map 容器时不执行 JSON roundtrip 改写业务值
	for _, id := range values.MapKeys() {
		results[id.String()] = values.MapIndex(id).Interface()
	}
	if len(results) != len(members) {
		return nil, fmt.Errorf("wego: batch returned %d results for %d members", len(results), len(members))
	}
	// 校验身份集合相等；数量相同也不能把 run1 的响应发送给 run3
	for id := range members {
		// _, ok 取得 results[id] 的结果，确认成功后才进入下一处理阶段
		if _, ok := results[id]; !ok {
			return nil, fmt.Errorf("wego: batch output missing member %s", id)
		}
	}
	return results, nil
}
