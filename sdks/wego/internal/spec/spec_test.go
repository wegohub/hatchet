package spec

import (
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// TestMergeExplicitZeroAndEmpty 用显式 0 和空列表覆盖非空默认配置，断言 Option.Set 与清空语义没有丢失
func TestMergeExplicitZeroAndEmpty(t *testing.T) {
	// base 保存默认重试与 Cron；显式 0 次重试必须覆盖默认 3 次
	base := Task{Retries: option.Some(3), Cron: []string{"* * * * *"}}
	// merged 合并默认与方法级策略；未设置字段继承，显式 nil 可以清除 CronInput
	merged := Merge(base, Task{Retries: option.Some(0), Cron: []string{}})
	if !merged.Retries.Set || merged.Retries.Value != 0 || len(merged.Cron) != 0 {
		t.Fatal(merged)
	}
}
