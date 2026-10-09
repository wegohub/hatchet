package spec

import (
	"testing"
	"time"
)

// TestLifecycleBudgetsValidate 校验新增轮询预算的默认、零值兼容与非法负值
func TestLifecycleBudgetsValidate(t *testing.T) {
	// config 是所有默认值的实际入口，不能在测试中重复写实现常量构造默认配置
	config := Defaults()
	if config.ResultPollInterval != time.Second {
		t.Fatal("unexpected default budgets")
	}
	config.ResultPollInterval = 0
	// err 验证应用省略新增字段时仍能使用默认策略
	if err := config.Validate(); err != nil {
		t.Fatal("zero budgets must use defaults:", err)
	}
	config.ResultPollInterval = -time.Second
	// err 表示负间隔不能退化为极密集的轮询
	if err := config.Validate(); err == nil {
		t.Fatal("negative polling interval accepted")
	}
}
