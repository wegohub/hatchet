package engine

import (
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// TestFrameObserverFollowsInstanceMetrics 验证关闭指标的实例不把计时 observer 传入热路径
// 相邻实例开启指标不改变默认实例的配置或出口
func TestFrameObserverFollowsInstanceMetrics(t *testing.T) {
	defaults := spec.Defaults()
	plain := &Engine{Config: defaults}
	enabledConfig := defaults
	enabledConfig.Telemetry.Metrics.Enabled = true
	enabled := &Engine{Config: enabledConfig}
	if plain.FrameObserver() != nil || enabled.FrameObserver() != enabled || plain.Config.Telemetry.Metrics.Enabled {
		t.Fatal("codec observer escaped instance metrics configuration")
	}
}
