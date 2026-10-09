package worker

import "testing"

// TestDisableMethodOptionKeepsConfigsIndependent 验证零值配置、重复应用及复用同一个 Option 的隔离。
func TestDisableMethodOptionKeepsConfigsIndependent(t *testing.T) {
	option := WithDisableMethod("/demo.Greeter/SayHello", "/demo.Greeter/UploadHellos", "/demo.Greeter/SayHello")
	// first、second 各自从零值配置应用同一个 Option，禁用集合不能共享可变存储。
	var first, second Config
	option(&first)
	option(&first)
	option(&second)
	if len(first.DisabledMethods) != 2 || len(second.DisabledMethods) != 2 {
		t.Fatal("repeated disable must be idempotent")
	}
	// 第一个实例追加 WatchHellos，不能把第二个实例的配置也改成禁用三个方法。
	WithDisableMethod("/demo.Greeter/WatchHellos")(&first)
	if _, disabled := second.DisabledMethods["/demo.Greeter/WatchHellos"]; disabled {
		t.Fatal("reused option shares mutable method set")
	}
}

// TestDisableMethodSnapshotsArguments 验证 names... 的源切片修改不会改变已创建 Option 的选择。
func TestDisableMethodSnapshotsArguments(t *testing.T) {
	names := []string{"/demo.Greeter/SayHello", "/demo.Greeter/WatchHellos"}
	option := WithDisableMethod(names...)
	names[0] = "/demo.Greeter/UploadHellos"
	// config 应包含构造时的 SayHello 和 WatchHellos，不应读取后来替换的 UploadHellos。
	var config Config
	option(&config)
	for _, name := range []string{"/demo.Greeter/SayHello", "/demo.Greeter/WatchHellos"} {
		if _, disabled := config.DisabledMethods[name]; !disabled {
			t.Errorf("method snapshot lost %s", name)
		}
	}
	if _, disabled := config.DisabledMethods[names[0]]; disabled {
		t.Fatal("option retained caller's mutable slice")
	}
}

// TestDisableMethodWithNoArgumentsIsNoOp 验证空参数不分配禁用集合，也不清除已有选择。
func TestDisableMethodWithNoArgumentsIsNoOp(t *testing.T) {
	// config 先从零值应用空选项，再保留 SayHello 的已有禁用配置。
	var config Config
	WithDisableMethod()(&config)
	if config.DisabledMethods != nil {
		t.Fatal("empty option changed zero config")
	}
	WithDisableMethod("/demo.Greeter/SayHello")(&config)
	WithDisableMethod()(&config)
	if _, disabled := config.DisabledMethods["/demo.Greeter/SayHello"]; !disabled || len(config.DisabledMethods) != 1 {
		t.Fatal("empty option cleared configured methods")
	}
}
