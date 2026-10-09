package binding

import (
	"strings"
	"testing"
)

// TestRPCNamesAndCaseIsolation 验证空 namespace、展示大小写和 action 的实例无关性
func TestRPCNamesAndCaseIsolation(t *testing.T) {
	if WorkflowName("", "/demo.Greeter/SayHello") != "demo.Greeter-SayHello" || WorkflowName("demo", "/demo.Greeter/SayHello") != "demo-demo.Greeter-SayHello" {
		t.Fatal("unexpected RPC workflow name")
	}
	// 方法大小写不同必须得到不同 action；固定完整摘要不依赖 Worker 副本或名称
	first, second := Action("demo", "/demo.Greeter/SayHello"), Action("demo", "/demo.Greeter/Sayhello")
	if first == second || len(first) != len("rpc_")+64+len(":invoke") || first != strings.ToLower(first) || first == Action("other", "/demo.Greeter/SayHello") {
		t.Fatalf("action collision: %s / %s", first, second)
	}
	for _, method := range []string{"demo/Call", "/demo/", "/demo/Call/Extra", "/demo/Bad-Name"} {
		if ValidateName("", method) == nil {
			t.Fatalf("invalid method accepted: %q", method)
		}
	}
	if ValidateName(strings.Repeat("a", 250), "/demo/Call") == nil {
		t.Fatal("name silently truncated")
	}
}
