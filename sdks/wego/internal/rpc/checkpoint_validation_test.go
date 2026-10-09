package rpc

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestCheckpointValidationBeforeLookup 拒绝不可保存的断点组合和超大 headers，再访问运行查询
func TestCheckpointValidationBeforeLookup(t *testing.T) {
	s, fixture, cancel := fixtureStream(t, "WatchHellos")
	defer cancel()
	base := model.StreamCheckpoint{Version: 4, Mode: "reliable", Method: pb.Greeter_WatchHellos_FullMethodName, RunID: uuid.NewString(), TaskID: uuid.NewString(), InputDigest: strings.Repeat("0", 64), Cursor: "cursor", OutputSeq: 1, WorkerKey: uuid.NewString(), Writer: uuid.NewString(), HeadersSeen: true}
	for _, scenario := range []struct {
		// name 按断点的结构性质描述错误，避免绑定业务输入
		name string
		// change 修改独立断点，不触发任何新的逻辑任务
		change func(*model.StreamCheckpoint)
		// code 保留协议不兼容、非法身份和资源超限的区别
		code codes.Code
	}{{"wrong_version", func(cp *model.StreamCheckpoint) { cp.Version = 3 }, codes.FailedPrecondition}, {"cursor_without_output", func(cp *model.StreamCheckpoint) { cp.OutputSeq = 0 }, codes.FailedPrecondition}, {"cursor_without_headers", func(cp *model.StreamCheckpoint) { cp.HeadersSeen = false }, codes.FailedPrecondition}, {"invalid_writer", func(cp *model.StreamCheckpoint) { cp.Writer = "bad" }, codes.InvalidArgument}, {"foreign_namespace", func(cp *model.StreamCheckpoint) { cp.Namespace = "other" }, codes.FailedPrecondition}, {"oversized_headers", func(cp *model.StreamCheckpoint) { cp.Headers = map[string][][]byte{"key": {make([]byte, 5<<20)}} }, codes.ResourceExhausted}, {"invalid_method", func(cp *model.StreamCheckpoint) { cp.Method = "not-a-method" }, codes.InvalidArgument}} {
		t.Run(scenario.name, func(t *testing.T) {
			cp := base
			scenario.change(&cp)
			if _, err := ResumeStream(context.Background(), s.engine, cp); status.Code(err) != scenario.code {
				t.Fatal(err)
			}
		})
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.calls != 0 {
		t.Fatal("invalid checkpoint submitted a task")
	}
}
