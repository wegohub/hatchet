package backend

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// TestTaskBackoffMaxSeconds 验证纳秒值不会被截断为官方拒绝的零秒。
func TestTaskBackoffMaxSeconds(t *testing.T) {
	// 合法值完整映射为官方的整秒字段，例如 10 秒仍为 10。
	for _, max := range []time.Duration{time.Second, 10 * time.Second, 24 * time.Hour} {
		request, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{BackoffMax: option.Some(max)}})
		if err != nil || request.BackoffMaxSeconds == nil || *request.BackoffMaxSeconds != int32(max/time.Second) {
			t.Fatalf("backoff max %s mapping: request=%v err=%v", max, request, err)
		}
	}
	// 10 作为 time.Duration 仅代表 10 纳秒，不能提交为 0 秒。
	for _, max := range []time.Duration{0, 10, time.Second + time.Millisecond, 24*time.Hour + time.Second} {
		_, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{BackoffMax: option.Some(max)}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("backoff max %s should fail locally: %v", max, err)
		}
	}
}
