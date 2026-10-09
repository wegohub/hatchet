package stream

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// TestCompletionRequiresAuthorityIdentityAndDelivery 不完整输出、缺结束帧或旧 attempt 均不能变成成功 EOF
func TestCompletionRequiresAuthorityIdentityAndDelivery(t *testing.T) {
	state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
	for _, frame := range []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("DATA", 0, "a", 1), logFrame("ATTEMPT_END", 0, "a", 1)} {
		if _, err := state.Apply(frame); err != nil {
			t.Fatal(err)
		}
	}
	final := Completion{Version: wire.LogVersion, TaskID: "task", Epoch: 0, Writer: "a", LastOutput: 1, EndID: state.End.FrameId}
	if err := VerifyCompletion(state, 1, final); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"prefetch_not_delivered", "old_epoch", "different_writer", "missing_end", "wrong_end", "missing_output"} {
		t.Run(scenario, func(t *testing.T) {
			copy, result, delivered := state.Snapshot(), final, uint64(1)
			switch scenario {
			case "prefetch_not_delivered":
				delivered = 0
			case "old_epoch":
				result.Epoch = 1
			case "different_writer":
				result.Writer = "b"
			case "missing_end":
				copy.End = nil
			case "wrong_end":
				result.EndID = "another-end"
			case "missing_output":
				result.LastOutput = 2
			}
			if err := VerifyCompletion(copy, delivered, result); status.Code(err) != codes.DataLoss {
				t.Fatal(err)
			}
		})
	}
}
