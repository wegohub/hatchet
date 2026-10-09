package stream

import (
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// logFrame 构造固定逻辑调用的有效身份，测试只改变顺序和代次
func logFrame(kind string, epoch int32, writer string, sequence uint64) *wire.LogFrame {
	return &wire.LogFrame{Version: wire.LogVersion, Kind: kind, TaskRunId: "task", Method: "/fixture/Call", Epoch: epoch,
		Writer: writer, WorkerKey: "worker-" + writer, OutputSeq: sequence, FrameId: kind + writer, InputDigest: "digest"}
}

// TestLogTakeover 验证接管前的输出保留、接管后的旧输出被忽略
func TestLogTakeover(t *testing.T) {
	state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
	frames := []*wire.LogFrame{
		logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("DATA", 0, "a", 1),
		logFrame("CLAIM", 1, "b", 0), logFrame("DATA", 0, "a", 2), logFrame("ATTEMPT_END", 0, "a", 1),
		logFrame("CLAIM", 0, "a", 0), logFrame("DATA", 1, "b", 2), logFrame("ATTEMPT_END", 1, "b", 2),
	}
	accepted := 0
	for _, frame := range frames {
		data, err := state.Apply(frame)
		if err != nil {
			t.Fatal(err)
		}
		if data {
			accepted++
		}
	}
	if accepted != 2 || state.LastOutput != 2 || state.Epoch != 1 || state.Writer != "b" || state.End == nil {
		t.Fatalf("invalid effective prefix: accepted=%d state=%+v", accepted, state)
	}
	if !state.HeadersSeen || state.Headers != nil {
		t.Fatal("empty initial headers were not retained")
	}
}

// TestLogRejectsMissingHistoryAndInvalidTerminal 防止截断历史和缺口被误报为成功
func TestLogRejectsMissingHistoryAndInvalidTerminal(t *testing.T) {
	cases := []struct {
		// name 描述协议结构，frames 为按持久顺序读取的帧
		name string
		// frames 按持久日志顺序提供，最后一帧必须被拒绝
		frames []*wire.LogFrame
	}{
		{"initial_claim_missing", []*wire.LogFrame{logFrame("CLAIM", 1, "a", 0)}},
		{"data_before_claim", []*wire.LogFrame{logFrame("DATA", 0, "a", 1)}},
		{"data_before_headers", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("DATA", 0, "a", 1)}},
		{"end_before_headers", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("ATTEMPT_END", 0, "a", 0)}},
		{"output_gap", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("DATA", 0, "a", 2)}},
		{"end_gap", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("ATTEMPT_END", 0, "a", 1)}},
		{"claim_gap", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("DATA", 1, "b", 1)}},
		{"data_after_end", []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("ATTEMPT_END", 0, "a", 0), logFrame("DATA", 0, "a", 1)}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
			for i, frame := range test.frames {
				before := state.Snapshot()
				_, err := state.Apply(frame)
				if i == len(test.frames)-1 {
					if status.Code(err) != codes.DataLoss {
						t.Fatalf("expected DataLoss, got %v", err)
					}
					if !reflect.DeepEqual(before, state) {
						t.Fatal("invalid frame changed the effective prefix")
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestCanonicalClaimAndSnapshot 验证同代次竞争只接受首个 writer，快照不共享可变 metadata
func TestCanonicalClaimAndSnapshot(t *testing.T) {
	state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
	for _, frame := range []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("CLAIM", 0, "b", 0), logFrame("DATA", 0, "b", 1)} {
		if _, err := state.Apply(frame); err != nil {
			t.Fatal(err)
		}
	}
	if state.Writer != "a" || state.LastOutput != 0 {
		t.Fatal("loser changed effective writer/output")
	}
	header := logFrame("HEADERS", 0, "a", 0)
	header.Metadata = map[string]*wire.Values{"key-bin": {Values: [][]byte{{0xff}}}}
	if _, err := state.Apply(header); err != nil {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	snapshot.Headers["key-bin"].Values[0][0] = 0
	if state.Headers["key-bin"].Values[0][0] != 0xff {
		t.Fatal("snapshot aliases live metadata")
	}
}

// FuzzLogInterpreter 验证不可信帧不使代次/输出倒退，拒绝帧不改变已确认前缀
func FuzzLogInterpreter(f *testing.F) {
	f.Add("DATA", int32(0), uint64(1), "a")
	f.Add("CLAIM", int32(1), uint64(0), "b")
	f.Add("ATTEMPT_END", int32(0), uint64(0), "a")
	f.Fuzz(func(t *testing.T, kind string, epoch int32, sequence uint64, writer string) {
		if len(kind) > 4096 || len(writer) > 4096 {
			return
		}
		state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
		if _, err := state.Apply(logFrame("CLAIM", 0, "a", 0)); err != nil {
			t.Fatal(err)
		}
		if _, err := state.Apply(logFrame("HEADERS", 0, "a", 0)); err != nil {
			t.Fatal(err)
		}
		before := state.Snapshot()
		accepted, err := state.Apply(logFrame(kind, epoch, writer, sequence))
		if err != nil && !reflect.DeepEqual(before, state) {
			t.Fatal("rejected frame modified durable prefix")
		}
		if state.Epoch < before.Epoch || state.LastOutput < before.LastOutput {
			t.Fatal("effective prefix moved backwards")
		}
		if accepted && state.LastOutput != before.LastOutput+1 {
			t.Fatal("accepted output has a gap")
		}
	})
}
