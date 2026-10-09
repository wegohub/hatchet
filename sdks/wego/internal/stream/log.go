// Package stream 实现持久输出的统一解释规则；业务交付和 Worker 恢复共用同一状态机
package stream

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// State 是一个已经解释的有效日志前缀，不包含预取后尚未交付的消费位置
type State struct {
	// TaskID、Method、InputDigest 约束日志所属的逻辑调用
	TaskID, Method, InputDigest string
	// RunID 在恢复已知运行时核对稳定身份；原型未指定时从首个 CLAIM 获取
	RunID string
	// Claimed 表示已看到 epoch=0 的可验证初始 CLAIM
	Claimed bool
	// Epoch 和 Writer 为当前有效发布者；更高 CLAIM 只能向前切换
	Epoch int32
	// Writer 为当前代次获胜的实际执行 nonce
	Writer string
	// WorkerKey 为当前发布者的定向通知地址
	WorkerKey string
	// LastOutput 跨代次累计有效业务输出序号
	LastOutput uint64
	// Headers 保存首次有效响应头，重试不能覆盖
	Headers map[string]*wire.Values
	// HeadersSeen 区分已交付空响应头与尚未出现响应头
	HeadersSeen bool
	// End 是当前代次的结束帧，不等于引擎终态
	End *wire.LogFrame
}

// Apply 按持久顺序解释一帧；只有有效 DATA 使 LastOutput 前进
// CLAIM(0), DATA(1), CLAIM(1), 旧 DATA(2), 新 DATA(2) 最终形成两条输出
func (s *State) Apply(frame *wire.LogFrame) (bool, error) {
	if diagnostic := s.validateIdentity(frame); diagnostic != "" {
		return false, status.Error(codes.DataLoss, diagnostic)
	}
	if frame.Kind == "CLAIM" {
		return false, s.applyClaim(frame)
	}
	effective, diagnostic := s.effectiveWriter(frame)
	if diagnostic != "" {
		return false, status.Error(codes.DataLoss, diagnostic)
	}
	if !effective {
		return false, nil
	}
	switch frame.Kind {
	case "HEADERS":
		if !s.HeadersSeen {
			s.Headers = cloneMetadata(frame.Metadata)
			s.HeadersSeen = true
		}
	case "DATA":
		if diagnostic := s.validateData(frame); diagnostic != "" {
			return false, status.Error(codes.DataLoss, diagnostic)
		}
		s.LastOutput = frame.OutputSeq
		return true, nil
	case "ATTEMPT_END":
		if !s.HeadersSeen {
			return false, status.Error(codes.DataLoss, "wego: attempt end precedes response headers")
		}
		if frame.OutputSeq != s.LastOutput || s.End != nil {
			return false, status.Error(codes.DataLoss, "wego: attempt end does not match effective output prefix")
		}
		s.End = proto.Clone(frame).(*wire.LogFrame)
	default:
		return false, status.Error(codes.DataLoss, "wego: unknown RPC log frame kind")
	}
	return false, nil
}

// validateData 不修改前缀，调用者确认无错误后才推进输出序号；快速校验可被内联
func (s *State) validateData(frame *wire.LogFrame) string {
	if !s.HeadersSeen {
		return "wego: output precedes response headers"
	}
	if s.End != nil || s.LastOutput == ^uint64(0) || frame.OutputSeq != s.LastOutput+1 {
		return "wego: noncontiguous output or DATA after attempt end"
	}
	return ""
}

// validateIdentity 返回固定诊断，不在有效帧路径构造错误对象，便于编译器内联快速检查
// 在任何状态修改前拒绝空帧、错误版本及其他逻辑调用
func (s *State) validateIdentity(frame *wire.LogFrame) string {
	if frame == nil || frame.Version != wire.LogVersion || frame.Epoch < 0 || frame.FrameId == "" {
		return "wego: invalid log frame identity"
	}
	if frame.TaskRunId != s.TaskID || frame.Method != s.Method || s.RunID != "" && frame.RunId != s.RunID {
		return "wego: log task/method identity mismatch"
	}
	return ""
}

// applyClaim 只接受起始代次或更高代次；同代次成功 ACK 不会替换规范 writer
func (s *State) applyClaim(frame *wire.LogFrame) error {
	if frame.Writer == "" || frame.WorkerKey == "" || frame.InputDigest != s.InputDigest {
		return status.Error(codes.DataLoss, "wego: invalid CLAIM writer or input identity")
	}
	if !s.Claimed && frame.Epoch != 0 {
		return status.Error(codes.DataLoss, "wego: initial CLAIM is unavailable; history cannot be proved complete")
	}
	if s.Claimed && frame.Epoch <= s.Epoch {
		return nil
	}
	s.Claimed, s.Epoch, s.Writer, s.WorkerKey = true, frame.Epoch, frame.Writer, frame.WorkerKey
	if s.RunID == "" {
		s.RunID = frame.RunId
	}
	s.End = nil
	return nil
}

// effectiveWriter 在分派具体帧前过滤晚到的旧执行，并核对获胜执行的完整身份
func (s *State) effectiveWriter(frame *wire.LogFrame) (bool, string) {
	if !s.Claimed {
		return false, "wego: output precedes initial CLAIM"
	}
	if frame.Epoch < s.Epoch || frame.Epoch == s.Epoch && frame.Writer != s.Writer {
		return false, ""
	}
	if frame.Epoch > s.Epoch {
		return false, "wego: output has no matching CLAIM"
	}
	if frame.InputDigest != s.InputDigest || frame.WorkerKey != s.WorkerKey {
		return false, "wego: effective output identity mismatch"
	}
	return true, ""
}

// Snapshot 深复制 headers 和结束清单，调用方修改快照不能改变解释器
func (s *State) Snapshot() State {
	// snapshot 先复制标量，再复制可变字段；创建快照不修改解释器本身。
	// 例如快照修改 headers 或结束帧，不会影响下一次 Apply 的有效前缀。
	snapshot := *s
	snapshot.Headers = cloneMetadata(s.Headers)
	if s.End != nil {
		snapshot.End = proto.Clone(s.End).(*wire.LogFrame)
	}
	return snapshot
}

// cloneMetadata 保留 nil 与空响应头的区别，逐项复制二进制值
func cloneMetadata(source map[string]*wire.Values) map[string]*wire.Values {
	if source == nil {
		return nil
	}
	result := make(map[string]*wire.Values, len(source))
	for key, value := range source {
		if value != nil {
			result[key] = proto.Clone(value).(*wire.Values)
		}
	}
	return result
}
