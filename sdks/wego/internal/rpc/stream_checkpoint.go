package rpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// checkpoint 复制已交付位置，不将当前预取解释器的高代次或尾部写入恢复位置
func (s *taskClientStream) checkpoint() (model.StreamCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.desc.ServerStreams || s.options.Mode != spec.Reliable {
		return model.StreamCheckpoint{}, status.Error(codes.FailedPrecondition, "wego: realtime or client-input stream cannot resume outputs")
	}
	if s.runID == "" || s.state.TaskID == "" {
		return model.StreamCheckpoint{}, status.Error(codes.FailedPrecondition, "wego: stream run identity is not yet available")
	}
	state := s.deliveredState
	cp := model.StreamCheckpoint{Version: wire.LogVersion, Namespace: s.engine.Config.Namespace, TenantID: s.engine.Config.TenantID, RunID: s.runID, TaskID: s.state.TaskID, Method: s.method, Mode: "reliable", InputDigest: s.state.InputDigest, Cursor: s.deliveredCursor, OutputSeq: s.delivered, Epoch: state.Epoch, Writer: state.Writer, WorkerKey: state.WorkerKey, HeadersSeen: s.headersDelivered}
	if cp.HeadersSeen {
		cp.Headers = checkpointMetadata(wire.Metadata(s.headers))
	}
	return cp, nil
}

// checkpointMetadata 保存独立 bytes，不暴露状态机的共享 headers map
func checkpointMetadata(md map[string]*wire.Values) map[string][][]byte {
	if md == nil {
		return nil
	}
	out := make(map[string][][]byte, len(md))
	for key, values := range md {
		if values != nil {
			for _, value := range values.Values {
				out[key] = append(out[key], append([]byte(nil), value...))
			}
		}
	}
	return out
}

// checkpointHeaders 恢复独立多值 metadata，与原始 checkpoint 解除可变所有权
func checkpointHeaders(md map[string][][]byte) map[string]*wire.Values {
	if md == nil {
		return nil
	}
	out := make(map[string]*wire.Values, len(md))
	for key, values := range md {
		entry := &wire.Values{}
		for _, value := range values {
			entry.Values = append(entry.Values, append([]byte(nil), value...))
		}
		out[key] = entry
	}
	return out
}

// ResumeStream 只恢复已有可靠运行的消费，输入方向已经关闭，不能追加请求
func ResumeStream(ctx context.Context, e *engine.Engine, cp model.StreamCheckpoint) (grpc.ClientStream, error) {
	if cp.Version != wire.LogVersion || cp.Mode != "reliable" || cp.RunID == "" || cp.TaskID == "" || cp.InputDigest == "" || cp.Namespace != e.Config.Namespace || cp.TenantID != e.Config.TenantID || cp.Epoch < 0 || cp.Cursor == "" && (cp.OutputSeq != 0 || cp.Epoch != 0 || cp.Writer != "" || cp.WorkerKey != "") || cp.Cursor != "" && (cp.OutputSeq == 0 || !cp.HeadersSeen || cp.Writer == "" || cp.WorkerKey == "") {
		return nil, status.Error(codes.FailedPrecondition, "wego: incompatible stream checkpoint identity")
	}
	if state, ok := callctx.Get(ctx); ok && state.Execution != nil && state.Execution.Info().Durable {
		return nil, status.Error(codes.FailedPrecondition, "wego: streams cannot participate in durable replay")
	}
	if uuid.Validate(cp.RunID) != nil || uuid.Validate(cp.TaskID) != nil || cp.Cursor != "" && (uuid.Validate(cp.Writer) != nil || uuid.Validate(cp.WorkerKey) != nil) {
		return nil, status.Error(codes.InvalidArgument, "wego: malformed checkpoint execution identity")
	}
	if digest, err := hex.DecodeString(cp.InputDigest); err != nil || len(digest) != 32 {
		return nil, status.Error(codes.InvalidArgument, "wego: malformed checkpoint input digest")
	}
	if len(cp.Cursor) > 4096 {
		return nil, status.Error(codes.ResourceExhausted, "wego: checkpoint cursor exceeds limit")
	}
	// 先按原始 map 大小计费，不能为检查超限而先复制全部 checkpoint 字节
	headerBytes := 0
	for key, values := range cp.Headers {
		if len(key) > 256 {
			return nil, status.Error(codes.ResourceExhausted, "wego: checkpoint header key exceeds limit")
		}
		headerBytes += len(key) + len(values)*4
		for _, value := range values {
			headerBytes += len(value)
			if headerBytes > e.Config.StreamOptionsFor(cp.Method).MaxFrameBytes {
				return nil, status.Error(codes.ResourceExhausted, "wego: checkpoint headers exceed frame budget")
			}
		}
	}
	if headerBytes > e.Config.StreamOptionsFor(cp.Method).MaxFrameBytes || proto.Size(&wire.LogFrame{Metadata: checkpointHeaders(cp.Headers)}) > e.Config.StreamOptionsFor(cp.Method).MaxFrameBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: checkpoint headers exceed frame budget")
	}
	parts := strings.Split(cp.Method, "/")
	if len(parts) != 3 || parts[0] != "" {
		return nil, status.Error(codes.InvalidArgument, "wego: invalid checkpoint method")
	}
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[1]))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "wego: checkpoint protobuf service unavailable")
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "wego: checkpoint service is invalid")
	}
	method := service.Methods().ByName(protoreflect.Name(parts[2]))
	if method == nil || !method.IsStreamingServer() || e.Config.StreamOptionsFor(cp.Method).Mode != spec.Reliable {
		return nil, status.Error(codes.FailedPrecondition, "wego: method does not support reliable output recovery")
	}
	reader, ok := e.Backend.(ports.RunInputReader)
	lookup, lookupOK := e.Backend.(ports.RunLookup)
	if !ok || !lookupOK {
		return nil, status.Error(codes.Unimplemented, "wego: run recovery unavailable")
	}
	ctx, done, err := e.BeginIO(ctx, "rpc.resume")
	if err != nil {
		return nil, err
	}
	ctx, finish := e.StartSpan(ctx, cp.Method, trace.SpanKindClient)
	// once 对启动失败和正常资源释放使用同一个回调
	var once sync.Once
	completed := func(err error) { once.Do(func() { finish(err); done() }) }
	input, err := reader.RunInput(ctx, cp.RunID)
	if err != nil {
		completed(err)
		return nil, err
	}
	if err := stream.VerifyRunIdentity(input, cp.Method, cp.InputDigest); err != nil {
		completed(err)
		return nil, err
	}
	// envelope 另外核对输出模式，避免用同方法名称恢复错误的调用种类
	var envelope wire.Envelope
	if err := json.Unmarshal(input, &envelope); err != nil || envelope.StreamMode != "reliable" || envelope.Stream == "client" || envelope.Stream == "" {
		err := status.Error(codes.FailedPrecondition, "wego: persisted run is not a reliable output stream")
		completed(err)
		return nil, err
	}
	taskID, ref, err := lookup.LookupRun(ctx, cp.RunID)
	if err != nil {
		completed(err)
		return nil, err
	}
	if taskID != cp.TaskID {
		err := status.Error(codes.DataLoss, "wego: checkpoint task identity mismatch")
		completed(err)
		return nil, err
	}
	desc := &grpc.StreamDesc{ClientStreams: method.IsStreamingClient(), ServerStreams: true}
	raw, err := newTaskClient(ctx, e, desc, cp.Method, completed)
	if err != nil {
		completed(err)
		return nil, err
	}
	s := raw.(*taskClientStream)
	// 在与取消同步的提交锁内安装观察者，禁止等待资源释放时新增 goroutine
	s.sendMu.Lock()
	if err := s.ctx.Err(); err != nil {
		s.sendMu.Unlock()
		return nil, status.FromContextError(err).Err()
	}
	s.submitted = true
	s.mu.Lock()
	s.runID = cp.RunID
	s.state = stream.State{TaskID: cp.TaskID, RunID: cp.RunID, Method: cp.Method, InputDigest: cp.InputDigest, Claimed: cp.Cursor != "", Epoch: cp.Epoch, Writer: cp.Writer, WorkerKey: cp.WorkerKey, LastOutput: cp.OutputSeq, HeadersSeen: cp.HeadersSeen, Headers: checkpointHeaders(cp.Headers)}
	s.deliveredState = s.state.Snapshot()
	s.delivered, s.deliveredCursor = cp.OutputSeq, cp.Cursor
	s.headers, s.headersReady = wire.FromMetadata(s.state.Headers), cp.HeadersSeen
	s.headersDelivered = cp.HeadersSeen
	if cp.Cursor != "" {
		cursor := cp.Cursor
		s.initialCursor = &cursor
		s.scannedCursor = cursor
	}
	s.mu.Unlock()
	s.startObservers(ref)
	close(s.inputClosed)
	s.sendMu.Unlock()
	// newTaskClient 已安装 Context 读取能力，恢复流继续使用相同生成桩的接收接口
	return s, nil
}
