package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// Endpoint 是业务 handler 的流端点。可变状态由 mu 保护，输入与输出各自维护序号和窗口。
type Endpoint struct {
	// mu 保护以下可变状态，等待和远程调用必须在解锁后执行。
	mu sync.Mutex

	// id 当前资源的唯一身份，用于查找对应运行或会话。
	id string
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称。
	method string
	// input START 带来的 protobuf 请求 envelope，用于恢复 metadata、trace 和 deadline。
	input wire.Envelope
	// backend 内部后端接口；业务层不能取出其具体实现。
	backend ports.Backend
	// config 当前实例使用的配置快照。
	config spec.Runtime

	// endpointLifecycle 按值保存握手与取消状态，不增加独立分配或独立锁。
	endpointLifecycle
	// endpointInput 按值保存输入方向排序和半关闭状态。
	endpointInput
	// endpointOutput 按值保存输出方向序号和未确认成本。
	endpointOutput
	// endpointResponse 按值保存 metadata 与单个最终响应。
	endpointResponse
}

// newEndpoint 初始化尚未 OPEN 的端点并为默认窗口预留有界缓存；通知通道在实际等待时建立。
func newEndpoint(id, method string, input wire.Envelope, b ports.Backend, config spec.Runtime) *Endpoint {
	return &Endpoint{
		id:      id,
		method:  method,
		input:   input,
		backend: b,
		config:  config,
		endpointLifecycle: endpointLifecycle{
			ctx:    context.Background(),
			cancel: func() {},
			opened: make(chan struct{}),
			failed: make(chan struct{}),
		},
		endpointInput: endpointInput{
			incoming: make(map[uint64][]byte, initialWindowCapacity(config.Stream.Window)),
		},
		endpointOutput: endpointOutput{
			outgoing: make(map[uint64]int, initialWindowCapacity(config.Stream.Window)),
		},
	}
}

// Context 返回当前执行上下文，包含取消与截止时间。
func (s *Endpoint) Context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx
}

// Method 返回当前完整 RPC 方法名，供拦截器和消息变换识别调用。
func (s *Endpoint) Method() string {
	return s.method
}

// error 读取受锁保护的会话错误，用于终态与取消路径。
func (s *Endpoint) error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

// wait 将状态广播与调用取消统一为等待入口；取消竞态中已经记录的协议故障优先返回。
// 例如 DataLoss 触发业务 ctx 取消时，不能把实际故障降为普通 context.Canceled。
func (s *Endpoint) wait(ctx context.Context, changed <-chan struct{}) error {
	if err := waitForChange(ctx, changed); err != nil {
		if failure := s.error(); failure != nil {
			return failure
		}
		return err
	}
	return nil
}

// fail 只记录第一个会话故障，关闭失败通知并取消业务上下文，唤醒发送和接收等待者。
func (s *Endpoint) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLocked(err)
}

// failLocked 在同一个临界区中保存首个故障并唤醒全部等待者；CANCEL 与 DATA 不存在解锁间隙。
func (s *Endpoint) failLocked(err error) {
	if s.failure != nil {
		return
	}
	s.failure = err
	close(s.failed)
	s.cancel()
	s.notify()
}

// publish 编码帧并发布到当前任务的流订阅出口。
func (s *Endpoint) publish(ctx context.Context, frame *wire.Frame) error {
	frame.Version = wire.Version
	frame.StreamId = s.id
	encoded, err := wire.EncodeFrame(frame)
	if err != nil {
		return err
	}

	s.mu.Lock()
	taskID := s.taskID
	s.mu.Unlock()
	// RUN 尚未提供业务任务身份时不能发布输出，控制任务身份不能代替业务订阅出口。
	if taskID == "" {
		return status.Error(codes.Unavailable, "wego: session task not ready")
	}

	return s.backend.Publish(ctx, taskID, []byte(encoded))
}

// Receive 只按连续序号交付输入，实际消费后发送累计 ACK。
// END 只半关闭输入，缓冲中的全部输入交付后才返回 EOF。
func (s *Endpoint) Receive(ctx context.Context) ([]byte, error) {
	for {
		s.mu.Lock()
		// 故障检查与取出消息或分配序号处于同一临界区；失败后不再消费缓存或占用新额度。
		if s.failure != nil {
			err := s.failure
			s.mu.Unlock()
			return nil, err
		}
		if payload, ok := s.incoming[s.consumed+1]; ok {
			s.consumed++
			seq := s.consumed
			delete(s.incoming, seq)
			s.inputBytes -= len(payload)
			s.notify()
			s.mu.Unlock()
			if err := s.publish(ctx, &wire.Frame{Kind: "ACK", Direction: "input", Ack: seq}); err != nil {
				s.fail(err)
				return nil, err
			}

			return payload, nil
		}
		// END 声明的输入全部交付后返回输入 EOF；例如 END=2、consumed=2，输出方向仍开放。
		if s.ended && s.consumed == s.lastInput {
			s.mu.Unlock()
			return nil, io.EOF
		}

		// changed 在锁内登记本次等待，广播与条件检查之间不能存在订阅空隙。
		changed := s.watch()
		s.mu.Unlock()
		if err := s.wait(ctx, changed); err != nil {
			return nil, err
		}
	}
}

// Send 在输出窗口和字节预算内发布数据；窗口满时等待 ACK，不占用控制 Worker。
func (s *Endpoint) Send(ctx context.Context, payload []byte) error {
	if len(payload) > s.config.Stream.MaxMessageBytes {
		return status.Error(codes.ResourceExhausted, "wego: output message too large")
	}
	if err := s.SendHeader(nil); err != nil {
		return err
	}

	for {
		s.mu.Lock()
		if s.failure != nil {
			err := s.failure
			s.mu.Unlock()
			return err
		}
		if s.final {
			s.mu.Unlock()
			return status.Error(codes.FailedPrecondition, "wego: stream completed")
		}
		if !s.outputWindowFull(len(payload)) {
			// 序号耗尽不能回绕到非法的 DATA=0，也不能覆盖尚未确认的消息。
			if s.outputSeq == ^uint64(0) {
				s.mu.Unlock()
				return status.Error(codes.ResourceExhausted, "wego: output sequence exhausted")
			}
			s.outputSeq++
			seq := s.outputSeq
			s.outgoing[seq] = len(payload)
			s.outputBytes += len(payload)
			s.mu.Unlock()
			if err := s.publish(ctx, &wire.Frame{
				Kind:      "DATA",
				Direction: "output",
				Seq:       seq,
				Payload:   payload,
			}); err != nil {
				s.fail(err)
				return err
			}

			return nil
		}

		// changed 在锁内登记本次等待，广播与条件检查之间不能存在订阅空隙。
		changed := s.watch()
		s.mu.Unlock()
		if err := s.wait(ctx, changed); err != nil {
			return err
		}
	}
}

// outputWindowFull 在持锁时检查输出条数与剩余字节预算；两者任一不足都必须等待 ACK。
func (s *Endpoint) outputWindowFull(payloadBytes int) bool {
	return len(s.outgoing) >= s.config.Stream.Window || payloadBytes > s.config.Stream.BufferBytes-s.outputBytes
}

// SetHeader 合并响应头缓存，不立即发送。
func (s *Endpoint) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.headersSent {
		return status.Error(codes.FailedPrecondition, "wego: headers already sent")
	}

	s.headers = metadata.Join(s.headers, md)
	return nil
}

// SendHeader 合并并发布响应头；重复发送由当前传输的状态规则处理。
func (s *Endpoint) SendHeader(md metadata.MD) error {
	s.mu.Lock()
	if s.headersSent {
		s.mu.Unlock()
		if len(md) > 0 {
			return status.Error(codes.FailedPrecondition, "wego: headers already sent")
		}

		return nil
	}

	s.headers = metadata.Join(s.headers, md)
	s.headersSent = true
	headers := s.headers.Copy()
	ctx := s.ctx
	s.mu.Unlock()
	return s.publish(ctx, &wire.Frame{Kind: "HEADER", Metadata: wire.Metadata(headers)})
}

// SetTrailer 合并响应尾部 metadata，随最终状态返回。
func (s *Endpoint) SetTrailer(md metadata.MD) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.trailers = metadata.Join(s.trailers, md)
}

// SetResponse 保存 client stream 的最终 unary 响应，随后与终态一起交付。
func (s *Endpoint) SetResponse(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.responseSent {
		return status.Error(codes.Internal, "wego: multiple final responses")
	}

	s.responseSent = true
	s.finalResponse = copyPayload(data)
	return nil
}

// finish 将最终 status、元数据及输出末序号一并上报。
// 客户端必须先收齐末序号之前的输出，才能返回最终 status 或 EOF。
func (s *Endpoint) finish(err error) *wire.StreamResult {
	// headerErr 保存自动补发响应头的结果，已有业务错误优先保留。
	if headerErr := s.SendHeader(nil); err == nil && headerErr != nil {
		err = headerErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failure != nil && err == nil {
		err = s.failure
	}
	s.final = true
	st := status.Convert(err)
	if errors.Is(err, context.Canceled) {
		st = status.New(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		st = status.New(codes.DeadlineExceeded, err.Error())
	}
	data, _ := proto.Marshal(st.Proto())
	return &wire.StreamResult{
		Response:    s.finalResponse,
		HasResponse: s.responseSent,
		Status:      data,
		Headers:     wire.Metadata(s.headers),
		Trailers:    wire.Metadata(s.trailers),
		LastSeq:     s.outputSeq,
	}
}

// Decode 解析引擎的 JSON 确认：string、[]byte、RawMessage 直接解码，结构化对象先编码。
// 例如 []byte(`{"consumed":3}`) 是 JSON 文本，不应被 json.Marshal 转换为 base64 字符串。
func Decode(value any, target any) error {
	// data 保存 JSON 文本，字节输入不回编码也不借机改写调用者缓冲。
	var data []byte
	switch source := value.(type) {
	case string:
		data = []byte(source)
	case []byte:
		data = source
	case json.RawMessage:
		data = source
	default:
		// err 保存对象编码错误，失败时不能使用部分 JSON 解码。
		var err error
		data, err = json.Marshal(source)
		if err != nil {
			return err
		}
	}
	// nil 字节或 RawMessage 遵循 JSON 的 null 语义；非 nil 的空文本仍是非法 JSON。
	if data == nil {
		data = []byte("null")
	}
	return json.Unmarshal(data, target)
}
