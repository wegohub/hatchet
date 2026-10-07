package session

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// ControlName 是已有会话的控制入口，ACK、END 等帧始终带 Required owner。
const ControlName = "wego-session-control"

// OwnerLabel 流会话 owner 标签名；Required=true 保证后续控制任务回到初始化实例。
const OwnerLabel = "wego_session_owner"

// Control 控制任务输入，关联流身份、动作和编码帧；业务消息不直接绕过控制协议。
type Control struct {
	// StreamID 流会话身份，同一会话的全部控制任务必须携带相同值。
	StreamID string `json:"stream_id"`
	// Method 完整 RPC 方法名，例如 /wego.example.v1.UnaryGreeter/SayHello。
	Method string `json:"method,omitempty"`
	// Kind 协议控制类型，例如 START、OPEN、DATA、ACK 或 END。
	Kind string `json:"kind"`
	// Frame protobuf 流帧编码后的 base64 字符串，适配引擎 string 输出。
	Frame string `json:"frame,omitempty"`
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope。
	Input wire.Envelope `json:"input,omitempty"`
}

// Acknowledgment 控制任务确认，返回 owner、打开状态和累计消费位置。
type Acknowledgment struct {
	// Owner START 选择的实例身份；后续提交附带 Required=true 的 owner 标签。
	Owner string `json:"owner"`
	// Open 会话是否已经打开。
	Open bool `json:"open"`
	// Consumed 累计消费序号，例如 3 表示前三条已按序交付。
	Consumed uint64 `json:"consumed"`
}

// Handler 业务流执行函数，使用标准 context 与当前会话端点。
type Handler func(context.Context, *Endpoint) error

// Manager 管理本实例的会话；注册 handler 在启动前完成，运行期间仅会话表可变。
type Manager struct {
	// backend 内部后端接口；业务层不能取出其具体实现。
	backend ports.Backend
	// config 当前实例使用的配置快照。
	config spec.Runtime
	// owner 拥有当前资源或会话的对象；流会话后续控制任务必须回到同一 owner。
	owner string
	// handlers 完整 RPC 方法名到业务流 handler 的注册表，启动前完成注册。
	handlers map[string]Handler
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// sessions stream ID 到本实例会话端点的映射。
	sessions map[string]*Endpoint
	// closed 是否已关闭或禁止接收新工作；已有工作按关闭策略处理。
	closed bool
	// stopped 是否已经强制停止；停止后不得再创建回收定时器。
	stopped bool
}

// New 创建本实例的流会话管理器，初始化 handler 与 session 注册表。
func New(backend ports.Backend, config spec.Runtime, owner string) *Manager {
	return &Manager{
		backend:  backend,
		config:   config,
		owner:    owner,
		handlers: map[string]Handler{},
		sessions: map[string]*Endpoint{},
	}
}

// Register 在启动前注册流 handler，运行期间仅会话表可变。
func (m *Manager) Register(method string, handler Handler) {
	m.handlers[method] = handler
}

// Control 只更新会话状态或写入有界缓冲，不等待业务 handler 消费消息。
func (m *Manager) Control(ctx context.Context, input Control) (Acknowledgment, error) {
	if input.StreamID == "" {
		return Acknowledgment{}, status.Error(codes.InvalidArgument, "wego: stream ID is required")
	}

	m.mu.Lock()
	s := m.sessions[input.StreamID]
	if input.Kind == "START" {
		if m.closed {
			m.mu.Unlock()
			return Acknowledgment{}, status.Error(codes.Unavailable, "wego: worker is draining")
		}
		if s == nil {
			if _, ok := m.handlers[input.Method]; !ok {
				m.mu.Unlock()
				return Acknowledgment{}, status.Error(codes.Unimplemented, "wego: streaming method not registered")
			}

			active := 0
			// 逐个检查或清理会话，遵守端点锁和管理器锁的顺序，避免与控制处理产生竞态。
			for _, existing := range m.sessions {
				existing.mu.Lock()
				// 只把仍活跃的会话计入初始化容量，短期保留的终态记录不占新会话名额。
				if !existing.final {
					active++
				}
				existing.mu.Unlock()
			}
			// START 预留初始化记录但尚不占业务槽，需单独限制数量并由 TTL 回收。
			if active >= m.config.Slots*2 {
				m.mu.Unlock()
				return Acknowledgment{}, status.Error(codes.ResourceExhausted, "wego: session initialization capacity exhausted")
			}

			s = newEndpoint(input.StreamID, input.Method, input.Input, m.backend, m.config)
			m.sessions[input.StreamID] = s
			s.timer = time.AfterFunc(m.config.Stream.InitializationTTL, func() {
				s.mu.Lock()
				opened := s.open
				s.mu.Unlock()
				// 初始化 TTL 内没有完成 OPEN，回收 START 记录并让等待任务明确失败。
				if !opened {
					m.remove(input.StreamID, s)
					s.fail(status.Error(codes.DeadlineExceeded, "wego: stream initialization expired"))
				}
			})
		}
		m.mu.Unlock()
		return Acknowledgment{Owner: m.owner}, nil
	}

	m.mu.Unlock()
	if s == nil {
		// 记录已清理后的 ACK 和 CANCEL 按幂等确认处理，不重新创建业务会话。
		if input.Kind == "ACK" || input.Kind == "CANCEL" {
			return Acknowledgment{Owner: m.owner}, nil
		}

		return Acknowledgment{}, status.Error(codes.Unavailable, "wego: stream owner no longer has session")
	}

	frame, err := wire.DecodeFrame(input.Frame, m.config.Stream.MaxMessageBytes)
	if err != nil {
		return Acknowledgment{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if frame.StreamId != input.StreamID || frame.Kind != input.Kind {
		return Acknowledgment{}, status.Error(codes.InvalidArgument, "wego: control identity mismatch")
	}

	err = s.control(ctx, frame)
	s.mu.Lock()
	ack := Acknowledgment{Owner: m.owner, Open: s.open, Consumed: s.consumed}
	s.mu.Unlock()
	return ack, err
}

// Run 占用业务槽并等待 OPEN；重复 RUN 被拒绝，业务 handler 只启动一次。
func (m *Manager) Run(ctx context.Context, input Control) (*wire.StreamResult, error) {
	m.mu.Lock()
	s := m.sessions[input.StreamID]
	m.mu.Unlock()
	if s == nil {
		return nil, status.Error(codes.Unavailable, "wego: initialized session missing")
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "wego: session task already running")
	}

	s.running = true
	// info, _ 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用。
	info, _ := callctx.Get(ctx)
	if info == nil || info.Execution == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "wego: session execution context missing")
	}

	s.taskID = info.Execution.Info().TaskRunID
	s.ctx, s.cancel = wire.Incoming(ctx, s.input)
	s.mu.Unlock()
	defer m.retire(input.StreamID, s)
	defer s.timer.Stop()
	defer s.cancel()

	select {
	case <-s.opened:
	case <-s.ctx.Done():
		return s.finish(s.ctx.Err()), nil
	case <-s.failed:
		return s.finish(s.error()), nil
	}
	err := m.handlers[s.method](s.ctx, s)
	m.config.Logger.Debug("stream handler completed", "method", s.method, "stream_id", s.id, "error", err)
	return s.finish(err), nil
}

// remove 仅当映射仍指向指定端点时删除，避免迟到的清理误删其他对象。
func (m *Manager) remove(id string, s *Endpoint) {
	m.mu.Lock()
	if m.sessions[id] == s {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
}

// retire 释放业务缓冲并短期保留终态，允许迟到 END 和 ACK 幂等完成。
func (m *Manager) retire(id string, s *Endpoint) {
	s.mu.Lock()
	s.incoming = nil
	s.outgoing = nil
	s.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// 已经进入强制停止，不再保留终态 TTL 或创建回收计时器，直接移除记录。
	if m.stopped {
		delete(m.sessions, id)
		return
	}

	s.mu.Lock()
	s.timer = time.AfterFunc(m.config.Stream.InitializationTTL, func() {
		m.remove(id, s)
	})
	s.mu.Unlock()
}

// Drain 停止分配新会话，已有会话的控制消息仍可继续处理。
func (m *Manager) Drain() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
}

// Stop 取消剩余会话并停止回收定时器；停止后不得再创建终态回收计时器。
func (m *Manager) Stop() {
	m.mu.Lock()
	m.closed = true
	m.stopped = true
	sessions := make([]*Endpoint, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	// 逐个检查或清理会话，遵守端点锁和管理器锁的顺序，避免与控制处理产生竞态。
	for _, s := range sessions {
		s.mu.Lock()
		if s.timer != nil {
			s.timer.Stop()
		}
		s.mu.Unlock()
		s.fail(status.Error(codes.Canceled, "wego: server shutting down"))
		m.remove(s.id, s)
	}
}

// StartName 按完整方法名注册分配入口；例如 A/Watch 的 START 不会落到只支持 B/Chat 的实例。
func StartName(method string) string { return binding.Name(method) + "-start" }
