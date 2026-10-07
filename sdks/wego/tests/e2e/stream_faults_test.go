//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/session"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// faultService 可阻塞的流 handler，供乱序、断流与 owner 退出故障注入。
type faultService struct {
	// pb.UnimplementedGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	pb.UnimplementedGreeterServer
	// started 入口是否已经启动；与停止并发时在锁内读写。
	started atomic.Int32
	// gate 故障测试的阻塞门，关闭后允许 handler 继续。
	gate <-chan struct{}
}

// UploadHellos 接收多条 Request，收到输入 EOF 后返回最终 Reply，例如两条输入得到 Count=2。
func (s *faultService) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	s.started.Add(1)
	if s.gate != nil {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-s.gate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	// count 此步骤已经观察到的执行或消息次数，后续与预期重试数、输入数或峰值比较。
	count := int32(0)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		in, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(scenarios.Reply(stream.Context(), &pb.Request{Count: count}))
		}
		if err != nil {
			return err
		}
		// 检查 in.Count != count；不满足协议或配置约束时返回 DataLoss（handler input order mismatch）。
		if in.Count != count {
			return status.Error(codes.DataLoss, "handler input order mismatch")
		}

		count++
	}
}

// WatchHellos 处理单条 Request 并连续发送 Reply，例如 Count=3 时发送三条输出。
func (s *faultService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	s.started.Add(1)
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := int32(0); i < in.Count; i++ {
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
		if err := stream.Send(scenarios.Reply(stream.Context(), &pb.Request{Count: i})); err != nil {
			return err
		}
	}
	return nil
}

// ChatHellos 边接收 Request 边发送 Reply，输入 EOF 只结束输入方向。
func (s *faultService) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	s.started.Add(1)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(scenarios.Reply(stream.Context(), in)); err != nil {
			return err
		}
	}
}

// faultBackend 透传真实后端并按模式暂扣或复制帧的故障注入层。
type faultBackend struct {
	// ports.Backend 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	ports.Backend
	// mode 当前故障注入模式。
	mode string
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// held 故障注入中暂扣的控制帧，稍后用于制造乱序或缺口。
	held *session.Control
	// heldOptions 暂扣控制帧对应的运行选项，恢复发送时保持 owner 亲和性。
	heldOptions model.RunOptions
	// last 最近一次控制消息，供测试制造重复帧。
	last session.Control
	// sessionRunID 被测会话任务的 RunID。
	sessionRunID string
}

// ackRun 构造返回指定控制确认的运行句柄，模拟真实任务等待接口。
func ackRun() ports.Run {
	return ports.Run{
		ID: "injected-control-ack",
		Wait: func(context.Context) (ports.Result, error) {
			return ports.Result{Outputs: map[string]any{"control": session.Acknowledgment{Open: true}}}, nil
		},
	}
}

// deliver 将控制帧交给实际后端，并等待执行确认，使注入故障位置可观察。
func (b *faultBackend) deliver(ctx context.Context, c session.Control, opts model.RunOptions) error {
	// ref, err 接收 b.Backend.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	ref, err := b.Backend.Run(ctx, session.ControlName, c, opts)
	if err == nil {
		_, err = ref.Wait(ctx)
	}
	return err
}

// Run 提交任务并按此入口的结果类型等待或返回执行结果；输入和执行策略共同决定调度。
func (b *faultBackend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	// c, control 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	c, control := input.(session.Control)
	if !control || (name != session.ControlName && c.Kind != "START") {
		// ref, err 接收 b.Backend.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		ref, err := b.Backend.Run(ctx, name, input, opts)
		if err == nil && control && c.Kind == "RUN" {
			b.mu.Lock()
			b.sessionRunID = ref.ID
			b.mu.Unlock()
		}
		return ref, err
	}

	b.mu.Lock()
	b.last = c
	b.mu.Unlock()
	if b.mode == "input-reorder" && c.Kind == "DATA" {
		// f, err 接收解码后的协议数据，错误时终止当前处理，不把非法输入推进到业务 handler。
		f, err := wire.DecodeFrame(c.Frame, 1<<20)
		if err != nil {
			return ports.Run{}, err
		}
		if f.Seq == 1 {
			b.mu.Lock()
			// copy 当前记录的独立结构体副本，用于改写本调用状态而不并发修改共享原对象。
			copy := c
			b.held = &copy
			b.heldOptions = opts
			b.mu.Unlock()
			return ackRun(), nil
		}
		if f.Seq == 2 {
			// end, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
			end, _ := wire.EncodeFrame(&wire.Frame{
				Version:   wire.Version,
				StreamId:  c.StreamID,
				Kind:      "END",
				Direction: "input",
				Seq:       2,
			})
			if err = b.deliver(ctx, session.Control{StreamID: c.StreamID, Kind: "END", Frame: end}, opts); err != nil {
				return ports.Run{}, err
			}
			if err = b.deliver(ctx, c, opts); err != nil {
				return ports.Run{}, err
			}
			if err = b.deliver(ctx, c, opts); err != nil {
				return ports.Run{}, err
			}

			b.mu.Lock()
			// held, heldOpts 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
			held, heldOpts := *b.held, b.heldOptions
			b.mu.Unlock()
			return b.Backend.Run(ctx, name, held, heldOpts)
		}
	}
	// ref, err 接收 b.Backend.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	ref, err := b.Backend.Run(ctx, name, input, opts)
	if err != nil {
		return ports.Run{}, err
	}
	if b.mode == "duplicate-open" && c.Kind == "OPEN" {
		// original 后端原始执行上下文，保留 durable 等待序号和父子运行身份。
		original := ref.Wait
		ref.Wait = func(ctx context.Context) (ports.Result, error) {
			// result, e 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
			result, e := original(ctx)
			if e == nil {
				e = b.deliver(ctx, c, opts)
			}
			return result, e
		}
	}
	return ref, nil
}

// Stream 在真实订阅前按故障模式制造延迟或断流，透传时保持原始运行身份。
func (b *faultBackend) Stream(ctx context.Context, id string, consume func(string) error) error {
	if b.mode == "delayed-subscription" {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(800 * time.Millisecond):
		}
	}
	// first 首个结果或帧记录，用于比较后续重复或恢复行为。
	var first string
	return b.Backend.Stream(ctx, id, func(value string) error {
		// frame, err 接收解码后的协议数据，错误时终止当前处理，不把非法输入推进到业务 handler。
		frame, err := wire.DecodeFrame(value, 1<<20)
		if err != nil {
			return err
		}
		// 检查 frame.Kind == "READY" && b.mode == "disconnect"；不满足协议或配置约束时返回 Unavailable（injected subscription disconnect）。
		if frame.Kind == "READY" && b.mode == "disconnect" {
			return status.Error(codes.Unavailable, "injected subscription disconnect")
		}
		if frame.Kind == "DATA" {
			// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
			switch b.mode {
			case "output-reorder":
				if frame.Seq == 1 {
					first = value
					return nil
				}
				if frame.Seq == 2 {
					// e 接收 consume 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
					if e := consume(value); e != nil {
						return e
					}
					// e 接收 consume 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
					if e := consume(value); e != nil {
						return e
					}

					return consume(first)
				}
			case "output-gap":
				// 丢弃业务帧但仍确认传输，让会话任务正常结束；接收端必须检测到输出缺口。
				if frame.Seq == 1 {
					b.mu.Lock()
					// c 当前控制消息或连接的视图，后续操作保持同一流身份及 owner 路由。
					c := b.last
					b.mu.Unlock()
					// encoded, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
					encoded, _ := wire.EncodeFrame(&wire.Frame{
						Version:   wire.Version,
						StreamId:  frame.StreamId,
						Kind:      "ACK",
						Direction: "output",
						Ack:       1,
					})
					c.Kind, c.Frame = "ACK", encoded
					return b.deliver(ctx, c, model.RunOptions{})
				}
			case "invalid-direction":
				frame.Direction = "input"
				value, _ = wire.EncodeFrame(frame)
			}
		}
		return consume(value)
	})
}

// faultConn 使用故障后端的标准 gRPC ClientConnInterface 适配器。
type faultConn struct {
	// backend 内部后端接口；业务层不能取出其具体实现。
	backend ports.Backend
	// config 当前实例使用的配置快照。
	config spec.Runtime
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭。
	engine *engine.Engine
}

// Invoke 实现 grpc.ClientConnInterface 的 unary 调用，将 protobuf 请求提交为任务并把结果解码到 reply；调用上下文控制等待预算。
func (c *faultConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return status.Error(codes.Unimplemented, "stream-only fixture")
}

// NewStream 实现 grpc.ClientConnInterface 的流调用，建立 owner 会话并返回标准 gRPC ClientStream。
func (c *faultConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空。
	ctx, done, err := c.engine.BeginIO(ctx)
	if err != nil {
		return nil, err
	}

	// stream, err 接收 session.NewClientManaged 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	stream, err := session.NewClientManaged(ctx, c.backend, c.config, desc, method, func(error) {
		done()
	}, opts...)
	if err != nil {
		done()
	}
	return stream, err
}

// TestStreamFaults 在真实 Worker 传输路径注入重复、乱序、缺口、慢消费、断流及 owner 退出，逐项断言结果。
func TestStreamFaults(t *testing.T) {
	preflight(t)
	// infoConn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	infoConn, err := client.New(client.WithRuntime(scenarios.Runtime("wego_fault_preflight_")...))
	if err != nil {
		t.Fatal(err)
	}
	// info, err 接收 infoConn.Info 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	info, err := infoConn.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = infoConn.Close(); err != nil {
		t.Fatal(err)
	}
	// report 并发安全的验收记录收集器，成功断言与清理结果逐条写入，不保存凭证。
	report := &scenarios.Report{}
	// results 场景名称到实际验收状态的映射；仅业务断言和清理通过后写入 PASSED。
	results := map[string]string{}
	// 逐项执行九组流故障：延迟、重复、双向乱序、缺口、非法方向、断流、满窗口及 owner 退出；每项记录断言和清理结果。
	for _, mode := range []string{
		"delayed-subscription",
		"duplicate-open",
		"input-reorder",
		"output-reorder",
		"output-gap",
		"invalid-direction",
		"disconnect",
		"full-window",
		"owner-exit",
	} {
		t.Run(mode, func(t *testing.T) {
			results[mode] = "FAILED"
			defer func() {
				if t.Failed() {
					results[mode] = "FAILED"
				}
			}()

			// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
			defer cancel()

			// gate 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
			gate := make(chan struct{})
			// service 本场景的受控业务服务，handler 注入明确行为并记录实际调用结果。
			service := &faultService{}
			if mode == "full-window" {
				service.gate = gate
			}
			// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应。
			names := []string{session.ControlName}
			// 逐项处理 pb.Greeter_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
			for _, d := range pb.Greeter_ServiceDesc.Methods {
				names = append(names, scenarios.TaskName("/wego.example.v1.Greeter/"+d.MethodName))
			}
			// 逐项处理 pb.Greeter_ServiceDesc.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
			for _, d := range pb.Greeter_ServiceDesc.Streams {
				names = append(names, scenarios.TaskName("/wego.example.v1.Greeter/"+d.StreamName)+"-session")
				names = append(names, scenarios.TaskName("/wego.example.v1.Greeter/"+d.StreamName)+"-start")
			}
			// h, err 接收 scenarios.StartRegistered 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			h, err := scenarios.StartRegistered(
				ctx,
				"stream-faults",
				report,
				func(r grpc.ServiceRegistrar) {
					pb.RegisterGreeterServer(r, service)
				},
				names,
				[]runtime.Option{
					runtime.WithSlots(1),
					runtime.WithControlSlots(4),
					runtime.WithStream(runtime.StreamConfig{
						Window:          2,
						BufferBytes:     4 << 20,
						MaxMessageBytes: 1 << 20,
						// 真实握手包含任务调度与数据库确认，预算覆盖长套件负载；800ms 延迟注入保持不变。
						HandshakeTimeout: 20 * time.Second,
						// 初始化保留时间须长于握手预算，避免正常排队被 TTL 回收。
						InitializationTTL: 30 * time.Second,
					}),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				// e 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略。
				if e := h.Close(); e != nil {
					t.Error(e)
				}
			}()

			// config 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil。
			config := spec.Defaults()
			// 逐项处理 h.Runtime，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
			for _, o := range h.Runtime {
				o(&config)
			}
			// e, err 接收 engine.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			e, err := engine.New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			// b 当前后端或测试后端对象，在所属实例内管理运行与资源，不经公开 API 暴露。
			b := &faultBackend{Backend: e.Backend, mode: mode}
			// rpc 构造标准 gRPC 流服务客户端，传输由传入连接决定。
			rpc := pb.NewGreeterClient(&faultConn{backend: b, config: config, engine: e})
			// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
			switch mode {
			case "delayed-subscription", "duplicate-open", "input-reorder", "full-window":
				// started 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
				started := time.Now()
				// stream, e 建立 client stream；发送 END 只半关闭输入，响应仍需完整接收。
				stream, e := rpc.UploadHellos(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if mode == "delayed-subscription" && time.Since(started) < 800*time.Millisecond {
					t.Fatal("OPEN bypassed subscription handshake")
				}
				// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
				for i := int32(0); i < 2; i++ {
					if e = stream.Send(&pb.Request{Count: i}); e != nil {
						t.Fatal(e)
					}
				}
				if mode == "full-window" {
					// third 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
					third := make(chan error, 1)
					go func() {
						third <- stream.Send(&pb.Request{Count: 2})
					}()
					// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
					select {
					// e 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
					case e := <-third:
						t.Fatalf("window did not block third send: %v", e)
					case <-time.After(150 * time.Millisecond):
					}
					b.mu.Lock()
					// last 最近一次控制消息，供测试制造重复帧。
					last := b.last
					b.mu.Unlock()
					// ping, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
					ping, _ := wire.EncodeFrame(&wire.Frame{
						Version:  wire.Version,
						StreamId: last.StreamID,
						Kind:     "PING",
						Nonce:    "window-fixture",
					})
					// controlCtx, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
					controlCtx, stop := context.WithTimeout(ctx, 3*time.Second)
					e = b.deliver(controlCtx, session.Control{StreamID: last.StreamID, Kind: "PING", Frame: ping}, model.RunOptions{})
					stop()
					if e != nil {
						t.Fatal("full business/window blocked control:", e)
					}
					close(gate)
					if e = <-third; e != nil {
						t.Fatal(e)
					}
				}
				// out, e 关闭发送方向并等待唯一最终响应，不能把 END 的确认当作成功结果。
				out, e := stream.CloseAndRecv()
				if e != nil {
					t.Fatal(e)
				}
				// want 本用例预期的状态或数量，后续将真实调用结果与此值比较。
				want := int32(2)
				if mode == "full-window" {
					want = 3
				}
				if out.Count != want || service.started.Load() != 1 {
					t.Fatalf("dedup/order/handler count: %v / %d", out, service.started.Load())
				}
				report.Add(h, mode+": ordered complete output and one handler", out)
			case "output-reorder", "output-gap", "invalid-direction":
				// stream, e 建立 server stream，输出通过订阅传递并由累计 ACK 释放窗口。
				stream, e := rpc.WatchHellos(ctx, &pb.Request{Count: 2})
				if e != nil {
					if mode == "invalid-direction" && status.Code(e) == codes.DataLoss {
						report.Add(h, "malformed output rejected during generated client initialization", nil)
						results[mode] = "PASSED"
						return
					}

					t.Fatal(e)
				}
				if mode == "output-reorder" {
					// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
					for i := int32(0); i < 2; i++ {
						// out, e 接收下一项业务数据；EOF 只在对应方向正常结束后成立。
						out, e := stream.Recv()
						if e != nil || out.Count != i {
							t.Fatalf("output reorder: %v / %v", out, e)
						}
					}
					if _, e = stream.Recv(); e != io.EOF {
						t.Fatal(e)
					}
				} else {
					if _, e = stream.Recv(); status.Code(e) != codes.DataLoss {
						t.Fatalf("missing/malformed output must fail DataLoss: %v", e)
					}
				}
				report.Add(h, mode+": protocol result asserted", nil)
			case "disconnect":
				// _, e 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
				if _, e := rpc.ChatHellos(ctx); status.Code(e) != codes.Unavailable {
					t.Fatalf("disconnected handshake: %v", e)
				}
				report.Add(h, "subscription disconnect fails without restarting session", nil)
			case "owner-exit":
				// stream, e 建立 bidi stream，两个方向分别维护序号与背压。
				stream, e := rpc.ChatHellos(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if e = stream.Send(&pb.Request{Message: "owner"}); e != nil {
					t.Fatal(e)
				}
				// out, e 接收下一项业务数据；EOF 只在对应方向正常结束后成立。
				out, e := stream.Recv()
				if e != nil {
					t.Fatal(e)
				}
				// stopCtx, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
				stopCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
				// closedAt 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
				closedAt := time.Now()
				e = support.StopServer(stopCtx, h.Server)
				if time.Since(closedAt) > time.Second {
					t.Fatal("owner shutdown exceeded caller budget")
				}
				stop()
				if e == nil || !errors.Is(e, context.DeadlineExceeded) {
					t.Fatalf("owner budget: %v", e)
				}
				_, e = stream.Recv()
				if status.Code(e) != codes.Unavailable && status.Code(e) != codes.Canceled {
					t.Fatalf("owner loss: %v", e)
				}
				// 此处主动耗尽关闭预算，截止时间错误就是预期的关闭结果。
				h.Server = nil
				report.Add(h, "owner exit explicitly fails stream without reassignment", out)
			}
			b.mu.Lock()
			// runID 会话业务任务的运行身份，用于订阅输出和取消。
			runID := b.sessionRunID
			b.mu.Unlock()
			report.Add(h, mode+": session identity recorded", &pb.Reply{RunId: runID})
			results[mode] = "PASSED"
		})
	}
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置。
	path := filepath.Join("..", "..", ".test-results", fmt.Sprintf("stream-faults-%d.json", time.Now().UnixNano()))
	// err 接收 os.MkdirAll 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, _ := json.MarshalIndent(
		map[string]any{
			"server_version": info.Version,
			"command":        "sdks/wego/scripts/local-test.py go test -tags=e2e ./sdks/wego/tests/e2e/... -run TestStreamFaults -v -timeout 10m",
			"mq":             "postgresql",
			"records":        report.Records,
			"results":        results,
		},
		"",
		"  ",
	)
	// err 接收 os.WriteFile 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log("fault report:", path)
}
