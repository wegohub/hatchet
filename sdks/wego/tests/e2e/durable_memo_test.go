//go:build e2e

package e2e

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestDurablePendingMemoRecovery 在真实引擎已登记 memo 后丢弃完成帧，重启 Worker 补算，再重启验证持久复用
func TestDurablePendingMemoRecovery(t *testing.T) {
	testDurableRestart(t, true)
}

// packetCodec 透明转发 protobuf 字节，不导入 Hatchet 类型，也不修改业务 API 或引擎
type packetCodec struct{}

// Name 保留原始 proto 内容类型，目标引擎仍使用自己的 protobuf decoder
func (*packetCodec) Name() string { return "proto" }

// Marshal 返回当前协议帧；调用方每次使用独立字节容器
func (*packetCodec) Marshal(value any) ([]byte, error) { return *value.(*[]byte), nil }

// Unmarshal 保存帧的副本，gRPC transport 复用原始缓冲不能改变已接收的数据
func (*packetCodec) Unmarshal(data []byte, value any) error {
	*value.(*[]byte) = append([]byte(nil), data...)
	return nil
}

// memoFaultProxy 只丢弃第一次 wego.now 完成通知，其余业务和协议透明转发
type memoFaultProxy struct {
	// conn 由此测试代理拥有，不注入到生产 SDK
	conn *grpc.ClientConn
	// dropOnce 跨 Worker 重启只允许一次故障注入
	dropOnce atomic.Bool
	// dropped 确认真实 CompleteMemo 已被拦截，不能用固定 sleep 猜测
	dropped chan struct{}
	// pending 确认恢复查询得到已存在、空 payload 的真实引擎 MemoAck
	pending chan struct{}
	// pumps 跟踪所有双向转发 goroutine，停止代理后必须归零
	pumps sync.WaitGroup
}

// protoField 读取一个官方协议字段；只供固定版本故障 fixture 使用，不参与 SDK 业务解码
// 当前请求 CompleteMemo 为字段 7、响应 MemoAck 为字段 2；不按字节子串匹配 payload
func protoField(data []byte, wanted protowire.Number) ([]byte, uint64, error) {
	for len(data) > 0 {
		// number 与 kind 来自 protobuf tag，不合法数据必须终止 fixture
		number, kind, size := protowire.ConsumeTag(data)
		if size < 0 {
			return nil, 0, protowire.ParseError(size)
		}
		data = data[size:]
		if number == wanted && kind == protowire.BytesType {
			// field 与 count 是 length-delimited 字段，不把嵌套消息视为字符串
			field, count := protowire.ConsumeBytes(data)
			if count < 0 {
				return nil, 0, protowire.ParseError(count)
			}
			return field, 0, nil
		}
		if number == wanted && kind == protowire.VarintType {
			// value 与 count 覆盖 memo_already_existed 的 bool 编码
			value, count := protowire.ConsumeVarint(data)
			if count < 0 {
				return nil, 0, protowire.ParseError(count)
			}
			return nil, value, nil
		}
		// count 用于跳过其他字段，不能假定字段顺序或省略情况
		count := protowire.ConsumeFieldValue(number, kind, data)
		if count < 0 {
			return nil, 0, protowire.ParseError(count)
		}
		data = data[count:]
	}
	return nil, 0, nil
}

// filterRequest 只丢弃 wego.now 的首个完整完成帧，其他 memo 或身份请求保持原样
func (p *memoFaultProxy) filterRequest(data []byte) (bool, error) {
	// complete 是官方请求 oneof 中的 CompleteMemo body
	complete, _, err := protoField(data, 7)
	if err != nil || len(complete) == 0 {
		return false, err
	}
	// keyBytes 是原始字节 memo key；业务数据里偶然出现 wego.now 不会匹配
	keyBytes, _, err := protoField(complete, 3)
	if err != nil {
		return false, err
	}
	if string(keyBytes) == "wego.now" && p.dropOnce.CompareAndSwap(false, true) {
		close(p.dropped)
		return true, nil
	}
	return false, nil
}

// observeResponse 观察真实引擎 pending memo ACK，既不改写响应也不合成引擎状态
func (p *memoFaultProxy) observeResponse(data []byte) error {
	// ack 是官方响应的 MemoAck body，仅在已注入故障后检查
	ack, _, err := protoField(data, 2)
	if err != nil || len(ack) == 0 || !p.dropOnce.Load() {
		return err
	}
	// existed 判断引擎是否确实恢复了已登记的节点
	_, existed, err := protoField(ack, 2)
	if err != nil {
		return err
	}
	// payload 的缺失表示该节点尚未完成，而非合法时间值
	payload, _, err := protoField(ack, 3)
	if err == nil && existed != 0 && len(payload) == 0 {
		select {
		case p.pending <- struct{}{}:
		default:
		}
	}
	return err
}

// forward 使用不解释业务类型的双向代理，仅在 durable 方法上注入一次 memo 故障
func (p *memoFaultProxy) forward(_ any, incoming grpc.ServerStream) error {
	// method 必须来自真实 transport，代理不能猜测服务名
	method, ok := grpc.MethodFromServerStream(incoming)
	if !ok {
		return errors.New("missing proxy method")
	}
	// headers 只转发授权与协议 metadata，不记录其中的内容
	headers, _ := metadata.FromIncomingContext(incoming.Context())
	// ctx 同时控制后台转发和目标 gRPC 订阅
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(incoming.Context(), headers.Copy()))
	defer cancel()
	// outgoing 为未知服务创建原始流，原生 unary 调用也通过单帧和半关闭完成
	outgoing, err := p.conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method, grpc.ForceCodec(&packetCodec{}))
	if err != nil {
		return err
	}
	// results 使用两个缓冲槽，handler 提前返回也不会阻塞转发 goroutine 的退出通知
	results := make(chan error, 2)
	p.pumps.Add(2)
	go func() {
		defer p.pumps.Done()
		for {
			// data 是客户端的一帧原始 protobuf 字节
			var data []byte
			// err 为入站关闭或传输故障，正常半关闭继续等待目标响应
			if err := incoming.RecvMsg(&data); err != nil {
				if err == io.EOF {
					err = outgoing.CloseSend()
				}
				results <- err
				return
			}
			if method == "/v1.V1Dispatcher/DurableTask" {
				// drop 表示已确认收到的首个 memo 完成通知，本次故障不会拦截后续恢复
				drop, err := p.filterRequest(data)
				if err != nil {
					results <- err
					return
				}
				if drop {
					continue
				}
			}
			// err 确认原始请求已交付，故障退出会取消另一个转发方向
			if err := outgoing.SendMsg(&data); err != nil {
				results <- err
				return
			}
		}
	}()
	go func() {
		defer p.pumps.Done()
		// sentHeaders 防止重复 SendHeader；每次 handler 各自拥有该状态
		sentHeaders := false
		for {
			// data 保存目标引擎交付的单帧，不改变 protobuf 编码
			var data []byte
			// err 保留目标引擎终态，正常 EOF 只表示输出方向结束
			if err := outgoing.RecvMsg(&data); err != nil {
				incoming.SetTrailer(outgoing.Trailer())
				if err == io.EOF {
					err = nil
				}
				results <- err
				return
			}
			if !sentHeaders {
				// header 与 err 复制原生响应 headers，不推断任务状态
				header, err := outgoing.Header()
				if err == nil {
					err = incoming.SendHeader(header)
				}
				if err != nil {
					results <- err
					return
				}
				sentHeaders = true
			}
			if method == "/v1.V1Dispatcher/DurableTask" {
				// err 来自官方 memo ACK 的结构检查，不合成恢复成功
				if err := p.observeResponse(data); err != nil {
					results <- err
					return
				}
			}
			// err 确认真实响应已转发到 Worker，不能静默丢失结果
			if err := incoming.SendMsg(&data); err != nil {
				results <- err
				return
			}
		}
	}()
	// direction 两个方向都退出才正常返回；错误先返回会撤销传输并唤醒另一方向
	for direction := 0; direction < 2; direction++ {
		// err 对应一个转发方向，缓冲通知保证取消后另一个方向可退出
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

// startMemoFaultProxy 创建本机明文故障代理，t.Cleanup 关闭所有连接与转发 goroutine
func startMemoFaultProxy(t *testing.T) (*memoFaultProxy, string) {
	t.Helper()
	// address 为实际本机引擎地址，不包含 token 或数据库密码
	address := os.Getenv("WEGO_GRPC_ADDRESS")
	if address == "" {
		address = "localhost:7077"
	}
	// conn 只属于代理，原始 token 从每次入站 metadata 透明转发
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	// listener 使用系统分配的空闲端口，避免影响现有 Hatchet 实例
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	// proxy 在整个测试内复用，第一次丢帧后所有恢复执行都正常转发
	proxy := &memoFaultProxy{conn: conn, dropped: make(chan struct{}), pending: make(chan struct{}, 1)}
	// server 只处理未知服务的原始帧，不注册业务 handler
	server := grpc.NewServer(grpc.ForceServerCodec(&packetCodec{}), grpc.UnknownServiceHandler(proxy.forward), grpc.WaitForHandlers(true))
	// stopped 确认监听已退出，停止测试不能留下代理 goroutine
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); conn.Close(); <-stopped; proxy.pumps.Wait() })
	return proxy, listener.Addr().String()
}
