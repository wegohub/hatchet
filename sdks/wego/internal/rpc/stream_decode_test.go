package rpc

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// blockedResponseCodec 用屏障模拟对象下载；late 场景故意迟到返回，验证终态不能被覆盖
type blockedResponseCodec struct {
	// entered 确认接收者已进入可能阻塞的业务解码
	entered chan struct{}
	// release 由测试控制迟到下载返回，不使用固定 sleep 排序
	release chan struct{}
	// late 为 true 时模拟下载已结束但取消仍与返回竞争
	late bool
}

// Encode 冻结输入，生成真实的 codec envelope 而非伪造最终响应
func (c *blockedResponseCodec) Encode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return bytes.Clone(data), nil
}

// Decode 等待显式屏障或阶段预算，网络实现必须遵守同样的 context 契约
func (c *blockedResponseCodec) Decode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	close(c.entered)
	if c.late {
		<-c.release
		return bytes.Clone(data), nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestClientStreamResponseDecodeDoesNotBlockStop 验证解码阶段预算与关闭可独立推进
// 下载迟到返回不能把 Canceled 改为成功，也不能提前标记 responseRead
func TestClientStreamResponseDecodeDoesNotBlockStop(t *testing.T) {
	for _, scenario := range []string{"cancel", "timeout", "late_success"} {
		t.Run(scenario, func(t *testing.T) {
			codec := &blockedResponseCodec{entered: make(chan struct{}), release: make(chan struct{}), late: scenario == "late_success"}
			// releaseOnce 保证失败清理也能释放迟到 codec，避免测试自身遗留 goroutine
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(codec.release) }) })
			config := spec.Defaults()
			config.Middleware = []middleware.Option{middleware.WithPayload(codec)}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			options := config.StreamOptionsFor("/fixture/Upload")
			options.DecodeTimeout = 50 * time.Millisecond
			envelope, err := wire.Encode(ctx, "/fixture/Upload", &pb.Reply{Message: "response"}, nil, config.Middleware, options.MaxMessageBytes)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			callCtx, callCancel := context.WithCancel(ctx)
			defer callCancel()
			s := &taskClientStream{ctx: callCtx, cancel: callCancel, engine: &engine.Engine{Config: config}, desc: &grpc.StreamDesc{ClientStreams: true}, method: "/fixture/Upload", outputType: (&pb.Reply{}).ProtoReflect().Descriptor().FullName(), options: options, changed: make(chan struct{}), inputClosed: make(chan struct{}), resultDone: true, result: &ports.Result{Outputs: map[string]any{"task": envelope}}, complete: func(error) { close(done) }}
			close(s.inputClosed)
			received := make(chan error, 1)
			go func() { received <- s.RecvMsg(&pb.Reply{}) }()
			select {
			case <-codec.entered:
			case <-ctx.Done():
				t.Fatal("decoder did not enter")
			}
			want := codes.DeadlineExceeded
			if scenario != "timeout" {
				want = codes.Canceled
				go s.finish(status.Error(codes.Canceled, "fixture stop"))
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("codec held stream state lock during Stop")
				}
				if scenario == "late_success" {
					releaseOnce.Do(func() { close(codec.release) })
				}
			}
			select {
			case err := <-received:
				if status.Code(err) != want {
					t.Fatalf("got %v want %s", err, want)
				}
			case <-ctx.Done():
				t.Fatal("receive did not finish")
			}
			<-done
			s.mu.Lock()
			read := s.responseRead
			s.mu.Unlock()
			if read {
				t.Fatal("failed or late decode advanced response state")
			}
		})
	}
}
