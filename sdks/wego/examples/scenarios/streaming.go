package scenarios

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// 每个流片段以 protobuf 编码，再通过引擎的 string 事件接口传输
func Streaming(ctx context.Context, report *Report) (err error) {
	// content 保存此作用域的初始配置或查找表，后续调用使用相同值保持注册与执行一致
	const content = "Happy families are all alike; every unhappy family is unhappy in its own way."
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(700 * time.Millisecond):
			}
			// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
			for pos, index := 0, int32(0); pos < len(content); pos, index = pos+10, index+1 {
				// end 当前文本分块的结束偏移，最多推进 10 个字节，尾块不超过内容长度
				end := min(pos+10, len(content))
				// data, e 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
				data, e := proto.Marshal(&pb.Reply{Message: content[pos:end], Count: index})
				if e != nil {
					return nil, e
				}
				if e = task.StreamEvent(ctx, []byte(base64.StdEncoding.EncodeToString(data))); e != nil {
					return nil, e
				}

				// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(25 * time.Millisecond):
				}
			}
			return Reply(ctx, &pb.Request{Message: "Streaming completed"}), nil
		},
	}
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	h, err := Start(ctx, "streaming", report, service, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// ref, err 接收 h.Conn.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ref, err := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, routedInput(&pb.Request{}))
	if err != nil {
		return err
	}

	// streamCtx, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()

	// messages, streamErrors 通过明确 RunID 查询或管理执行记录，结果必须来自实际引擎
	messages, streamErrors := h.Conn.Runs().SubscribeToStream(streamCtx, ref.RunID)
	// fixture 将实际引擎输出逐块转发为 HTTP 响应，便于验证输出顺序与结束状态
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		// 逐项处理 messages，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for value := range messages {
			// data 是订阅出口中还原的业务 protobuf 字节，解码失败不能继续转发 HTTP 输出
			data, e := base64.StdEncoding.DecodeString(value)
			// chunk 逐条解码的流输出，检查内容与消息数量而不是只确认订阅成功
			var chunk pb.Reply
			if e == nil {
				e = proto.Unmarshal(data, &chunk)
			}
			if e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			if _, e = io.WriteString(w, chunk.Message); e != nil {
				return
			}

			w.(http.Flusher).Flush()
		}
	}))
	defer fixture.Close()

	// req, _ 将请求绑定到调用预算，取消后 HTTP I/O 必须结束
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fixture.URL, nil)
	// response, err 接收 http.DefaultClient.Do 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	// completed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	completed := make(chan error, 1)
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out pb.Reply
	go func() {
		// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
		result, e := ref.Result(ctx)
		if e == nil {
			e = result.Into(&out)
		}
		// 最终运行结果确认任务完成；事件订阅独立存在，消费完全部片段后显式取消
		completed <- e
	}()
	// buffer 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
	buffer := make([]byte, len(content))
	if _, err = io.ReadFull(response.Body, buffer); err != nil {
		return err
	}
	if string(buffer) != content {
		return fmt.Errorf("stream chunk order or HTTP body mismatch")
	}
	if err = <-completed; err != nil {
		return err
	}
	if out.Message != "Streaming completed" {
		return fmt.Errorf("final stream result: %v", &out)
	}

	stop()
	// 逐项处理 streamErrors，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for e := range streamErrors {
		if e != nil && !errors.Is(e, context.Canceled) && !strings.Contains(e.Error(), "Canceled") {
			return e
		}
	}
	report.Add(h, "protobuf output chunks forwarded to HTTP in order and final result confirmed", &out)
	return nil
}
