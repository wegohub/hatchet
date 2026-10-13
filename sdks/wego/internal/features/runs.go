package features

import (
	"context"
	"fmt"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// RunsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type RunsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// SubscribeToStream 分别保留消息和订阅错误；调用方取消上下文后两个通道都会关闭
func (c *RunsClient) SubscribeToStream(ctx context.Context, id string) (<-chan string, <-chan error) {
	// messages 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	messages := make(chan string, 1)
	// errors 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	errors := make(chan error, 1)
	// 订阅及回调解码共用实例可取消预算，强制关闭不遗留对象下载。
	ctx, done, beginErr := c.begin(ctx)
	if beginErr != nil {
		errors <- beginErr
		close(messages)
		close(errors)
		return messages, errors
	}
	go func() {
		defer done()
		defer close(messages)
		defer close(errors)

		// 发布模式来自持久输入；不能根据 chunk 外形猜测原生或协议消息。
		reader, ok := c.backend.(ports.RunInputReader)
		if !ok {
			errors <- fmt.Errorf("wego: stored stream response contract unavailable")
			return
		}
		// 订阅与契约读取并行，避免慢 REST 查询使实时输出在订阅建立前全部丢失。
		lifetime, cancel := context.WithCancel(ctx)
		defer cancel()
		ready := make(chan struct{})
		// packet 在 ready 关闭后才读取，关闭 channel 建立内存可见性。
		var packet wire.Envelope
		// contractErr 保存持久输入的解析失败，不被订阅取消错误覆盖。
		var contractErr error
		go func() {
			defer close(ready)
			input, err := reader.RunInput(lifetime, id)
			if err == nil {
				packet, err = wire.AsEnvelope(input)
			}
			contractErr = err
			if err != nil {
				cancel()
			}
		}()
		defer func() { cancel(); <-ready }()
		// err 接收 c.backend.Stream 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		err := c.backend.Stream(lifetime, id, func(payload string) error {
			select {
			case <-ready:
			case <-lifetime.Done():
				return lifetime.Err()
			}
			if contractErr != nil {
				return contractErr
			}
			if packet.Response == "wego" {
				decoded, err := wire.DecodeNativeStream(ctx, c.config, payload)
				if err != nil {
					return err
				}
				payload = string(decoded)
			}
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
			select {
			case messages <- payload:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		cancel()
		<-ready
		if contractErr != nil {
			err = contractErr
		}
		if err != nil {
			errors <- err
		}
	}()
	return messages, errors
}

// Get 读取运行，通过共享后端调用 Runs.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) Get(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsGet{ID: id})
}

// GetStatus 查询状态运行，通过共享后端调用 Runs.GetStatus；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) GetStatus(ctx context.Context, id string) (model.RunStatus, error) {
	// value 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var value model.RunStatus
	// err 接收 c.backend.Feature 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	err := c.backend.Feature(ctx, ports.RunsGetStatus{ID: id}, &value)
	return value, err
}

// GetDetails 读取详情运行，通过共享后端调用 Runs.GetDetails；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) GetDetails(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsGetDetails{ID: id})
}

// List 查询列表运行，通过共享后端调用 Runs.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsList{Query: query})
}

// Replay 重新执行运行，通过共享后端调用 Runs.Replay；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) Replay(ctx context.Context, request model.Resource) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsReplay{Request: request})
}

// Cancel 取消运行，通过共享后端调用 Runs.Cancel；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) Cancel(ctx context.Context, request model.Resource) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsCancel{Request: request})
}

// Restore 恢复运行，通过共享后端调用 Runs.Restore；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RunsClient) Restore(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.RunsRestore{ID: id})
}
