// Package support 提供示例运行器的消费能力检查和有预算的退出控制
package support

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// WaitWorkers 查询实际监听状态，不通过 SDK 内部就绪标志代替消费能力
// 例如 namespace="case-1-"，只接受当前隔离场景的 ACTIVE 注册；摘要 action 不包含可读 namespace
func WaitWorkers(ctx context.Context, conn *client.Conn, namespace string, minimum int, serve <-chan error) error {
	// ticker 周期计时器，驱动轮询或续期；当前步骤结束时停止以释放资源
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// resource, err 接收 conn.Workers 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		resource, err := conn.Workers().List(ctx)
		if err == nil {
			// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
			data, err := json.Marshal(resource)
			if err != nil {
				return err
			}
			// list Worker API 响应，包含状态与动作名称；就绪必须匹配当前 namespace
			var list struct {
				// Rows 查询返回的行集合，按状态及注册动作识别可消费的 Worker
				Rows []struct {
					// Name 用于匹配此验收唯一 namespace 下的默认实例名称
					Name string `json:"name"`
					// Status 运行或 Worker 的状态
					Status string `json:"status"`
					// Actions 注册的动作列表或 Webhook 动作数据
					Actions []string `json:"actions"`
					// Workflows 已注册工作流集合，可作为 Worker 就绪信息的补充
					Workflows []struct {
						// Name 资源或任务名称，注册与调用必须使用相同值
						Name string `json:"name"`
					} `json:"registeredWorkflows"`
				} `json:"rows"`
			}
			// err 保存 JSON 解析结果；解码失败不使用目标的零值作为有效业务数据
			if err := json.Unmarshal(data, &list); err != nil {
				return err
			}
			// count 当前步骤的初始计数 0，后续根据实际执行或数据量更新
			count := 0
			// 逐项处理 list.Rows，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
			for _, row := range list.Rows {
				if row.Status != "ACTIVE" {
					continue
				}
				// matched 当前 Worker 的注册动作是否匹配本场景前缀，用于确认实际消费就绪
				matched := strings.HasPrefix(row.Name, namespace+"-") && len(row.Actions) > 0
				// 逐项处理 row.Actions，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
				for _, action := range row.Actions {
					if strings.HasPrefix(action, namespace) {
						matched = true
						break
					}
				}
				if !matched {
					// 逐项处理 row.Workflows，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
					for _, workflow := range row.Workflows {
						if strings.HasPrefix(workflow.Name, namespace) {
							matched = true
							break
						}
					}
				}
				if matched {
					count++
				}
			}
			if count >= minimum {
				return nil
			}
		}
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		// err 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
		case err := <-serve:
			// 当前步骤失败时终止处理：server stopped before consumers became ready；不把无效结果交给下一步
			if err == nil {
				return fmt.Errorf("server stopped before consumers became ready")
			}
			return err
		case <-ctx.Done():
			return fmt.Errorf("consumer readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// StopServer 将测试预算转换为强制停止；预算耗尽仍确认资源已释放
func StopServer(ctx context.Context, srv *server.Server) error {
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		srv.Stop()
		<-done
		return ctx.Err()
	}
}
