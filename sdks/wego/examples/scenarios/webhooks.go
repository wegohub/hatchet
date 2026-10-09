package scenarios

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// webhookInput 受控 Webhook 请求结构，复现来源协议的必要字段
type webhookInput struct {
	// EventKey 事件路由键
	EventKey string `json:"event_key"`
	// Type 事件或 protobuf 类型名称，用于分派及类型校验
	Type string `json:"type"`
	// Data 对应 JSON 字段 customer，用于将受控请求或验收数据解码到此结构
	Data struct {
		// Object 对应 JSON 字段 customer，用于将受控请求或验收数据解码到此结构
		Object struct {
			// Customer 对应 JSON 字段 customer，用于将受控请求或验收数据解码到此结构
			Customer string `json:"customer"`
			// Amount 对应 JSON 字段 amount，用于将受控请求或验收数据解码到此结构
			Amount int `json:"amount"`
		} `json:"object"`
	} `json:"data"`
	// PullRequest 对应 JSON 字段 number，用于将受控请求或验收数据解码到此结构
	PullRequest struct {
		// Number 对应 JSON 字段 number，用于将受控请求或验收数据解码到此结构
		Number int `json:"number"`
		// Title 对应 JSON 字段 title，用于将受控请求或验收数据解码到此结构
		Title string `json:"title"`
	} `json:"pull_request"`
	// Repository 对应 JSON 字段 full_name，用于将受控请求或验收数据解码到此结构
	Repository struct {
		// FullName 完整名称；RPC 绑定时必须与生成客户端调用的方法名一致
		FullName string `json:"full_name"`
	} `json:"repository"`
	// Event 对应 JSON 字段 event，用于将受控请求或验收数据解码到此结构
	Event struct {
		// Type, User, Text, Channel 是受控消息事件的类型、发送者、文本和目标 channel，用于核对 Webhook 解析
		Type, User, Text, Channel string
	} `json:"event"`
	// Command 对应 JSON 字段 command，用于将受控请求或验收数据解码到此结构
	Command string `json:"command"`
	// Text 对应 JSON 字段 text，用于将受控请求或验收数据解码到此结构
	Text string `json:"text"`
	// Actions 注册的动作列表或 Webhook 动作数据
	Actions []struct {
		// ActionID 对应 JSON 字段 action_id，用于将受控请求或验收数据解码到此结构
		ActionID string `json:"action_id"`
	} `json:"actions"`
}

// Webhooks 使用受控 HTTP fixture 验证来源认证、事件生成与触发 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Webhooks(ctx context.Context, report *Report) (err error) {
	// h 保存本场景的连接、定义和资源名称，失败路径同样负责关闭与清理
	h := &Harness{
		Scenario:  "webhooks",
		Namespace: fmt.Sprintf("wego_accept_webhooks_%d_", time.Now().UnixNano()),
		Report:    report,
	}
	h.Conn, err = client.New(client.WithRuntime(Runtime(h.Namespace)...))
	if err != nil {
		return err
	}

	// called 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	called := make(chan *pb.Reply, 8)
	// keys 是五种受控 Webhook 生成的事件键，分别覆盖支付、代码事件及消息交互
	keys := []string{
		"stripe:payment_intent.succeeded",
		"github:pull_request:opened",
		"slack:event:app_mention",
		"slack:command:/deploy",
		"slack:interaction:block_actions",
	}
	// definitions 交给 Worker 注册的任务定义集合
	definitions := []client.Definition{}
	// 逐项处理 keys，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i, key := range keys {
		// name 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
		name := fmt.Sprintf("hook-%d", i)
		h.Names = append(h.Names, name)
		definitions = append(definitions, h.Conn.NewStandaloneTask(name, func(ctx context.Context, in webhookInput) (map[string]any, error) {
			// out 当前结果的按键映射，逐项写入转换后的输出；不会把后端对象直接放入公开返回值
			out := map[string]any{}
			// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
			switch i {
			case 0:
				out = map[string]any{"customer": in.Data.Object.Customer, "amount": in.Data.Object.Amount}
				if in.Data.Object.Amount != 42 || in.Data.Object.Customer != "fixture-account" {
					return nil, fmt.Errorf("payment payload mismatch")
				}
			case 1:
				out = map[string]any{"repo": in.Repository.FullName, "pr": in.PullRequest.Number}
				if in.PullRequest.Number != 7 || in.Repository.FullName != "fixture/repository" {
					return nil, fmt.Errorf("pull request payload mismatch")
				}
			case 2:
				out["handled"] = in.Event.Type == "app_mention"
				if out["handled"] != true {
					return nil, fmt.Errorf("mention payload mismatch")
				}
			case 3:
				out = map[string]any{"command": in.Command, "args": in.Text}
				if in.Command != "/deploy" || in.Text != "fixture" {
					return nil, fmt.Errorf("command payload mismatch")
				}
			case 4:
				if len(in.Actions) != 1 || in.Actions[0].ActionID != "approve" {
					return nil, fmt.Errorf("interaction payload mismatch")
				}
				out["action"] = in.Actions[0].ActionID
			}
			called <- Reply(ctx, &pb.Request{Message: key})
			return out, nil
		}, task.WithEvents(key)))
	}
	h.NativeWorker, err = h.Conn.NewWorker(h.Namespace+"worker", client.WithWorkflows(definitions...))
	if err != nil {
		_ = h.Conn.Close()
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	if _, err = h.NativeWorker.Start(); err != nil {
		return err
	}
	if err = h.NativeWorker.WaitReady(ctx); err != nil {
		return err
	}

	// secret 受控验签 fixture 的随机字节密钥，不写入测试报告
	var secret [16]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return err
	}

	// key 把原始字节转换为传输或诊断文本，不改变字节内容
	key := hex.EncodeToString(secret[:])
	// name 注册或查询时使用的名称；必须与提交任务的名称对应
	name := h.Namespace + "incoming"
	_, err = h.Conn.Webhooks().Create(ctx, model.CreateWebhookOpts{
		Name:                         name,
		SourceName:                   model.Generic,
		EventKeyExpression:           "input.event_key",
		ReturnEventAsResponsePayload: pointer(true),
		Auth:                         model.APIKeyAuth{HeaderName: "X-Api-Key", APIKey: key},
	})
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Conn.Webhooks().Delete(context.Background(), name))
	}()

	if _, err = h.Conn.Webhooks().Get(ctx, name); err != nil {
		return err
	}
	if _, err = h.Conn.Webhooks().List(ctx, model.Query{}); err != nil {
		return err
	}

	// info, err 接收 h.Conn.Info 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	info, err := h.Conn.Info(ctx)
	if err != nil {
		return err
	}

	// fixtures 是与 keys 一一对应的受控请求；例如金额 42 用于核对 JSON 字段解析，没有生产业务数据
	fixtures := []map[string]any{
		{
			"type": "payment_intent.succeeded",
			"data": map[string]any{"object": map[string]any{"customer": "fixture-account", "amount": 42}},
		},
		{
			"action":       "opened",
			"pull_request": map[string]any{"number": 7, "title": "fixture"},
			"repository":   map[string]any{"full_name": "fixture/repository"},
		},
		{
			"event": map[string]any{
				"type":    "app_mention",
				"user":    "fixture-user",
				"channel": "fixture-channel",
				"text":    "fixture",
			},
		},
		{"command": "/deploy", "text": "fixture", "user_name": "fixture-user"},
		{
			"type":    "block_actions",
			"actions": []any{map[string]any{"action_id": "approve"}},
			"user":    map[string]any{"username": "fixture-user"},
		},
	}
	// 逐项处理 fixtures，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i, fixture := range fixtures {
		fixture["event_key"] = h.Namespace + keys[i]
		// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
		data, _ := json.Marshal(fixture)
		// req, e 将请求绑定到调用预算，取消后 HTTP I/O 必须结束
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, info.APIURL+"/api/v1/stable/tenants/"+info.TenantID+"/webhooks/"+name, bytes.NewReader(data))
		if e != nil {
			return e
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Api-Key", key)
		// response 来自受控 HTTP 请求，状态和响应体共同验证业务交互，退出前必须关闭 Body
		response, e := http.DefaultClient.Do(req)
		if e != nil {
			return e
		}

		// body, e 读取受控 HTTP 响应字节，用于比较转发数据及顺序
		body, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if e != nil {
			return e
		}
		if response.StatusCode != 200 {
			return fmt.Errorf("webhook fixture %d HTTP %d: %s", i, response.StatusCode, body)
		}
		if !bytes.Contains(body, []byte(keys[i])) {
			return fmt.Errorf("webhook did not return its event")
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
		case out := <-called:
			if out.Message != keys[i] {
				return fmt.Errorf("webhook event routed incorrectly")
			}
			if err = waitStatus(ctx, h, out.RunId, model.Completed); err != nil {
				return err
			}
			report.Add(h, "controlled HTTP webhook "+keys[i]+" payload and result", out)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
