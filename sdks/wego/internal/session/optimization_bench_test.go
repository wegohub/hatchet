package session

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// benchmarkPayload 强制保留复制结果，避免编译器把未使用的缓冲分配消除。
var benchmarkPayload []byte

// BenchmarkEndpointInput 测量真实 DATA 校验、窗口判断及独立缓存复制；不包含网络与调度。
// 每次接受 256 字节后模拟顺序消费，下一条继续使用同样的窗口，初始化不计时。
func BenchmarkEndpointInput(b *testing.B) {
	endpoint := newEndpoint("benchmark", "method", wire.Envelope{}, nil, spec.Defaults())
	endpoint.open = true
	frame := &wire.Frame{Kind: "DATA", Direction: "input", Payload: make([]byte, 256)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame.Seq++
		if err := endpoint.control(context.Background(), frame); err != nil {
			b.Fatal(err)
		}
		// 消费仍遵守端点锁；这里只隔离控制处理，不编码或发布累计 ACK。
		endpoint.mu.Lock()
		delete(endpoint.incoming, frame.Seq)
		endpoint.consumed = frame.Seq
		endpoint.inputBytes = 0
		endpoint.mu.Unlock()
	}
}

// BenchmarkPayloadCopy 对比 append 与精确容量复制，不能把切片容量减少等同于堆分配字节减少。
func BenchmarkPayloadCopy(b *testing.B) {
	// size 同时覆盖分配器取整附近的短消息和较长消息。
	for _, size := range []int{253, 4097} {
		// payload 在计时之前创建，两种复制路径处理同一批字节。
		payload := make([]byte, size)
		b.Run(fmt.Sprintf("append/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			// i 使用独立目标执行复制，不把源创建成本纳入采样。
			for i := 0; i < b.N; i++ {
				benchmarkPayload = append([]byte(nil), payload...)
			}
		})
		b.Run(fmt.Sprintf("exact/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			// i 调用实际缓存复制方法，结果保留到下一次迭代。
			for i := 0; i < b.N; i++ {
				benchmarkPayload = copyPayload(payload)
			}
		})
	}
}

// BenchmarkEndpointACK 模拟满窗口持续发送、每次确认一条的控制热路径。
// 例如窗口 64 时，ACK=1 只释放第 1 条，随即补入第 65 条；初始化不计入采样。
func BenchmarkEndpointACK(b *testing.B) {
	// window 同时覆盖默认窗口与较大窗口，观察每条 ACK 是否仍扫描全部在途消息。
	for _, window := range []int{64, 1024} {
		b.Run(fmt.Sprint(window), func(b *testing.B) {
			// config 使用相同窗口配置，排除传输、序列化和真实引擎调度成本。
			config := spec.Defaults()
			config.Stream.Window = window
			// endpoint 保留真实 control 路径，初始未确认消息长度均为 256 字节。
			endpoint := newEndpoint("benchmark", "method", wire.Envelope{}, nil, config)
			endpoint.outputSeq = uint64(window)
			endpoint.outputBytes = window * 256
			// seq 逐项建立满窗口；此步骤发生在计时开始前。
			for seq := uint64(1); seq <= uint64(window); seq++ {
				endpoint.outgoing[seq] = 256
			}
			// frame 重用 ACK 结构，避免把调用者创建帧的成本计入控制处理。
			frame := &wire.Frame{Kind: "ACK", Direction: "output"}
			b.ReportAllocs()
			b.ResetTimer()
			// i 每次推进一个序号并补充一条，整个采样保持同样大小的窗口。
			for i := 0; i < b.N; i++ {
				frame.Ack++
				// err 表示真实 ACK 校验结果；非法状态不能作为成功采样。
				if err := endpoint.control(context.Background(), frame); err != nil {
					b.Fatal(err)
				}
				endpoint.outputSeq++
				endpoint.outgoing[endpoint.outputSeq] = 256
				endpoint.outputBytes += 256
			}
		})
	}
}

// BenchmarkEndpointNotify 分别测量无等待者与已有等待者的广播成本。
// 等待者取得通知必须与状态检查处于同一把锁内；这里检查广播关闭当前代通道。
func BenchmarkEndpointNotify(b *testing.B) {
	// waiting 区分空闲控制消息和实际需要唤醒业务的场景。
	for _, waiting := range []bool{false, true} {
		b.Run(fmt.Sprint(waiting), func(b *testing.B) {
			// endpoint 使用真实通知实现，构建成本不纳入热路径结果。
			endpoint := newEndpoint("benchmark", "method", wire.Envelope{}, nil, spec.Defaults())
			b.ReportAllocs()
			b.ResetTimer()
			// i 逐次广播；有等待者时检查它持有的通道已关闭。
			for i := 0; i < b.N; i++ {
				endpoint.mu.Lock()
				// changed 保存本次等待代；懒创建实现仅在存在等待者时分配它。
				var changed chan struct{}
				if waiting {
					if endpoint.changed == nil {
						endpoint.changed = make(chan struct{})
					}
					changed = endpoint.changed
				}
				endpoint.notify()
				endpoint.mu.Unlock()
				if waiting {
					select {
					case <-changed:
					default:
						b.Fatal("broadcast did not wake registered waiter")
					}
				}
			}
		})
	}
}

// BenchmarkSessionDecode 对比直接 JSON 字节与对象回编码路径，分别记录分配数与耗时。
func BenchmarkSessionDecode(b *testing.B) {
	// value 使用相同的控制确认 JSON；源类型不同但目标内容必须一致。
	for name, value := range map[string]any{
		"string": `{"owner":"instance","consumed":12}`,
		"raw":    json.RawMessage(`{"owner":"instance","consumed":12}`),
		"object": map[string]any{"owner": "instance", "consumed": 12},
	} {
		b.Run(name, func(b *testing.B) {
			// target 重用目标对象，测量解码入口而非调用者构建结构体的成本。
			var target Acknowledgment
			b.ReportAllocs()
			// i 重复实际解码，保留失败检查以拒绝错误的优化结果。
			for i := 0; i < b.N; i++ {
				// err 确认各输入类型都能解析为相同业务确认。
				if err := Decode(value, &target); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
