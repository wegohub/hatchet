package payload

import "github.com/hatchet-dev/hatchet/sdks/wego/middleware"

var (
	// gzip 的发送阈值和有界解码由统一注册入口读取。
	_ middleware.ThresholdPayload = (*Gzip)(nil)
	// zstd 的发送阈值和有界解码由统一注册入口读取。
	_ middleware.ThresholdPayload = (*Zstd)(nil)
	// AES-GCM 每条消息都执行加密，无发送阈值。
	_ middleware.Payload = (*AESGCM)(nil)
	// S3 卸载在达到当前步骤阈值时上传对象。
	_ middleware.ThresholdPayload = (*Objects)(nil)
)
