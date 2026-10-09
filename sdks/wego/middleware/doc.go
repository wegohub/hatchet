// Package middleware 提供标准 gRPC 拦截器和载荷变换
// 持久流 codec 处理完整帧，并在恢复解码时检查预算；调度 routing 独立保持 JSON 可读
package middleware
