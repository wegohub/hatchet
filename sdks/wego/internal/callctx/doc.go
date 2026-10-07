// Package callctx 将执行能力附加到标准 context.Context。
// 子调用派生上下文时共享执行身份，但不得修改父调用的取消和截止时间。
package callctx
