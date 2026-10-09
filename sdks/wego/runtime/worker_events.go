package runtime

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// WorkerEventOptions 是实例通知预算，零值字段继承默认值
type WorkerEventOptions = spec.WorkerEventOptions

// WithWorkerEventOptions 配置有界回调队列、有效期和 codec 预算，不建立额外 Worker
func WithWorkerEventOptions(options WorkerEventOptions) Option {
	return func(config *spec.Runtime) { config.WorkerEvents = options }
}

// WithWorkerEventHandler 配置当前实例的业务通知回调，与任务 slots 和内部取消通道独立
func WithWorkerEventHandler(handler func(context.Context, model.WorkerEvent) error) Option {
	return func(config *spec.Runtime) { config.WorkerEventHandler = handler }
}
