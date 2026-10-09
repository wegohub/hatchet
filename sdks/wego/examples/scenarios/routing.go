package scenarios

import (
	"context"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// routingFields 是验收夹具显式声明的调度数据；SDK 不从业务消息自动投影字段
// 例如 GroupKey=one、Count=3 的本次调用配置为 routing.group=one、routing.count=3
func routingFields(request *pb.Request) map[string]any {
	return map[string]any{"group": request.GroupKey, "account": request.Account, "tier": request.Tier,
		"user_id": request.UserId, "id": request.Id, "count": request.Count}
}

// routedInput 为 Run/RunMany/触发器中的每个请求显式提供 routing
// 目标参数已给出完整 RPC 方法，因此这里省略 RPCInput.Method；多个输入不共享可变 map
func routedInput(request *pb.Request) model.RPCInput {
	return model.RPCInput{Message: request, Routing: routingFields(request)}
}

// routingFixtureInterceptor 仅统一验收夹具的调用数据，使用公开 WithRouting 为每次生成桩调用提供 map
func routingFixtureInterceptor(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if request, ok := req.(*pb.Request); ok && request != nil {
		ctx = client.WithRouting(ctx, routingFields(request))
	}
	return next(ctx, method, req, reply, conn, opts...)
}
