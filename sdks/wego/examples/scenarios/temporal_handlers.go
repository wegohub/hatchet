package scenarios

import (
	"context"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
)

// Charge 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Charge(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Charge")
}

// Validate 检查配置组合的有效性，在资源启动前拒绝不合法的容量和预算。
func (s *temporalService) Validate(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Validate")
}

// Fulfill 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Fulfill(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Fulfill")
}

// Process 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Process(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Process")
}

// Welcome 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Welcome(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Welcome")
}

// Followup 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Followup(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Followup")
}

// Onboard 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Onboard(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Onboard")
}

// ChargeWithRetries 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) ChargeWithRetries(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "ChargeWithRetries")
}

// Digest 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Digest(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Digest")
}

// Approval 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Approval(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Approval")
}

// Item 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Item(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Item")
}

// ShipItems 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) ShipItems(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "ShipItems")
}

// Report 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) Report(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "Report")
}

// SyncRecords 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) SyncRecords(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "SyncRecords")
}

// CallModel 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) CallModel(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "CallModel")
}

// LoggedCharge 执行 s.execute 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (s *temporalService) LoggedCharge(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	return s.execute(ctx, in, "LoggedCharge")
}
