package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
)

// Basic 验证生成桩、异步、批量、元数据、错误详情、截止时间和取消。
func Basic(ctx context.Context, report *Report) (err error) {
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			if in.Fail {
				_ = grpc.SetHeader(ctx, metadata.Pairs("worker", "ready"))
				grpc.SetTrailer(ctx, metadata.Pairs("result", "failed"))
				// st, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details。
				st, _ := status.New(codes.InvalidArgument, "invalid input").WithDetails(&errdetails.BadRequest{})
				return nil, st.Err()
			}
			if in.DelayMillis > 0 {
				// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(in.DelayMillis) * time.Millisecond):
				}
			}
			// md 是 handler 实际收到的多值 metadata，例如 x-tag 的两项不能被合并。
			md, _ := metadata.FromIncomingContext(ctx)
			if in.Message == "metadata" && len(md.Get("request-key")) != 1 {
				return nil, fmt.Errorf("metadata missing")
			}

			_ = grpc.SetHeader(ctx, metadata.Pairs("worker", "ready"))
			grpc.SetTrailer(ctx, metadata.Pairs("result", "complete"))
			return Reply(ctx, in), nil
		},
	}
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	h, err := Start(ctx, "basic", report, service, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// headers, trailers 响应头缓存；调用方读取时返回副本。 响应尾部 metadata，随最终状态交付。
	var headers, trailers metadata.MD
	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	out, err := h.RPC.SayHello(
		metadata.AppendToOutgoingContext(ctx, "request-key", "one"),
		&pb.Request{Message: "metadata"},
		grpc.Header(&headers),
		grpc.Trailer(&trailers),
	)
	if err != nil {
		return err
	}
	if err = expect(
		out.Message == "metadata" && out.RunId != "" && out.WorkerId != "" && len(headers.Get("worker")) == 1 && len(trailers.Get("result")) == 1,
		"protobuf, metadata, headers and trailers",
	); err != nil {
		return err
	}

	report.Add(h, "standard generated unary stub", out)
	// ref, err 接收 h.Conn.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	ref, err := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Message: "async"})
	if err != nil {
		return err
	}

	// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
	result, err := ref.Result(ctx)
	if err != nil {
		return err
	}

	// async 异步运行等待后的 protobuf 响应，用于与同步结果比较。
	var async pb.Reply
	if err = result.Into(&async); err != nil {
		return err
	}
	if async.Message != "async" {
		return fmt.Errorf("async payload")
	}

	report.Add(h, "RunNoWait and result decoding", &async)
	// refs, err 接收 h.Conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	refs, err := h.Conn.RunMany(
		ctx,
		pb.UnaryGreeter_SayHello_FullMethodName,
		[]client.RunManyInput{{Input: &pb.Request{Message: "one"}}, {Input: &pb.Request{Message: "two"}}},
	)
	if err != nil {
		return err
	}

	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range refs {
		// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
		result, err := refs[i].Result(ctx)
		if err != nil {
			return err
		}

		// value 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var value pb.Reply
		if err = result.Into(&value); err != nil {
			return err
		}
		if value.Message != []string{"one", "two"}[i] {
			return fmt.Errorf("RunMany order")
		}

		report.Add(h, "RunMany input/result mapping", &value)
	}
	_, err = h.RPC.SayHello(ctx, &pb.Request{Fail: true}, grpc.Header(&headers), grpc.Trailer(&trailers))
	if status.Code(err) != codes.InvalidArgument || len(status.Convert(err).Details()) != 1 || len(headers.Get("worker")) != 1 || len(trailers.Get("result")) != 1 || trailers.Get("result")[0] != "failed" {
		return fmt.Errorf("status details: %v", err)
	}

	report.Add(h, "gRPC status details and error headers/trailers", nil)
	// deadline, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	deadline, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = h.RPC.SayHello(deadline, &pb.Request{DelayMillis: 5000})
	stop()
	if status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("deadline: %v", err)
	}

	report.Add(h, "deadline cancellation", nil)
	// cancelled, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	cancelled, stop := context.WithCancel(ctx)
	stop()
	_, err = h.RPC.SayHello(cancelled, &pb.Request{})
	if status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("cancellation: %v", err)
	}

	report.Add(h, "explicit cancellation", nil)
	return nil
}
