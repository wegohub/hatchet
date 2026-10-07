package session

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
)

// TestInvalidMessagesDoNotAdvanceWindows 验证发送和接收使用同一校验，错误不改变缓存与序号。
// 接收缓冲已存在合法的第 1 条响应，非法对象和 (*Reply)(nil) 都不能提前消费或确认它。
func TestInvalidMessagesDoNotAdvanceWindows(t *testing.T) {
	// 请求对象由标准生成器提供；nil 指针满足 proto.Message，必须与合法空消息区分。
	var nilReply *pb.Reply
	for _, value := range []any{nil, "text", nilReply} {
		c := receivingClient(t)
		payload, err := proto.Marshal(&pb.Reply{Count: 7})
		if err != nil {
			t.Fatal(err)
		}
		c.messages[1], c.inputBytes = payload, len(payload)
		for _, operation := range []func(any) error{c.SendMsg, c.RecvMsg} {
			if err := operation(value); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid %T: %v", value, err)
			}
		}
		if c.sent != 0 || c.consumed != 0 || len(c.messages) != 1 || c.inputBytes != len(payload) || c.ctx.Err() != nil {
			t.Fatal("invalid message changed window or stream lifetime")
		}
	}
}

// TestCallFailureRetainsFirstStatus 验证统一错误出口取消本地等待，迟到故障不能覆盖已有状态。
func TestCallFailureRetainsFirstStatus(t *testing.T) {
	c := receivingClient(t)
	first := status.Error(codes.DataLoss, "missing output")
	if err := c.failCall(first); err != first || c.ctx.Err() != context.Canceled {
		t.Fatalf("initial failure: %v; cancellation: %v", err, c.ctx.Err())
	}
	if err := c.failCall(context.Canceled); err != first {
		t.Fatalf("cleanup overwrote status: %v", err)
	}
}
