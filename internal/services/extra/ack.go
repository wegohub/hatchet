package extra

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// ackIdentity 完整保留一次投递的回复地址；sequence 相同的不同投递不能互相替换 token。
type ackIdentity struct {
	// Version 固定 ACK 载体解释规则，未知版本必须拒绝。
	Version int `json:"v"`
	// Tenant 是签发时的认证租户，确认者必须属于同一租户。
	Tenant string `json:"tenant"`
	// Stream 与 Consumer 保留原生名称，并与 Subject 解析出的身份一致。
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	// Subject 是完整的原生回复地址，不允许业务自行拼接。
	Subject string `json:"subject"`
}

// signACK 对本次投递的完整原生回复地址签名，不能仅用消息序号代替投递身份。
func (s *Service) signACK(ctx context.Context, stream, consumer, subject string) ([]byte, error) {
	id, err := tenant(ctx)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(subject, "$JS.ACK.") || strings.ContainsAny(subject, " \t\r\n") {
		return nil, status.Error(codes.DataLoss, "invalid ACK subject")
	}
	body, err := json.Marshal(ackIdentity{
		Version:  1,
		Tenant:   id.String(),
		Stream:   stream,
		Consumer: consumer,
		Subject:  subject,
	})
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, s.cfg.ACKKey)
	_, _ = mac.Write(body)
	return append(body, mac.Sum(nil)...), nil
}

// verifyACK 先验证 HMAC，再核对租户和原生投递地址，拒绝篡改或跨租户 token。
func (s *Service) verifyACK(ctx context.Context, token []byte) (ackIdentity, error) {
	denied := status.Error(codes.PermissionDenied, "invalid ACK token")
	if len(token) <= sha256.Size || len(token) > 4096 {
		return ackIdentity{}, denied
	}
	body, sig := token[:len(token)-sha256.Size], token[len(token)-sha256.Size:]
	mac := hmac.New(sha256.New, s.cfg.ACKKey)
	_, _ = mac.Write(body)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return ackIdentity{}, denied
	}
	var v ackIdentity
	if json.Unmarshal(body, &v) != nil || v.Version != 1 || !strings.HasPrefix(v.Subject, "$JS.ACK.") {
		return ackIdentity{}, denied
	}
	id, err := tenant(ctx)
	if err != nil {
		return ackIdentity{}, err
	}
	if v.Tenant != id.String() {
		return ackIdentity{}, denied
	}
	// 签名载体与原生回复地址必须描述同一投递，不能让描述字段与实际寻址脱节。
	// Metadata 只解析 Reply；空订阅仅满足 nats.go 的绑定检查，不创建网络订阅。
	meta, err := (&nats.Msg{
		Reply: v.Subject,
		Sub:   &nats.Subscription{},
	}).Metadata()
	prefix := "X_" + strings.ReplaceAll(id.String(), "-", "") + "_"
	if err != nil || meta.Stream != v.Stream || meta.Consumer != v.Consumer || !strings.HasPrefix(v.Stream, prefix) {
		return ackIdentity{}, denied
	}
	return v, nil
}

// Ack 验证签名投递身份后执行确认、拒绝、续期或终止；仅 ACK 支持服务端确认。
func (s *Service) Ack(ctx context.Context, r *pb.AckRequest) (*emptypb.Empty, error) {
	if r == nil {
		return nil, invalid("ACK request required")
	}
	v, err := s.verifyACK(ctx, r.AckToken)
	if err != nil {
		return nil, err
	}
	if r.Confirm && r.Kind != pb.AckRequest_ACK {
		return nil, invalid("confirm requires ACK")
	}
	if r.NakDelayNs != nil && (r.Kind != pb.AckRequest_NAK || *r.NakDelayNs <= 0) {
		return nil, invalid("NAK delay requires positive delay and NAK")
	}
	if r.TermReason != nil && (r.Kind != pb.AckRequest_TERM || len(*r.TermReason) > 1024 || strings.ContainsAny(*r.TermReason, "\r\n")) {
		return nil, invalid("invalid TERM reason")
	}
	var body []byte
	switch r.Kind {
	case pb.AckRequest_ACK:
		body = []byte("+ACK")
	case pb.AckRequest_NAK:
		if r.NakDelayNs == nil {
			body = []byte("-NAK")
		} else {
			body = []byte(fmt.Sprintf("-NAK {%q: %d}", "delay", *r.NakDelayNs))
		}
	case pb.AckRequest_IN_PROGRESS:
		body = []byte("+WPI")
	case pb.AckRequest_TERM:
		body = []byte("+TERM")
		if r.GetTermReason() != "" {
			body = append(body, []byte(" "+r.GetTermReason())...)
		}
	default:
		return nil, invalid("invalid ACK kind")
	}
	// DoubleAck 的请求回复确认服务端已经接受 ACK；普通 ACK 只确认本地发布成功。
	if r.Confirm {
		budget, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = s.nc.RequestWithContext(budget, v.Subject, body)
	} else {
		if ctx.Err() != nil {
			return nil, rpcError(ctx.Err())
		}
		err = s.nc.Publish(v.Subject, body)
	}
	return &emptypb.Empty{}, rpcError(err)
}
