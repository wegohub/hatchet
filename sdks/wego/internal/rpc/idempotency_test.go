package rpc

import (
	"strings"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// TestStreamIdempotencyReservation 三种流始终使用 SDK 的 TTL 策略，拒绝业务策略覆盖提交身份。
func TestStreamIdempotencyReservation(t *testing.T) {
	s, _, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	methods, err := binding.Methods(&pb.Greeter_ServiceDesc, &streamFixtureService{})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		if method.Stream == nil {
			continue
		}
		t.Run(string(method.Descriptor.Name()), func(t *testing.T) {
			definition, err := StreamDefinition(s.engine, method, spec.Task{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			actual := definition.Policy.Idempotency
			if !actual.Set || actual.Value.Method != model.IdempotencyMethodTTL || actual.Value.TTL != 24*time.Hour ||
				!strings.Contains(actual.Value.Expression, "input._wego.idempotency_key") {
				t.Fatal("stream submission policy changed", actual)
			}
			// 即使业务配置与发送期限一致，也不能替换内部表达式或切换为 STATUS。
			for _, configured := range []model.Idempotency{
				{},
				{Expression: "input.routing.id", TTL: 24 * time.Hour},
				{Expression: "input.routing.id", Method: model.IdempotencyMethodStatus, TTL: time.Minute},
			} {
				policy := spec.Task{Idempotency: option.Some(configured)}
				if _, err := StreamDefinition(s.engine, method, policy, nil); err == nil || !strings.Contains(err.Error(), "reserves its submission idempotency") {
					t.Fatal("custom stream idempotency accepted", configured, err)
				}
			}
		})
	}
}
