package backend

import (
	"slices"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// TestEventRegistrationDeduplicatesFinalKey 未加 namespace 和已加前缀的同一事件只注册一次。
func TestEventRegistrationDeduplicatesFinalKey(t *testing.T) {
	policy := spec.Task{Events: []string{"/fixture.Service/Call", "other", "fixture_/fixture.Service/Call", "other"}}
	request, err := workflowDefinition("fixture", policy, "fixture_")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(request.EventTriggers, []string{"fixture_/fixture.Service/Call", "fixture_other"}) {
		t.Fatal("event order or deduplication differs", request.EventTriggers)
	}
	if len(policy.Events) != 4 || policy.Events[0] != "/fixture.Service/Call" {
		t.Fatal("registration mutated caller policy")
	}
}
