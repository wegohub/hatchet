package tidb

import (
	"testing"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestStatusOrdering(t *testing.T) {
	statuses := []sqlcv1.V1ReadableStatusOlap{"QUEUED", "RUNNING", "EVICTED", "CANCELLED", "FAILED", "COMPLETED"}
	for i, old := range statuses {
		for j, next := range statuses {
			if !shouldUpdateStatus(old, 2, next, 3) {
				t.Errorf("new retry %s -> %s was rejected", old, next)
			}
			if shouldUpdateStatus(old, 2, next, 1) {
				t.Errorf("stale retry %s -> %s was accepted", old, next)
			}
			want := j > i || old == "EVICTED" && next != "EVICTED"
			if got := shouldUpdateStatus(old, 2, next, 2); got != want {
				t.Errorf("same retry %s -> %s: %v, want %v", old, next, got, want)
			}
		}
	}
}

func TestDAGRollup(t *testing.T) {
	cases := []struct {
		name    string
		current sqlcv1.V1ReadableStatusOlap
		total   int
		tasks   []sqlcv1.V1ReadableStatusOlap
		want    sqlcv1.V1ReadableStatusOlap
	}{
		{"no_tasks", "QUEUED", 2, nil, "QUEUED"},
		{"queued_partial", "QUEUED", 2, []sqlcv1.V1ReadableStatusOlap{"QUEUED"}, "QUEUED"},
		{"queued_preserves_outcome", "FAILED", 2, []sqlcv1.V1ReadableStatusOlap{"QUEUED", "QUEUED"}, "FAILED"},
		{"terminal_partial", "QUEUED", 2, []sqlcv1.V1ReadableStatusOlap{"COMPLETED"}, "RUNNING"},
		{"running_before_failure", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"FAILED", "RUNNING"}, "RUNNING"},
		{"all_evicted", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"EVICTED", "EVICTED"}, "EVICTED"},
		{"failure_before_cancel", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"FAILED", "CANCELLED"}, "FAILED"},
		{"cancel_before_complete", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"CANCELLED", "COMPLETED"}, "CANCELLED"},
		{"all_complete", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"COMPLETED", "COMPLETED"}, "COMPLETED"},
		{"mixed_eviction", "RUNNING", 2, []sqlcv1.V1ReadableStatusOlap{"EVICTED", "COMPLETED"}, "RUNNING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rollupDAGStatus(tc.current, tc.total, tc.tasks); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
