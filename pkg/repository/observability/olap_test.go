package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fakeOLAP struct {
	repository.OLAPRepository
	result  *repository.StatusUpdateResult
	blocked map[uuid.UUID]struct{}
	err     error
}

func (f *fakeOLAP) CreateTasks(context.Context, uuid.UUID, []*repository.V1TaskWithPayload) (*repository.StatusUpdateResult, map[uuid.UUID]struct{}, error) {
	return f.result, f.blocked, f.err
}

func (f *fakeOLAP) CreateDAGs(context.Context, uuid.UUID, []*repository.DAGWithData) (map[uuid.UUID]struct{}, error) {
	return f.blocked, f.err
}

func histogramCount(t *testing.T, histogram prom.Observer) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := histogram.(prom.Metric).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestCreationMetricsExcludeFailedAndBlockedWrites(t *testing.T) {
	run := uuid.New()
	for _, tc := range []struct {
		name    string
		blocked bool
		err     error
		valid   bool
		future  bool
		count   uint64
	}{
		{name: "published", valid: true, count: 1},
		{name: "blocked", blocked: true, valid: true},
		{name: "failed", err: errors.New("write failed"), valid: true},
		{name: "missing_creation_time"},
		{name: "clock_ahead", valid: true, future: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.OLAPWriteDuration.Reset()
			metrics.OLAPCreatedToPublished.Reset()
			inner := &fakeOLAP{result: &repository.StatusUpdateResult{}, err: tc.err, blocked: map[uuid.UUID]struct{}{}}
			if tc.blocked {
				inner.blocked[run] = struct{}{}
			}
			at := time.Now().Add(-time.Second)
			if tc.future {
				at = time.Now().Add(time.Hour)
			}
			r := NewOLAP(inner, "postgres")
			result, blocked, err := r.CreateTasks(context.Background(), uuid.New(), []*repository.V1TaskWithPayload{{V1Task: &sqlcv1.V1Task{WorkflowRunID: run, InsertedAt: pgtype.Timestamptz{Time: at, Valid: tc.valid}}}})
			if result != inner.result || !errors.Is(err, tc.err) || len(blocked) != len(inner.blocked) {
				t.Fatal("wrapper changed repository result")
			}
			if count := histogramCount(t, metrics.OLAPCreatedToPublished.WithLabelValues("postgres", "task")); count != tc.count {
				t.Fatalf("published count = %d, want %d", count, tc.count)
			}
			label := "success"
			if tc.err != nil {
				label = "error"
			}
			if count := histogramCount(t, metrics.OLAPWriteDuration.WithLabelValues("postgres", "create_tasks", label)); count != 1 {
				t.Fatalf("write attempts = %d", count)
			}
		})
	}
}

func TestDAGMetricsUseExternalIDForBlockedRuns(t *testing.T) {
	metrics.OLAPCreatedToPublished.Reset()
	blocked, published := uuid.New(), uuid.New()
	inner := &fakeOLAP{blocked: map[uuid.UUID]struct{}{blocked: {}}}
	at := pgtype.Timestamptz{Time: time.Now().Add(-time.Second), Valid: true}
	r := NewOLAP(inner, "clickhouse")
	_, err := r.CreateDAGs(context.Background(), uuid.New(), []*repository.DAGWithData{
		{V1Dag: &sqlcv1.V1Dag{ExternalID: blocked, InsertedAt: at}},
		{V1Dag: &sqlcv1.V1Dag{ExternalID: published, InsertedAt: at}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if count := histogramCount(t, metrics.OLAPCreatedToPublished.WithLabelValues("clickhouse", "dag")); count != 1 {
		t.Fatalf("published DAG count = %d", count)
	}
}
