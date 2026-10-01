package observability

import (
	"context"
	"time"

	"github.com/google/uuid"
	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type olap struct {
	repository.OLAPRepository
	backend string
}

var _ repository.OLAPRepository = (*olap)(nil)

func NewOLAP(inner repository.OLAPRepository, backend string) repository.OLAPRepository {
	return &olap{OLAPRepository: inner, backend: backend}
}

func (r *olap) observeWrite(operation string, started time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.OLAPWriteDuration.WithLabelValues(r.backend, operation, result).Observe(time.Since(started).Seconds())
}

func (r *olap) observeCreation(kind string, inserted time.Time, valid bool, completed time.Time) {
	if valid && !inserted.After(completed) {
		metrics.OLAPCreatedToPublished.WithLabelValues(r.backend, kind).Observe(completed.Sub(inserted).Seconds())
	}
}

func (r *olap) CreateTasks(ctx context.Context, tenant uuid.UUID, tasks []*repository.V1TaskWithPayload) (*repository.StatusUpdateResult, map[uuid.UUID]struct{}, error) {
	started := time.Now()
	result, blocked, err := r.OLAPRepository.CreateTasks(ctx, tenant, tasks)
	completed := time.Now()
	r.observeWrite("create_tasks", started, err)
	if err == nil {
		for _, task := range tasks {
			if task == nil || task.V1Task == nil {
				continue
			}
			if _, skip := blocked[task.WorkflowRunID]; !skip {
				r.observeCreation("task", task.InsertedAt.Time, task.InsertedAt.Valid, completed)
			}
		}
	}
	return result, blocked, err
}

func (r *olap) CreateDAGs(ctx context.Context, tenant uuid.UUID, dags []*repository.DAGWithData) (map[uuid.UUID]struct{}, error) {
	started := time.Now()
	blocked, err := r.OLAPRepository.CreateDAGs(ctx, tenant, dags)
	completed := time.Now()
	r.observeWrite("create_dags", started, err)
	if err == nil {
		for _, dag := range dags {
			if dag == nil || dag.V1Dag == nil {
				continue
			}
			if _, skip := blocked[dag.ExternalID]; !skip {
				r.observeCreation("dag", dag.InsertedAt.Time, dag.InsertedAt.Valid, completed)
			}
		}
	}
	return blocked, err
}

func (r *olap) CreateTaskEvents(ctx context.Context, tenant uuid.UUID, events []sqlcv1.CreateTaskEventsOLAPParams, mapping map[uuid.UUID]uuid.UUID, updates []repository.OrchestratorDAGStatusUpdateOpt, operators map[uuid.UUID]struct{}) (*repository.StatusUpdateResult, map[uuid.UUID]struct{}, error) {
	started := time.Now()
	result, blocked, err := r.OLAPRepository.CreateTaskEvents(ctx, tenant, events, mapping, updates, operators)
	r.observeWrite("create_task_events", started, err)
	return result, blocked, err
}

func (r *olap) observeRead(operation string, started time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.OLAPReadDuration.WithLabelValues(r.backend, operation, result).Observe(time.Since(started).Seconds())
}

func (r *olap) ReadWorkflowRun(ctx context.Context, id uuid.UUID) (*repository.V1WorkflowRunPopulator, error) {
	started := time.Now()
	result, err := r.OLAPRepository.ReadWorkflowRun(ctx, id)
	r.observeRead("read_workflow_run", started, err)
	return result, err
}

func (r *olap) ReadTaskRunData(ctx context.Context, tenant uuid.UUID, id int64, at pgtype.Timestamptz, retry *int) (*repository.TaskWithPayloads, uuid.UUID, error) {
	started := time.Now()
	result, workflow, err := r.OLAPRepository.ReadTaskRunData(ctx, tenant, id, at, retry)
	r.observeRead("read_task_run_data", started, err)
	return result, workflow, err
}

func (r *olap) ListWorkflowRuns(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]*repository.WorkflowRunData, int, error) {
	started := time.Now()
	result, count, err := r.OLAPRepository.ListWorkflowRuns(ctx, tenant, opts)
	r.observeRead("list_workflow_runs", started, err)
	return result, count, err
}

func (r *olap) ReadTaskRunMetrics(ctx context.Context, tenant uuid.UUID, opts repository.ReadTaskRunMetricsOpts) ([]repository.TaskRunMetric, error) {
	started := time.Now()
	result, err := r.OLAPRepository.ReadTaskRunMetrics(ctx, tenant, opts)
	r.observeRead("read_task_run_metrics", started, err)
	return result, err
}
