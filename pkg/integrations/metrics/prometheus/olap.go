package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var OLAPWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hatchet_olap_write_duration_seconds",
	Help:    "Repository write call duration including coordination and commit; observations include failed attempts.",
	Buckets: []float64{.005, .01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 10, 30},
}, []string{"backend", "operation", "result"})

var OLAPReadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hatchet_olap_read_duration_seconds",
	Help:    "Repository detail and analytic read duration by backend and operation.",
	Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .2, .3, .5, 1, 2, 5, 10},
}, []string{"backend", "operation", "result"})

var OLAPCreatedToPublished = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hatchet_olap_created_to_published_seconds",
	Help:    "Core creation timestamp to successful OLAP creation call completion, including queue delay. Retried creation deliveries can be observed again; this is not an HTTP visibility measurement.",
	Buckets: []float64{.005, .01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 10, 30},
}, []string{"backend", "kind"})

var OLAPPublicationPhaseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hatchet_olap_publication_phase_duration_seconds",
	Help:    "ClickHouse publication phase duration, including failed attempts.",
	Buckets: []float64{.001, .005, .01, .025, .05, .1, .2, .3, .5, .75, 1, 2, 5, 10, 30},
}, []string{"phase"})

var OLAPTiFlashFallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "hatchet_olap_tidb_tiflash_fallback_total",
	Help: "TiDB analytical reads retried on TiKV after a TiFlash error or read timeout.",
}, []string{"table", "reason"})

var OLAPTiDBTransactionConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "hatchet_olap_tidb_transaction_conflicts_total",
	Help: "TiDB OLAP transaction conflicts observed during publication.",
}, []string{"operation"})

var OLAPTiFlashReplicaAvailable = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hatchet_olap_tidb_tiflash_replica_available",
	Help: "Whether all TiFlash replicas of an analytical table are available and fully replicated.",
}, []string{"table"})

var OLAPTiFlashReplicaProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hatchet_olap_tidb_tiflash_replica_progress",
	Help: "Minimum TiFlash replication progress across a table's partitions.",
}, []string{"table"})

var OLAPTiDBPhaseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hatchet_olap_tidb_phase_duration_seconds",
	Help:    "TiDB OLAP queue, connection acquisition, lock, SQL and commit duration.",
	Buckets: []float64{.0005, .001, .0025, .005, .01, .02, .04, .08, .16, .32, .64, 1, 2, 5},
}, []string{"phase"})

var OLAPTiDBReadRoute = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "hatchet_olap_tidb_read_route_total",
	Help: "TiDB queries by requested storage route; optimizer route does not identify the actual execution engine.",
}, []string{"table", "route", "result"})

var OLAPTiDBVerifiedPlan = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hatchet_olap_tidb_verified_plan",
	Help: "Sampled EXPLAIN ANALYZE engine for a query template, independent of per-request route counters.",
}, []string{"query", "engine"})

var OLAPTiDBPoolWaits = promauto.NewCounter(prometheus.CounterOpts{
	Name: "hatchet_olap_tidb_pool_wait_total",
	Help: "Connection requests that waited for the TiDB pool.",
})
var OLAPTiDBPoolWaitSeconds = promauto.NewCounter(prometheus.CounterOpts{
	Name: "hatchet_olap_tidb_pool_wait_seconds_total",
	Help: "Cumulative time spent waiting for a TiDB connection.",
})
var OLAPTiDBPoolConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hatchet_olap_tidb_pool_connections",
	Help: "TiDB pool connections by state.",
}, []string{"state"})
