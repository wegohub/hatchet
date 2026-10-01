package clickhouse

import "github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"

func statusPriority(status sqlcv1.V1ReadableStatusOlap) int {
	switch status {
	case sqlcv1.V1ReadableStatusOlapQUEUED:
		return 1
	case sqlcv1.V1ReadableStatusOlapRUNNING:
		return 2
	case sqlcv1.V1ReadableStatusOlapEVICTED:
		return 3
	case sqlcv1.V1ReadableStatusOlapCANCELLED:
		return 4
	case sqlcv1.V1ReadableStatusOlapFAILED:
		return 5
	case sqlcv1.V1ReadableStatusOlapCOMPLETED:
		return 6
	default:
		return 0
	}
}

// Retries supersede earlier outcomes even when their readable status is equal.
// EVICTED is reversible within the same attempt when work resumes.
func shouldUpdateStatus(old sqlcv1.V1ReadableStatusOlap, oldRetry int32, next sqlcv1.V1ReadableStatusOlap, nextRetry int32) bool {
	return nextRetry > oldRetry || nextRetry == oldRetry && (statusPriority(next) > statusPriority(old) || old == sqlcv1.V1ReadableStatusOlapEVICTED && next != sqlcv1.V1ReadableStatusOlapEVICTED)
}

func rollupDAGStatus(current sqlcv1.V1ReadableStatusOlap, total int, statuses []sqlcv1.V1ReadableStatusOlap) sqlcv1.V1ReadableStatusOlap {
	if len(statuses) == 0 {
		return current
	}
	counts := make(map[sqlcv1.V1ReadableStatusOlap]int)
	for _, status := range statuses {
		counts[status]++
	}
	if counts[sqlcv1.V1ReadableStatusOlapQUEUED] == len(statuses) {
		return current
	}
	if len(statuses) != total || counts[sqlcv1.V1ReadableStatusOlapRUNNING] > 0 || counts[sqlcv1.V1ReadableStatusOlapQUEUED] > 0 {
		return sqlcv1.V1ReadableStatusOlapRUNNING
	}
	if counts[sqlcv1.V1ReadableStatusOlapEVICTED] == total {
		return sqlcv1.V1ReadableStatusOlapEVICTED
	}
	if counts[sqlcv1.V1ReadableStatusOlapFAILED] > 0 {
		return sqlcv1.V1ReadableStatusOlapFAILED
	}
	if counts[sqlcv1.V1ReadableStatusOlapCANCELLED] > 0 {
		return sqlcv1.V1ReadableStatusOlapCANCELLED
	}
	if counts[sqlcv1.V1ReadableStatusOlapCOMPLETED] == total {
		return sqlcv1.V1ReadableStatusOlapCOMPLETED
	}
	return sqlcv1.V1ReadableStatusOlapRUNNING
}
