# Proposal: demand-aware weighted group round-robin scheduling

<!-- Local draft, not submitted. A human must review and edit this proposal before submission, as required by Hatchet's AI_POLICY.md. No accepted issue or assignment has been obtained. -->

## Problem

I would like groups within a single task definition to share available worker capacity by weight, while allowing a group to use spare capacity when other groups have insufficient runnable work.

The current `GROUP_ROUND_ROBIN` concurrency decision fills each group's independent `max_runs` allowance. A small per-group limit can leave worker slots idle when only one group is active. Raising that limit does not establish weighted group ordering at the subsequent worker-assignment stage.

This proposal is based on main commit [`4a02006a40cf`](https://github.com/hatchet-dev/hatchet/tree/4a02006a40cff43180ca2f56c59113a1b12eca76), specifically the [concurrency decision](https://github.com/hatchet-dev/hatchet/blob/4a02006a40cff43180ca2f56c59113a1b12eca76/pkg/scheduling/v1/concurrency/strategy.go) and [worker-assignment path](https://github.com/hatchet-dev/hatchet/blob/4a02006a40cff43180ca2f56c59113a1b12eca76/pkg/scheduling/v1/scheduler.go).

## Requested behavior

For 100 available slots, one slot per task, and no additional placement constraints:

| Runnable demand | Desired assignment |
|---|---|
| Only group A has work | Up to 100 tasks from A |
| Backlogged A and B, equal weights | 50 from each |
| Backlogged A and B, weights 1:3 | 25 from A, 75 from B |
| Equal weights; A has 10 tasks, B has at least 90 | 10 from A, 90 from B |

The allocation concerns **new successful assignments**, not rebalancing running tasks. If A already occupies 90 slots and ten slots become available when an equally weighted B joins, both groups should receive approximately five new opportunities. Running work is not preempted.

Within each group, retain Hatchet's priority ordering: higher priority first, then stable FIFO ordering among equal-priority candidates. Across groups, task priority must not override the weighted group policy.

## Proposed configuration and scope

Add an opt-in `GROUP_WEIGHTED_ROUND_ROBIN` strategy and a CEL `weight_expression` alongside the existing grouping expression. For example, group by `input.group` and obtain its weight from `input.weight`. An omitted weight expression means weight 1; evaluated weights are integers from 1 through 10,000.

An omitted `max_runs` would mean no additional per-group cap for this new strategy only. Explicit static or dynamic group caps would remain hard limits; existing strategies would retain their defaults.

Initial scope: groups within one task definition/version, shared across its worker replicas. This would support ordinary and durable tasks, including individual DAG nodes. Cross-workflow tenant-wide fairness and batch scheduling are outside the first version. Unsupported combinations should fail registration explicitly.

## Implementation direction

- Preserve an ordered group queue, logical rounds, remaining weighted allowances, and a cursor across scheduling batches. One-slot batches must retain weighted service rather than degrading to equal round robin.
- Freeze each round's membership and weights. New groups join at the tail for the next round; groups without runnable backlog do not accumulate idle credit. The newest created task can update its group's weight for the next round, without older retries rolling it back.
- Batch complete weighted rounds using demand and capacity snapshots. Handle the incomplete round using the cursor. Compare the optimized allocator against a simple reference implementation.
- Retrieve candidates by group, rather than discovering groups from the first page of a globally priority-sorted queue.
- Reuse existing affinity, sticky, rate-limit, multi-slot, and worker reservation logic. Placement failures release unused opportunities to other eligible groups through bounded retries; this is not a new global optimal-placement solver.
- Commit fairness state together with assignments. Restore it on scheduler takeover, and handle rollback or an ambiguous commit before proceeding to another batch. Perform database I/O outside the scheduler run loop.

The fairness unit is successful task assignments, not CPU time or completed throughput. Unrelated task definitions sharing the same workers would not gain a cross-definition fairness guarantee from this feature.

## Validation

Tests would cover weighted and equal allocation, demand exhaustion, one-slot batches, new groups, priority, weight updates, explicit caps, affinity, slot costs, rate limits, and retries. Integration tests would assert actual worker starts rather than only candidate ordering.

Fault tests would cover transaction rollback, lost commit responses, lease transfer, stale scheduler writes, cancellation, and worker churn. Benchmarks would cover 10, 100, and 10,000 active groups, including mostly blocked candidates, with regression measurements for tasks not using the strategy.

Would this capability fit Hatchet's scheduling model, and is the proposed task-level scope an acceptable starting point? I would like to take on an implementation after the issue is accepted and assigned. This issue proposes the behavior and integration direction; it does not claim an implementation or completed engine validation.

<details open id="ai-disclosure">
<summary><b>AI disclosure</b></summary>

OpenAI Codex was used to inspect the scheduler and help draft this proposal and the implementation plan. This local draft still requires human review and editing before submission.

</details>
