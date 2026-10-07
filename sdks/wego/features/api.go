package features

import core "github.com/hatchet-dev/hatchet/sdks/wego/internal/features"

// Clients 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type Clients = core.Clients

// RunsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type RunsClient = core.RunsClient

// CronsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type CronsClient = core.CronsClient

// SchedulesClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type SchedulesClient = core.SchedulesClient

// EventsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type EventsClient = core.EventsClient

// FiltersClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type FiltersClient = core.FiltersClient

// WorkersClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type WorkersClient = core.WorkersClient

// WorkflowsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type WorkflowsClient = core.WorkflowsClient

// LogsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type LogsClient = core.LogsClient

// WebhooksClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type WebhooksClient = core.WebhooksClient

// MetricsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type MetricsClient = core.MetricsClient

// RateLimitsClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type RateLimitsClient = core.RateLimitsClient

// CELClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type CELClient = core.CELClient

// TenantClient 由所属 Conn 提供并共享实例生命周期；业务无法注入或取出内部后端。
type TenantClient = core.TenantClient
