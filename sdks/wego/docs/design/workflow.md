# Workflow 模式

> 使用方式草案，API 尚未实现。示例省略 import；`cfg` 由使用方加载。

Workflow 通过 proto 定义图：本地节点在当前进程执行，远程节点调用导入的 Worker standalone 能力。

## 一、使用方式

### 1. 定义 proto 图

节点 ID 标识一次能力使用，edge 按 ID 连线；相同 RPC 可以用不同 ID 多次使用。输入输出类型用于校验兼容性。

```proto
syntax = "proto3";

package demo.ingest.v1;

import "demo/worker/v1/fetcher.proto";
import "demo/worker/v1/enricher.proto";
import "demo/worker/v1/saver.proto";
import "wego/v1/workflow.proto"; // 候选图协议；不泄露引擎类型。

message IngestRequest {
  string source = 1;
  string request_id = 2;
}
message IngestReply { string receipt = 1; }

service Ingest {
  option (wego.v1.workflow) = {
    entry: "Run" // 唯一对外入口。
    result: "Finish" // 此节点输出作为入口响应。

    // local.method 指向本 service 方法；未填写 id 时才默认等于 method。
    nodes: { id: "Prepare" local: { method: "Prepare" } }
    nodes: { id: "Fetch" remote: { rpc: "/demo.worker.v1.Fetcher/Fetch" } }
    nodes: { id: "Expand" local: { method: "Expand" } }
    nodes: { id: "Enrich" remote: { rpc: "/demo.worker.v1.Enricher/Enrich" } }
    nodes: { id: "Collect" local: { method: "Collect" } }
    nodes: { id: "Save" remote: { rpc: "/demo.worker.v1.Saver/Save" } }
    nodes: { id: "Finish" local: { method: "Finish" } }

    // 未填写 input_bindings 时，edge 默认将 from 节点的完整输出传给 to 节点输入。
    edges: { from: "$input" to: "Prepare" }
    edges: { from: "Prepare" to: "Fetch" }
    edges: { from: "Fetch" to: "Expand" }
    edges: {
      from: "Expand" to: "Enrich"
      map: { group: "items" item_key: "item_id" max_items: 1000 max_in_flight: 32 }
    }
    edges: { from: "Enrich" to: "Collect" collect: { group: "items" } }
    edges: { from: "Collect" to: "Save" }
    edges: { from: "Save" to: "Finish" }
  };

  // 唯一外部入口：option 指定，运行时接管其调度。
  rpc Run(IngestRequest) returns (IngestReply);

  // 以下是本地业务签名来源，不生成网络 RPC 服务/客户端桩。
  rpc Prepare(IngestRequest) returns (demo.worker.v1.FetchRequest);
  rpc Expand(demo.worker.v1.FetchReply)
      returns (stream demo.worker.v1.EnrichRequest);
  rpc Collect(stream demo.worker.v1.EnrichReply)
      returns (demo.worker.v1.SaveRequest);
  rpc Finish(demo.worker.v1.SaveReply) returns (IngestReply);
}
```

`Expand` 产生有限批次，`Enrich` 按项执行，全部成功后 `Collect` 按输入顺序汇聚；空批次收到 EOF。这里的本地 stream 用于扇出/扇入，远程节点当前只引用 unary RPC。

### 2. 生成与启动

消息由 `protoc-gen-go` 生成，图和入口桩由候选 `protoc-gen-wego` 生成；同一 Workflow service 不再交给 `protoc-gen-go-grpc`。生成的网络 Client、Server、ServiceDesc **只保留 `Run`**。

```go
func runIngest(cfg AppConfig) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv, err := wego.NewServer(
		server.WithRuntime(
			runtime.WithToken(cfg.Token),               // 引擎认证与连接信息。
			runtime.WithDurableSlots(cfg.DurableSlots), // 协调器容量，不是 local handler 槽位。
			runtime.WithLogger(logger),                 // 统一使用 slog 输出 SDK 日志。
			runtime.WithTelemetry(telemetry.Config{ // 运行时可观测性配置。
				Metrics: telemetry.MetricsConfig{Enabled: false}, // 默认关闭。
			}),
		),
		server.WithWorkflow( // 配置本地执行策略，入口和拓扑已经由 proto 确定。
			workflow.WithLocalConcurrency(8), // 当前 Server 的本地 handler 并发上限。
		),
		server.WithGRPC(cfg.GRPCAddr), // 监听地址由使用方提供，仅暴露入口 RPC。
	)
	if err != nil {
		return err
	}

	local := &ingest{}
	pb.RegisterIngestServer(srv, pb.NewIngestWorkflowServer(local))
	return srv.Serve()
}
```

生成适配器接管 `Run`，一次标准注册即可绑定图与本地业务实现。入口由 proto option 指定，Go 不再配置 `WithEntry` 或连线。

## 二、业务实现

只实现 proto 中的本地节点；远程节点由对应 Worker 提供。`IngestExpandLocalStream` / `IngestCollectLocalStream` 是进程内流接口。

```go
type ingest struct{}

func (s *ingest) Prepare(ctx context.Context, in *pb.IngestRequest) (*workerpb.FetchRequest, error) {
	// 输入也可用 workflow.Input(ctx, &root) 读取；这里已经由 edge 提供。
	return &workerpb.FetchRequest{Source: in.GetSource(), RequestId: in.GetRequestId()}, nil
}

func (s *ingest) Expand(in *workerpb.FetchReply, stream pb.IngestExpandLocalStream) error {
	if len(in.GetDocuments()) > 1000 {
		return status.Error(codes.ResourceExhausted, "documents 超过 map 上限")
	}
	for _, doc := range in.GetDocuments() {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		if doc == nil || doc.GetId() == "" {
			return status.Error(codes.InvalidArgument, "document 和 item id 必填")
		}
		if err := stream.Send(&workerpb.EnrichRequest{ItemId: doc.GetId(), Document: doc.GetBody()}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ingest) Collect(stream pb.IngestCollectLocalStream) error {
	var items []*workerpb.EnrichReply
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if len(items) >= 1000 {
			return status.Error(codes.ResourceExhausted, "items 超过汇聚上限")
		}
		items = append(items, item)
	}
	return stream.SendAndClose(&workerpb.SaveRequest{Items: items})
}

func (s *ingest) Finish(ctx context.Context, in *workerpb.SaveReply) (*pb.IngestReply, error) {
	// 读取较早的祖先输出，无需让每条边重复传递 request_id。
	var prepared workerpb.FetchRequest
	if err := workflow.Output(ctx, "Prepare", &prepared); err != nil {
		return nil, err
	}
	log.Info(ctx, "ingest 完成", "request_id", prepared.GetRequestId()) // wego 的 slog 日志封装。
	return &pb.IngestReply{Receipt: in.GetReceipt()}, nil
}
```

`workflow.Input` 读取原始输入，`workflow.Output` 读取已完成祖先的不可变输出；没有全局可写 State。循环读取需区分轮次，本地节点的副作用需幂等，持久等待交给编排控制节点。
