# NATS Go v1.54.0 全量方法与 gRPC 对照

本表是首版选型材料，不表示所有候选接口都要实现。原始函数/方法来自源码 AST；包括旧 JetStream、micro、编码器、工具与 bench。签名中的 context、callback、channel 不直接序列化。

每条 RPC 的完整请求/响应定义见 [nats_proxy.proto](nats_proxy.proto)，原生配置字段见 [nats_models.proto](nats_models.proto)。选择建议见 [README.md](README.md)。可筛选表格见 [inventory.csv](inventory.csv)。

共 836 条审计项：800 个公开函数/方法声明、14 项接口嵌入、22 项类型式 option（含配置结构）。继承项不重复展开计数；并非 836 个 RPC。


## bench — 包级函数与选项

### `HumanBytes`

```go
func(bytes float64, si bool) string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L323)

### `MsgsPerClient`

```go
func(numMsgs, numClients int) []int
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L342)

### `NewBenchmark`

```go
func(name string, subCnt, pubCnt int) *Benchmark
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L58)

### `NewSample`

```go
func(jobCount int, msgSize int, start, end time.Time, nc *nats.Conn) *Sample
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L146)

### `NewSampleGroup`

```go
func() *SampleGroup
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L181)


## bench — *Benchmark

### `*Benchmark.AddPubSample`

```go
func(s *Sample)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L115)

### `*Benchmark.AddSubSample`

```go
func(s *Sample)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L110)

### `*Benchmark.CSV`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L120)

### `*Benchmark.Close`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L68)

### `*Benchmark.Report`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L270)


## bench — *Sample

### `*Sample.Duration`

```go
func() time.Duration
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L171)

### `*Sample.Rate`

```go
func() int64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L160)

### `*Sample.Seconds`

```go
func() float64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L176)

### `*Sample.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L164)

### `*Sample.Throughput`

```go
func() float64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L155)


## bench — *SampleGroup

### `*SampleGroup.AddSample`

```go
func(e *Sample)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L243)

### `*SampleGroup.AvgRate`

```go
func() int64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L217)

### `*SampleGroup.HasSamples`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L265)

### `*SampleGroup.MaxRate`

```go
func() int64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L205)

### `*SampleGroup.MinRate`

```go
func() int64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L193)

### `*SampleGroup.Statistics`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L188)

### `*SampleGroup.StdDev`

```go
func() float64
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：基准、统计、CSV/格式化或本地样本工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/bench/bench.go#L229)


## encoders/builtin — *DefaultEncoder

### `*DefaultEncoder.Decode`

```go
func(subject string, data []byte, vPtr any) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/default_enc.go#L67)

### `*DefaultEncoder.Encode`

```go
func(subject string, v any) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/default_enc.go#L42)


## encoders/builtin — *GobEncoder

### `*GobEncoder.Decode`

```go
func(subject string, data []byte, vPtr any) (err error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/gob_enc.go#L47)

### `*GobEncoder.Encode`

```go
func(subject string, v any) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/gob_enc.go#L35)


## encoders/builtin — *JsonEncoder

### `*JsonEncoder.Decode`

```go
func(subject string, data []byte, vPtr any) (err error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/json_enc.go#L44)

### `*JsonEncoder.Encode`

```go
func(subject string, v any) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端序列化器；传输结果为 bytes，服务端不执行任意 Go 对象编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/builtin/json_enc.go#L33)


## encoders/protobuf — *ProtobufEncoder

### `*ProtobufEncoder.Decode`

```go
func(subject string, data []byte, vPtr any) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 protobuf 编解码，消息体通过 bytes 传输。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/protobuf/protobuf_enc.go#L71)

### `*ProtobufEncoder.Encode`

```go
func(subject string, v any) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 protobuf 编解码，消息体通过 bytes 传输。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/encoders/protobuf/protobuf_enc.go#L52)


## jetstream — 包级函数与选项

### `ConsumeErrHandler`

```go
type ConsumeErrHandler ConsumeErrHandlerFunc
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 ConsumeEvent.error 回调，不把函数发送到服务器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L453)

### `DecodeObjectDigest`

```go
func(data string) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端对象摘要格式工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L823)

### `DeleteMarkersOlderThan`

```go
type DeleteMarkersOlderThan time.Duration
```

- 对应：`字段 KeyValuePurgeDeletesRequest.delete_markers_older_than_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L83)

### `FetchContext`

```go
func(ctx context.Context) FetchOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：设置 gRPC context/deadline 或单次迭代等待预算，不序列化 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L559)

### `FetchHeartbeat`

```go
func(hb time.Duration) FetchOpt
```

- 对应：`字段 FetchOptions.heartbeat_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L546)

### `FetchMaxWait`

```go
func(timeout time.Duration) FetchOpt
```

- 对应：`字段 FetchOptions.max_wait_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L527)

### `FetchMinAckPending`

```go
func(min int64) FetchOpt
```

- 对应：`字段 FetchOptions.min_ack_pending`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L492)

### `FetchMinPending`

```go
func(min int64) FetchOpt
```

- 对应：`字段 FetchOptions.min_pending`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L478)

### `FetchPrioritized`

```go
func(priority uint8) FetchOpt
```

- 对应：`字段 FetchOptions.priority`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L508)

### `FetchPriorityGroup`

```go
func(group string) FetchOpt
```

- 对应：`字段 FetchOptions.priority_group`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L517)

### `GetObjectDigestValue`

```go
func(data hash.Hash) string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端对象摘要格式工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L817)

### `GetObjectInfoShowDeleted`

```go
func() GetObjectInfoOpt
```

- 对应：`字段 ObjectGetRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object_options.go#L27)

### `GetObjectShowDeleted`

```go
func() GetObjectOpt
```

- 对应：`字段 ObjectGetRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object_options.go#L18)

### `IgnoreDeletes`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.ignore_deletes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L52)

### `IncludeHistory`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.include_history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L29)

### `KeyTTL`

```go
func(ttl time.Duration) KVCreateOpt
```

- 对应：`字段 KeyValueCreateRequest.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L126)

### `LastRevision`

```go
func(revision uint64) KVDeleteOpt
```

- 对应：`字段 KeyValueDeleteRequest.expected_revision`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L98)

### `ListObjectsShowDeleted`

```go
func() ListObjectsOpt
```

- 对应：`字段 ObjectListRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object_options.go#L36)

### `MetaOnly`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.metadata_only`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L61)

### `New`

```go
func(nc *nats.Conn, opts ...JetStreamOpt) (JetStream, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：创建本地代理客户端；NATS domain/API prefix 由服务端配置，不能让租户任意指定内部路由。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L471)

### `NewWithAPIPrefix`

```go
func(nc *nats.Conn, apiPrefix string, opts ...JetStreamOpt) (JetStream, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：创建本地代理客户端；NATS domain/API prefix 由服务端配置，不能让租户任意指定内部路由。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L518)

### `NewWithDomain`

```go
func(nc *nats.Conn, domain string, opts ...JetStreamOpt) (JetStream, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：创建本地代理客户端；NATS domain/API prefix 由服务端配置，不能让租户任意指定内部路由。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L557)

### `NextContext`

```go
func(ctx context.Context) NextOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：设置 gRPC context/deadline 或单次迭代等待预算，不序列化 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L860)

### `NextMaxWait`

```go
func(timeout time.Duration) NextOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：设置 gRPC context/deadline 或单次迭代等待预算，不序列化 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L852)

### `PullExpiry`

```go
type PullExpiry time.Duration
```

- 对应：`字段 FetchOptions.max_wait_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L226)

### `PullHeartbeat`

```go
type PullHeartbeat time.Duration
```

- 对应：`字段 FetchOptions.heartbeat_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L406)

### `PullMaxBytes`

```go
type PullMaxBytes int
```

- 对应：`字段 ConsumeOpen.pull_max_bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L259)

### `PullMaxMessages`

```go
type PullMaxMessages int
```

- 对应：`字段 ConsumeOpen.pull_max_messages`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L143)

### `PullMaxMessagesWithBytesLimit`

```go
func(maxMessages, byteLimit int) pullMaxMessagesWithBytesLimit
```

- 对应：`字段 ConsumeOpen.pull_max_messages / pull_max_bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L182)

### `PullMinAckPending`

```go
type PullMinAckPending int
```

- 对应：`字段 FetchOptions.min_ack_pending`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L343)

### `PullMinPending`

```go
type PullMinPending int
```

- 对应：`字段 FetchOptions.min_pending`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L318)

### `PullPrioritized`

```go
type PullPrioritized uint8
```

- 对应：`字段 FetchOptions.priority`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L370)

### `PullPriorityGroup`

```go
type PullPriorityGroup string
```

- 对应：`字段 FetchOptions.priority_group`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L386)

### `PullThresholdBytes`

```go
type PullThresholdBytes int
```

- 对应：`字段 ConsumeOpen.threshold_bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L299)

### `PullThresholdMessages`

```go
type PullThresholdMessages int
```

- 对应：`字段 ConsumeOpen.threshold_messages`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L282)

### `PurgeTTL`

```go
func(ttl time.Duration) KVDeleteOpt
```

- 对应：`字段 KeyValueDeleteRequest.purge_ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L110)

### `ResumeFromRevision`

```go
func(revision uint64) WatchOpt
```

- 对应：`字段 WatchOptions.resume_from_revision`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L70)

### `StopAfter`

```go
type StopAfter int
```

- 对应：`字段 ConsumeOpen.stop_after`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L431)

### `UpdatesOnly`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.updates_only`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv_options.go#L41)

### `WithClientTrace`

```go
func(ct *ClientTrace) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 trace/回调/在途请求上限和等待预算；回调不能序列化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L33)

### `WithDefaultTimeout`

```go
func(timeout time.Duration) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：设置 gRPC context/deadline 或单次迭代等待预算，不序列化 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L82)

### `WithDeletedDetails`

```go
func(deletedDetails bool) StreamInfoOpt
```

- 对应：`字段 StreamInfoRequest.deleted_details`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L583)

### `WithExpectLastMsgID`

```go
func(id string) PublishOpt
```

- 对应：`字段 PublishOptions.expected_last_message_id`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L678)

### `WithExpectLastSequence`

```go
func(seq uint64) PublishOpt
```

- 对应：`字段 PublishOptions.expected_last_sequence`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L642)

### `WithExpectLastSequenceForSubject`

```go
func(seq uint64, subject string) PublishOpt
```

- 对应：`字段 PublishOptions.expected_last_subject / expected_last_subject_sequence`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L664)

### `WithExpectLastSequencePerSubject`

```go
func(seq uint64) PublishOpt
```

- 对应：`字段 PublishOptions.expected_last_subject_sequence`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L653)

### `WithExpectStream`

```go
func(stream string) PublishOpt
```

- 对应：`字段 PublishOptions.expected_stream`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L632)

### `WithGetMsgSubject`

```go
func(subject string) GetMsgOpt
```

- 对应：`字段 GetMessageRequest.subject`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L130)

### `WithMessagesErrOnMissingHeartbeat`

```go
func(hbErr bool) PullMessagesOpt
```

- 对应：`字段 ConsumeOpen.error_on_missing_heartbeat`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L467)

### `WithMsgID`

```go
func(id string) PublishOpt
```

- 对应：`字段 PublishOptions.message_id`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L613)

### `WithMsgTTL`

```go
func(dur time.Duration) PublishOpt
```

- 对应：`字段 PublishOptions.message_ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L622)

### `WithPublishAsyncAckHandler`

```go
func(cb MsgAckHandler) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 trace/回调/在途请求上限和等待预算；回调不能序列化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L51)

### `WithPublishAsyncErrHandler`

```go
func(cb MsgErrHandler) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 trace/回调/在途请求上限和等待预算；回调不能序列化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L41)

### `WithPublishAsyncMaxPending`

```go
func(max int) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 trace/回调/在途请求上限和等待预算；回调不能序列化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L60)

### `WithPublishAsyncTimeout`

```go
func(dur time.Duration) JetStreamOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端 trace/回调/在途请求上限和等待预算；回调不能序列化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L72)

### `WithPurgeKeep`

```go
func(keep uint64) StreamPurgeOpt
```

- 对应：`字段 PurgeStreamRequest.keep`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L117)

### `WithPurgeSequence`

```go
func(sequence uint64) StreamPurgeOpt
```

- 对应：`字段 PurgeStreamRequest.before_sequence`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L104)

### `WithPurgeSubject`

```go
func(subject string) StreamPurgeOpt
```

- 对应：`字段 PurgeStreamRequest.subject`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L94)

### `WithRetryAttempts`

```go
func(num int) PublishOpt
```

- 对应：`字段 PublishOptions.retry_attempts`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L699)

### `WithRetryWait`

```go
func(dur time.Duration) PublishOpt
```

- 对应：`字段 PublishOptions.retry_wait_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L687)

### `WithScheduleAt`

```go
func(t time.Time) PublishOpt
```

- 对应：`字段 ScheduleOptions.at`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L733)

### `WithScheduleCron`

```go
func(expr string) PublishOpt
```

- 对应：`字段 ScheduleOptions.cron`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L758)

### `WithScheduleEvery`

```go
func(d time.Duration) PublishOpt
```

- 对应：`字段 ScheduleOptions.every_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L743)

### `WithScheduleRollup`

```go
func() PublishOpt
```

- 对应：`字段 ScheduleOptions.rollup`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L837)

### `WithScheduleSource`

```go
func(subject string) PublishOpt
```

- 对应：`字段 ScheduleOptions.source_subject`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L786)

### `WithScheduleTTL`

```go
func(d time.Duration) PublishOpt
```

- 对应：`字段 ScheduleOptions.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L798)

### `WithScheduleTTLNever`

```go
func() PublishOpt
```

- 对应：`字段 ScheduleOptions.ttl_never`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L811)

### `WithScheduleTarget`

```go
func(subject string) PublishOpt
```

- 对应：`字段 ScheduleOptions.target_subject`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L772)

### `WithScheduleTimeZone`

```go
func(zone string) PublishOpt
```

- 对应：`字段 ScheduleOptions.time_zone`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L823)

### `WithStallWait`

```go
func(ttl time.Duration) PublishOpt
```

- 对应：`字段 PublishOptions.stall_wait_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L712)

### `WithStreamListSubject`

```go
func(subject string) StreamListOpt
```

- 对应：`字段 StreamListRequest.subject_filter`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L605)

### `WithSubjectFilter`

```go
func(subject string) StreamInfoOpt
```

- 对应：`字段 StreamInfoRequest.subject_filter`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream_options.go#L595)


## jetstream — *APIError

### `*APIError.APIError`

```go
func() *APIError
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/errors.go#L517)

### `*APIError.Error`

```go
func() string
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/errors.go#L512)

### `*APIError.Is`

```go
func(err error) bool
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/errors.go#L522)


## jetstream — *AckPolicy

### `*AckPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L510)


## jetstream — *DeliverPolicy

### `*DeliverPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L434)


## jetstream — *DiscardPolicy

### `*DiscardPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L680)


## jetstream — *KeyValueBucketStatus

### `*KeyValueBucketStatus.BackingStore`

```go
func() string
```

- 对应：`字段 KeyValueStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L829)

### `*KeyValueBucketStatus.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L817)

### `*KeyValueBucketStatus.Bytes`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L835)

### `*KeyValueBucketStatus.Config`

```go
func() KeyValueConfig
```

- 对应：`字段 KeyValueStatus.config`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L847)

### `*KeyValueBucketStatus.History`

```go
func() int64
```

- 对应：`字段 KeyValueStatus.history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L823)

### `*KeyValueBucketStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 KeyValueStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L838)

### `*KeyValueBucketStatus.LimitMarkerTTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.limit_marker_ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L842)

### `*KeyValueBucketStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 KeyValueStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L868)

### `*KeyValueBucketStatus.StreamInfo`

```go
func() *StreamInfo
```

- 对应：`字段 KeyValueStatus.stream_info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L832)

### `*KeyValueBucketStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L826)

### `*KeyValueBucketStatus.Values`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.values`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L820)


## jetstream — *ObjectBucketStatus

### `*ObjectBucketStatus.BackingStore`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1419)

### `*ObjectBucketStatus.Bucket`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1398)

### `*ObjectBucketStatus.Description`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.description`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1401)

### `*ObjectBucketStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1428)

### `*ObjectBucketStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 ObjectStoreStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1422)

### `*ObjectBucketStatus.Replicas`

```go
func() int
```

- 对应：`字段 ObjectStoreStatus.replicas`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1410)

### `*ObjectBucketStatus.Sealed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.sealed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1413)

### `*ObjectBucketStatus.Size`

```go
func() uint64
```

- 对应：`字段 ObjectStoreStatus.size`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1416)

### `*ObjectBucketStatus.Storage`

```go
func() StorageType
```

- 对应：`字段 ObjectStoreStatus.storage`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1407)

### `*ObjectBucketStatus.StreamInfo`

```go
func() *StreamInfo
```

- 对应：`字段 ObjectStoreStatus.stream_info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1425)

### `*ObjectBucketStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 ObjectStoreStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L1404)


## jetstream — *PersistModeType

### `*PersistModeType.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L714)


## jetstream — *PriorityPolicy

### `*PriorityPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L378)


## jetstream — *ReplayPolicy

### `*ReplayPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L563)


## jetstream — *RetentionPolicy

### `*RetentionPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L644)


## jetstream — *StorageType

### `*StorageType.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L760)


## jetstream — *StoreCompression

### `*StoreCompression.UnmarshalJSON`

```go
func(b []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L808)


## jetstream — AckPolicy

### `AckPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L526)

### `AckPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L540)


## jetstream — ConsumeContext

### `ConsumeContext.Closed`

```go
func() <-chan struct{}
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地完成通知 channel。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L70)

### `ConsumeContext.Drain`

```go
func()
```

- 对应：`字段 ConsumeCommand.stop / drain`。
- 建议：随所属能力。
- 说明：操作既有双向流，不创建新的消费 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L65)

### `ConsumeContext.Stop`

```go
func()
```

- 对应：`字段 ConsumeCommand.stop / drain`。
- 建议：随所属能力。
- 说明：操作既有双向流，不创建新的消费 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L61)


## jetstream — Consumer

### `Consumer.CachedInfo`

```go
func() *ConsumerInfo
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地信息缓存，不新增请求。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L162)

### `Consumer.Consume`

```go
func(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)
```

- 对应：`ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：首版建议。
- 说明：回调在客户端执行；网关通过流返回 delivery。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L137)

### `Consumer.Fetch`

```go
func(batch int, opts ...FetchOpt) (MessageBatch, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：首版建议。
- 说明：max_messages / max_bytes / no_wait；Next 相当于 max_messages=1。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L75)

### `Consumer.FetchBytes`

```go
func(maxBytes int, opts ...FetchOpt) (MessageBatch, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：首版建议。
- 说明：max_messages / max_bytes / no_wait；Next 相当于 max_messages=1。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L101)

### `Consumer.FetchNoWait`

```go
func(batch int) (MessageBatch, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：首版建议。
- 说明：max_messages / max_bytes / no_wait；Next 相当于 max_messages=1。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L120)

### `Consumer.Info`

```go
func(context.Context) (*ConsumerInfo, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L157)

### `Consumer.Messages`

```go
func(opts ...PullMessagesOpt) (MessagesContext, error)
```

- 对应：`ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：首版建议。
- 说明：客户端迭代器包裹实际 gRPC 流。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L149)

### `Consumer.Next`

```go
func(opts ...FetchOpt) (Msg, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：首版建议。
- 说明：max_messages / max_bytes / no_wait；Next 相当于 max_messages=1。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L154)


## jetstream — ConsumerInfoLister

### `ConsumerInfoLister.Err`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L295)

### `ConsumerInfoLister.Info`

```go
func() <-chan *ConsumerInfo
```

- 对应：`ConsumerService.ListConsumers(StreamRef) → stream NatsConsumerInfo`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L294)


## jetstream — ConsumerManager

### `ConsumerManager.Consumer`

```go
func(ctx context.Context, consumer string) (Consumer, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- 说明：读取信息，构造 pull/push 本地句柄。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L100)

### `ConsumerManager.ConsumerNames`

```go
func(context.Context) ConsumerNameLister
```

- 对应：`ConsumerService.ListConsumerNames(StreamRef) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L118)

### `ConsumerManager.CreateConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.CreateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L82)

### `ConsumerManager.CreateOrUpdateConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L74)

### `ConsumerManager.CreateOrUpdatePushConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L139)

### `ConsumerManager.CreatePushConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.CreateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L147)

### `ConsumerManager.DeleteConsumer`

```go
func(ctx context.Context, consumer string) error
```

- 对应：`ConsumerService.DeleteConsumer(ConsumerRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L104)

### `ConsumerManager.ListConsumers`

```go
func(context.Context) ConsumerInfoLister
```

- 对应：`ConsumerService.ListConsumers(StreamRef) → stream NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L114)

### `ConsumerManager.OrderedConsumer`

```go
func(ctx context.Context, cfg OrderedConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.ReadOrdered(OrderedReadRequest) → stream Delivery`。
- 建议：后续候选。
- 说明：延迟到读取时建立临时会话；不是永久消费者。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L93)

### `ConsumerManager.PauseConsumer`

```go
func(ctx context.Context, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)
```

- 对应：`ConsumerService.PauseConsumer(PauseConsumerRequest) → NatsConsumerPauseResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L107)

### `ConsumerManager.PushConsumer`

```go
func(ctx context.Context, consumer string) (PushConsumer, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- 说明：读取信息，构造 pull/push 本地句柄。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L159)

### `ConsumerManager.ResetConsumer`

```go
func(ctx context.Context, consumer string) (*ConsumerResetResponse, error)
```

- 对应：`ConsumerService.ResetConsumer(ResetConsumerRequest) → NatsConsumerResetResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L127)

### `ConsumerManager.ResetConsumerToSequence`

```go
func(ctx context.Context, consumer string, seq uint64) (*ConsumerResetResponse, error)
```

- 对应：`ConsumerService.ResetConsumer(ResetConsumerRequest) → NatsConsumerResetResponse`。
- 建议：后续候选。
- 说明：设置 start_sequence。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L134)

### `ConsumerManager.ResumeConsumer`

```go
func(ctx context.Context, consumer string) (*ConsumerPauseResponse, error)
```

- 对应：`ConsumerService.ResumeConsumer(ConsumerRef) → NatsConsumerPauseResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L110)

### `ConsumerManager.UnpinConsumer`

```go
func(ctx context.Context, consumer string, group string) error
```

- 对应：`ConsumerService.UnpinConsumer(UnpinConsumerRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L122)

### `ConsumerManager.UpdateConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.UpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L87)

### `ConsumerManager.UpdatePushConsumer`

```go
func(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.UpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L152)


## jetstream — ConsumerNameLister

### `ConsumerNameLister.Err`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L303)

### `ConsumerNameLister.Name`

```go
func() <-chan string
```

- 对应：`ConsumerService.ListConsumerNames(StreamRef) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L302)


## jetstream — DeliverPolicy

### `DeliverPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L455)

### `DeliverPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L473)


## jetstream — DiscardPolicy

### `DiscardPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L669)

### `DiscardPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L658)


## jetstream — JetStream

### `JetStream.AccountInfo`

```go
func(ctx context.Context) (*AccountInfo, error)
```

- 对应：`StreamService.GetAccountInfo(Scope) → NatsAccountInfo`。
- 建议：后续候选。
- 说明：需账户级租户隔离或过滤，不可返回其他租户统计。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L51)

### `JetStream.Conn`

```go
func() *nats.Conn
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：网关连接为服务端私有；SDK 只能查看自身代理配置，不能导出内部连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L54)

### `JetStream.KeyValueManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L63)

### `JetStream.ObjectStoreManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L64)

### `JetStream.Options`

```go
func() JetStreamOptions
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：网关连接为服务端私有；SDK 只能查看自身代理配置，不能导出内部连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L58)

### `JetStream.Publisher`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L62)

### `JetStream.StreamConsumerManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L60)

### `JetStream.StreamManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L61)


## jetstream — JetStreamError

### `JetStreamError.APIError`

```go
func() *APIError
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/errors.go#L25)

### `JetStreamError.error`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/errors.go#L26)


## jetstream — KeyLister

### `KeyLister.Keys`

```go
func() <-chan string
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L286)

### `KeyLister.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L287)


## jetstream — KeyValue

### `KeyValue.Bucket`

```go
func() string
```

- 对应：`字段 BucketRef.bucket`。
- 建议：随所属能力。
- 说明：本地句柄已有的名称。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L198)

### `KeyValue.Create`

```go
func(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (uint64, error)
```

- 对应：`KeyValueService.Create(KeyValueCreateRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L123)

### `KeyValue.Delete`

```go
func(ctx context.Context, key string, opts ...KVDeleteOpt) error
```

- 对应：`KeyValueService.Delete(KeyValueDeleteRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L139)

### `KeyValue.Get`

```go
func(ctx context.Context, key string) (KeyValueEntry, error)
```

- 对应：`KeyValueService.Get(KeyValueGetRequest) → KeyValueEntry`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L95)

### `KeyValue.GetRevision`

```go
func(ctx context.Context, key string, revision uint64) (KeyValueEntry, error)
```

- 对应：`KeyValueService.Get(KeyValueGetRequest) → KeyValueEntry`。
- 建议：首版建议。
- 说明：设置 revision。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L100)

### `KeyValue.History`

```go
func(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error)
```

- 对应：`KeyValueService.History(KeyValueHistoryRequest) → stream KeyValueEntry`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L195)

### `KeyValue.Keys`

```go
func(ctx context.Context, opts ...WatchOpt) ([]string, error)
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：首版建议。
- 说明：客户端收集列表；大列表建议流式 ListKeys。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L180)

### `KeyValue.ListKeys`

```go
func(ctx context.Context, opts ...WatchOpt) (KeyLister, error)
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L186)

### `KeyValue.ListKeysFiltered`

```go
func(ctx context.Context, filters ...string) (KeyLister, error)
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L191)

### `KeyValue.Purge`

```go
func(ctx context.Context, key string, opts ...KVDeleteOpt) error
```

- 对应：`KeyValueService.Purge(KeyValueDeleteRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L149)

### `KeyValue.PurgeDeletes`

```go
func(ctx context.Context, opts ...KVPurgeOpt) error
```

- 对应：`KeyValueService.PurgeDeletes(KeyValuePurgeDeletesRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L206)

### `KeyValue.Put`

```go
func(ctx context.Context, key string, value []byte) (uint64, error)
```

- 对应：`KeyValueService.Put(KeyValuePutRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L108)

### `KeyValue.PutString`

```go
func(ctx context.Context, key string, value string) (uint64, error)
```

- 对应：`KeyValueService.Put(KeyValuePutRequest) → Revision`。
- 建议：首版建议。
- 说明：客户端转换 UTF-8 字节。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L116)

### `KeyValue.Status`

```go
func(ctx context.Context) (KeyValueStatus, error)
```

- 对应：`KeyValueService.GetBucket(BucketRef) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L209)

### `KeyValue.Update`

```go
func(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)
```

- 对应：`KeyValueService.Update(KeyValueUpdateRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L129)

### `KeyValue.Watch`

```go
func(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L167)

### `KeyValue.WatchAll`

```go
func(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- 说明：keys=[">"]，匹配全部 key。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L171)

### `KeyValue.WatchFiltered`

```go
func(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- 说明：keys 为多个过滤表达式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L175)


## jetstream — KeyValueEntry

### `KeyValueEntry.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueEntry.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L359)

### `KeyValueEntry.Created`

```go
func() time.Time
```

- 对应：`字段 KeyValueEntry.created`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L371)

### `KeyValueEntry.Delta`

```go
func() uint64
```

- 对应：`字段 KeyValueEntry.delta`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L375)

### `KeyValueEntry.Key`

```go
func() string
```

- 对应：`字段 KeyValueEntry.key`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L362)

### `KeyValueEntry.Operation`

```go
func() KeyValueOp
```

- 对应：`字段 KeyValueEntry.operation`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L379)

### `KeyValueEntry.Revision`

```go
func() uint64
```

- 对应：`字段 KeyValueEntry.revision`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L368)

### `KeyValueEntry.Value`

```go
func() []byte
```

- 对应：`字段 KeyValueEntry.value`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L365)


## jetstream — KeyValueLister

### `KeyValueLister.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L297)

### `KeyValueLister.Status`

```go
func() <-chan KeyValueStatus
```

- 对应：`KeyValueService.ListBuckets(Scope) → stream KeyValueStatus`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L296)


## jetstream — KeyValueManager

### `KeyValueManager.CreateKeyValue`

```go
func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
```

- 对应：`KeyValueService.CreateBucket(KeyValueConfigRequest) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L47)

### `KeyValueManager.CreateOrUpdateKeyValue`

```go
func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
```

- 对应：`KeyValueService.CreateOrUpdateBucket(KeyValueConfigRequest) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L59)

### `KeyValueManager.DeleteKeyValue`

```go
func(ctx context.Context, bucket string) error
```

- 对应：`KeyValueService.DeleteBucket(BucketRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L65)

### `KeyValueManager.KeyValue`

```go
func(ctx context.Context, bucket string) (KeyValue, error)
```

- 对应：`KeyValueService.GetBucket(BucketRef) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L40)

### `KeyValueManager.KeyValueStoreNames`

```go
func(ctx context.Context) KeyValueNamesLister
```

- 对应：`KeyValueService.ListBucketNames(Scope) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L72)

### `KeyValueManager.KeyValueStores`

```go
func(ctx context.Context) KeyValueLister
```

- 对应：`KeyValueService.ListBuckets(Scope) → stream KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L79)

### `KeyValueManager.UpdateKeyValue`

```go
func(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
```

- 对应：`KeyValueService.UpdateBucket(KeyValueConfigRequest) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L54)


## jetstream — KeyValueNamesLister

### `KeyValueNamesLister.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L307)

### `KeyValueNamesLister.Name`

```go
func() <-chan string
```

- 对应：`KeyValueService.ListBucketNames(Scope) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L306)


## jetstream — KeyValueOp

### `KeyValueOp.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L472)


## jetstream — KeyValueStatus

### `KeyValueStatus.BackingStore`

```go
func() string
```

- 对应：`字段 KeyValueStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L326)

### `KeyValueStatus.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L313)

### `KeyValueStatus.Bytes`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L329)

### `KeyValueStatus.Config`

```go
func() KeyValueConfig
```

- 对应：`字段 KeyValueStatus.config`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L342)

### `KeyValueStatus.History`

```go
func() int64
```

- 对应：`字段 KeyValueStatus.history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L319)

### `KeyValueStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 KeyValueStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L332)

### `KeyValueStatus.LimitMarkerTTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.limit_marker_ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L336)

### `KeyValueStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 KeyValueStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L339)

### `KeyValueStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L322)

### `KeyValueStatus.Values`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.values`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L316)


## jetstream — KeyWatcher

### `KeyWatcher.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L353)

### `KeyWatcher.Updates`

```go
func() <-chan KeyValueEntry
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/kv.go#L352)


## jetstream — MessageBatch

### `MessageBatch.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L161)

### `MessageBatch.Messages`

```go
func() <-chan Msg
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L160)


## jetstream — MessagesContext

### `MessagesContext.Drain`

```go
func()
```

- 对应：`字段 ConsumeCommand.stop / drain`。
- 建议：随所属能力。
- 说明：操作既有双向流，不创建新的消费 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L52)

### `MessagesContext.Next`

```go
func(opts ...NextOpt) (Msg, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：从已建立 Consume 流读取下一条；NextContext/NextMaxWait 限制本次本地等待。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L41)

### `MessagesContext.Stop`

```go
func()
```

- 对应：`字段 ConsumeCommand.stop / drain`。
- 建议：随所属能力。
- 说明：操作既有双向流，不创建新的消费 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/pull.go#L46)


## jetstream — Msg

### `Msg.Ack`

```go
func() error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：ACK。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L53)

### `Msg.Data`

```go
func() []byte
```

- 对应：`字段 Delivery.message.data`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L40)

### `Msg.DoubleAck`

```go
func(context.Context) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：ACK + confirm=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L58)

### `Msg.Headers`

```go
func() nats.Header
```

- 对应：`字段 Delivery.message.headers`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L43)

### `Msg.InProgress`

```go
func() error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：IN_PROGRESS。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L74)

### `Msg.Metadata`

```go
func() (*MsgMetadata, error)
```

- 对应：`字段 Delivery.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L37)

### `Msg.Nak`

```go
func() error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：NAK。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L66)

### `Msg.NakWithDelay`

```go
func(delay time.Duration) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：NAK + nak_delay_ns。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L70)

### `Msg.Reply`

```go
func() string
```

- 对应：`字段 Delivery.message.reply`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L49)

### `Msg.Subject`

```go
func() string
```

- 对应：`字段 Delivery.message.subject`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L46)

### `Msg.Term`

```go
func() error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：TERM。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L78)

### `Msg.TermWithReason`

```go
func(reason string) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：TERM + term_reason。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go#L87)


## jetstream — ObjectResult

### `ObjectResult.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：对象下载流终态错误；校验失败不能返回成功 EOF。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L432)

### `ObjectResult.Info`

```go
func() (*ObjectInfo, error)
```

- 对应：`字段 ObjectGetPart.info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L431)

### `ObjectResult.io.ReadCloser`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L430)


## jetstream — ObjectStore

### `ObjectStore.AddBucketLink`

```go
func(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.AddLink(ObjectLinkRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L217)

### `ObjectStore.AddLink`

```go
func(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.AddLink(ObjectLinkRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L207)

### `ObjectStore.Delete`

```go
func(ctx context.Context, name string) error
```

- 对应：`ObjectStoreService.Delete(ObjectRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L194)

### `ObjectStore.Get`

```go
func(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L143)

### `ObjectStore.GetBytes`

```go
func(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L152)

### `ObjectStore.GetFile`

```go
func(ctx context.Context, name, file string, opts ...GetObjectOpt) error
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- 说明：文件路径只在客户端使用，传输文件字节；不会让服务器读写任意路径。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L170)

### `ObjectStore.GetInfo`

```go
func(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.GetInfo(ObjectGetRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L179)

### `ObjectStore.GetString`

```go
func(ctx context.Context, name string, opts ...GetObjectOpt) (string, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L161)

### `ObjectStore.List`

```go
func(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error)
```

- 对应：`ObjectStoreService.List(ObjectListRequest) → stream NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L238)

### `ObjectStore.Put`

```go
func(ctx context.Context, obj ObjectMeta, reader io.Reader) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L111)

### `ObjectStore.PutBytes`

```go
func(ctx context.Context, name string, data []byte) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L118)

### `ObjectStore.PutFile`

```go
func(ctx context.Context, file string) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- 说明：文件路径只在客户端使用，传输文件字节；不会让服务器读写任意路径。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L132)

### `ObjectStore.PutString`

```go
func(ctx context.Context, name string, data string) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L125)

### `ObjectStore.Seal`

```go
func(ctx context.Context) error
```

- 对应：`ObjectStoreService.Seal(BucketRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L220)

### `ObjectStore.Status`

```go
func(ctx context.Context) (ObjectStoreStatus, error)
```

- 对应：`ObjectStoreService.GetBucket(BucketRef) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L241)

### `ObjectStore.UpdateMeta`

```go
func(ctx context.Context, name string, meta ObjectMeta) error
```

- 对应：`ObjectStoreService.UpdateMeta(ObjectUpdateMetaRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L186)

### `ObjectStore.Watch`

```go
func(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error)
```

- 对应：`ObjectStoreService.Watch(ObjectWatchRequest) → stream ObjectWatchEvent`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L233)


## jetstream — ObjectStoreManager

### `ObjectStoreManager.CreateObjectStore`

```go
func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)
```

- 对应：`ObjectStoreService.CreateBucket(ObjectStoreConfigRequest) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L53)

### `ObjectStoreManager.CreateOrUpdateObjectStore`

```go
func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)
```

- 对应：`ObjectStoreService.CreateOrUpdateBucket(ObjectStoreConfigRequest) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L65)

### `ObjectStoreManager.DeleteObjectStore`

```go
func(ctx context.Context, bucket string) error
```

- 对应：`ObjectStoreService.DeleteBucket(BucketRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L71)

### `ObjectStoreManager.ObjectStore`

```go
func(ctx context.Context, bucket string) (ObjectStore, error)
```

- 对应：`ObjectStoreService.GetBucket(BucketRef) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L46)

### `ObjectStoreManager.ObjectStoreNames`

```go
func(ctx context.Context) ObjectStoreNamesLister
```

- 对应：`ObjectStoreService.ListBucketNames(Scope) → stream Name`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L80)

### `ObjectStoreManager.ObjectStores`

```go
func(ctx context.Context) ObjectStoresLister
```

- 对应：`ObjectStoreService.ListBuckets(Scope) → stream ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L89)

### `ObjectStoreManager.UpdateObjectStore`

```go
func(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)
```

- 对应：`ObjectStoreService.UpdateBucket(ObjectStoreConfigRequest) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L60)


## jetstream — ObjectStoreNamesLister

### `ObjectStoreNamesLister.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L312)

### `ObjectStoreNamesLister.Name`

```go
func() <-chan string
```

- 对应：`ObjectStoreService.ListBucketNames(Scope) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L311)


## jetstream — ObjectStoreStatus

### `ObjectStoreStatus.BackingStore`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L344)

### `ObjectStoreStatus.Bucket`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L318)

### `ObjectStoreStatus.Description`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.description`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L321)

### `ObjectStoreStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L350)

### `ObjectStoreStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 ObjectStoreStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L347)

### `ObjectStoreStatus.Replicas`

```go
func() int
```

- 对应：`字段 ObjectStoreStatus.replicas`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L332)

### `ObjectStoreStatus.Sealed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.sealed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L336)

### `ObjectStoreStatus.Size`

```go
func() uint64
```

- 对应：`字段 ObjectStoreStatus.size`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L340)

### `ObjectStoreStatus.Storage`

```go
func() StorageType
```

- 对应：`字段 ObjectStoreStatus.storage`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L328)

### `ObjectStoreStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 ObjectStoreStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L324)


## jetstream — ObjectStoresLister

### `ObjectStoresLister.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L302)

### `ObjectStoresLister.Status`

```go
func() <-chan ObjectStoreStatus
```

- 对应：`ObjectStoreService.ListBuckets(Scope) → stream ObjectStoreStatus`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L301)


## jetstream — ObjectWatcher

### `ObjectWatcher.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L252)

### `ObjectWatcher.Updates`

```go
func() <-chan *ObjectInfo
```

- 对应：`ObjectStoreService.Watch(ObjectWatchRequest) → stream ObjectWatchEvent`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/object.go#L251)


## jetstream — PersistModeType

### `PersistModeType.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L703)

### `PersistModeType.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L692)


## jetstream — PriorityPolicy

### `PriorityPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L394)


## jetstream — PubAckFuture

### `PubAckFuture.Err`

```go
func() <-chan error
```

- 对应：`字段 PublishResult.ack / error`。
- 建议：随所属能力。
- 说明：读取 correlation_id 对应结果。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/publish.go#L80)

### `PubAckFuture.Msg`

```go
func() *nats.Msg
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：保留客户端最初提交的消息快照。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/publish.go#L83)

### `PubAckFuture.Ok`

```go
func() <-chan *PubAck
```

- 对应：`字段 PublishResult.ack / error`。
- 建议：随所属能力。
- 说明：读取 correlation_id 对应结果。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/publish.go#L77)


## jetstream — Publisher

### `Publisher.CleanupPublisher`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L124)

### `Publisher.Publish`

```go
func(ctx context.Context, subject string, payload []byte, opts ...PublishOpt) (*PubAck, error)
```

- 对应：`PublisherService.Publish(PublishRequest) → NatsPubAck`。
- 建议：首版建议。
- 说明：确认持久发布；只改变消息构造形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L74)

### `Publisher.PublishAsync`

```go
func(subject string, payload []byte, opts ...PublishOpt) (PubAckFuture, error)
```

- 对应：`PublisherService.PublishAsync(stream PublishRequest) → stream PublishResult`。
- 建议：首版建议。
- 说明：双向流水线；按 correlation_id 关联结果，不提供原子批量。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L90)

### `Publisher.PublishAsyncComplete`

```go
func() <-chan struct{}
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L111)

### `Publisher.PublishAsyncPending`

```go
func() int
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L106)

### `Publisher.PublishMsg`

```go
func(ctx context.Context, msg *nats.Msg, opts ...PublishOpt) (*PubAck, error)
```

- 对应：`PublisherService.Publish(PublishRequest) → NatsPubAck`。
- 建议：首版建议。
- 说明：确认持久发布；只改变消息构造形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L79)

### `Publisher.PublishMsgAsync`

```go
func(msg *nats.Msg, opts ...PublishOpt) (PubAckFuture, error)
```

- 对应：`PublisherService.PublishAsync(stream PublishRequest) → stream PublishResult`。
- 建议：首版建议。
- 说明：双向流水线；按 correlation_id 关联结果，不提供原子批量。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L101)


## jetstream — PushConsumer

### `PushConsumer.CachedInfo`

```go
func() *ConsumerInfo
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地信息缓存，不新增请求。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L177)

### `PushConsumer.Consume`

```go
func(handler MessageHandler, opts ...PushConsumeOpt) (ConsumeContext, error)
```

- 对应：`ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：回调在客户端执行；网关通过流返回 delivery。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L171)

### `PushConsumer.Info`

```go
func(context.Context) (*ConsumerInfo, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer.go#L174)


## jetstream — ReplayPolicy

### `ReplayPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L575)

### `ReplayPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go#L585)


## jetstream — RetentionPolicy

### `RetentionPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L631)

### `RetentionPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L618)


## jetstream — StorageType

### `StorageType.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L749)

### `StorageType.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L738)


## jetstream — StoreCompression

### `StoreCompression.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L795)

### `StoreCompression.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream_config.go#L784)


## jetstream — Stream

### `Stream.CachedInfo`

```go
func() *StreamInfo
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：返回本地最近一次 Info 快照，不发网络请求。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L41)

### `Stream.ConsumerManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L33)

### `Stream.DeleteMsg`

```go
func(ctx context.Context, seq uint64) error
```

- 对应：`StreamService.DeleteMessage(DeleteMessageRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：secure=false / true，保持原生擦除语义。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L56)

### `Stream.GetLastMsgForSubject`

```go
func(ctx context.Context, subject string) (*RawStreamMsg, error)
```

- 对应：`StreamService.GetMessage(GetMessageRequest) → NatsRawStreamMsg`。
- 建议：首版建议。
- 说明：分别使用 sequence / last_subject 选择器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L52)

### `Stream.GetMsg`

```go
func(ctx context.Context, seq uint64, opts ...GetMsgOpt) (*RawStreamMsg, error)
```

- 对应：`StreamService.GetMessage(GetMessageRequest) → NatsRawStreamMsg`。
- 建议：首版建议。
- 说明：分别使用 sequence / last_subject 选择器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L48)

### `Stream.Info`

```go
func(ctx context.Context, opts ...StreamInfoOpt) (*StreamInfo, error)
```

- 对应：`StreamService.GetStream(StreamInfoRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L36)

### `Stream.Purge`

```go
func(ctx context.Context, opts ...StreamPurgeOpt) error
```

- 对应：`StreamService.PurgeStream(PurgeStreamRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L45)

### `Stream.SecureDeleteMsg`

```go
func(ctx context.Context, seq uint64) error
```

- 对应：`StreamService.DeleteMessage(DeleteMessageRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：secure=false / true，保持原生擦除语义。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/stream.go#L61)


## jetstream — StreamConsumerManager

### `StreamConsumerManager.Consumer`

```go
func(ctx context.Context, stream string, consumer string) (Consumer, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- 说明：读取信息，构造 pull/push 本地句柄。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L204)

### `StreamConsumerManager.CreateConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.CreateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L187)

### `StreamConsumerManager.CreateOrUpdateConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L179)

### `StreamConsumerManager.CreateOrUpdatePushConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L229)

### `StreamConsumerManager.CreatePushConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.CreateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L237)

### `StreamConsumerManager.DeleteConsumer`

```go
func(ctx context.Context, stream string, consumer string) error
```

- 对应：`ConsumerService.DeleteConsumer(ConsumerRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L208)

### `StreamConsumerManager.OrderedConsumer`

```go
func(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.ReadOrdered(OrderedReadRequest) → stream Delivery`。
- 建议：后续候选。
- 说明：延迟到读取时建立临时会话；不是永久消费者。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L199)

### `StreamConsumerManager.PauseConsumer`

```go
func(ctx context.Context, stream string, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)
```

- 对应：`ConsumerService.PauseConsumer(PauseConsumerRequest) → NatsConsumerPauseResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L211)

### `StreamConsumerManager.PushConsumer`

```go
func(ctx context.Context, stream string, consumer string) (PushConsumer, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- 说明：读取信息，构造 pull/push 本地句柄。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L249)

### `StreamConsumerManager.ResetConsumer`

```go
func(ctx context.Context, stream, consumer string) (*ConsumerResetResponse, error)
```

- 对应：`ConsumerService.ResetConsumer(ResetConsumerRequest) → NatsConsumerResetResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L218)

### `StreamConsumerManager.ResetConsumerToSequence`

```go
func(ctx context.Context, stream, consumer string, seq uint64) (*ConsumerResetResponse, error)
```

- 对应：`ConsumerService.ResetConsumer(ResetConsumerRequest) → NatsConsumerResetResponse`。
- 建议：后续候选。
- 说明：设置 start_sequence。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L224)

### `StreamConsumerManager.ResumeConsumer`

```go
func(ctx context.Context, stream string, consumer string) (*ConsumerPauseResponse, error)
```

- 对应：`ConsumerService.ResumeConsumer(ConsumerRef) → NatsConsumerPauseResponse`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L214)

### `StreamConsumerManager.UpdateConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
```

- 对应：`ConsumerService.UpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L192)

### `StreamConsumerManager.UpdatePushConsumer`

```go
func(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)
```

- 对应：`ConsumerService.UpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：后续候选。
- 说明：ConsumerConfigRequest.push=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L242)


## jetstream — StreamInfoLister

### `StreamInfoLister.Err`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L413)

### `StreamInfoLister.Info`

```go
func() <-chan *StreamInfo
```

- 对应：`StreamService.ListStreams(StreamListRequest) → stream NatsStreamInfo`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L412)


## jetstream — StreamManager

### `StreamManager.CreateOrUpdateStream`

```go
func(ctx context.Context, cfg StreamConfig) (Stream, error)
```

- 对应：`StreamService.CreateOrUpdateStream(StreamConfigRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L144)

### `StreamManager.CreateStream`

```go
func(ctx context.Context, cfg StreamConfig) (Stream, error)
```

- 对应：`StreamService.CreateStream(StreamConfigRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L136)

### `StreamManager.DeleteStream`

```go
func(ctx context.Context, stream string) error
```

- 对应：`StreamService.DeleteStream(StreamRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L157)

### `StreamManager.ListStreams`

```go
func(context.Context, ...StreamListOpt) StreamInfoLister
```

- 对应：`StreamService.ListStreams(StreamListRequest) → stream NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L161)

### `StreamManager.Stream`

```go
func(ctx context.Context, stream string) (Stream, error)
```

- 对应：`StreamService.GetStream(StreamInfoRequest) → NatsStreamInfo`。
- 建议：首版建议。
- 说明：查询定义并构造本地 Stream 句柄。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L148)

### `StreamManager.StreamNameBySubject`

```go
func(ctx context.Context, subject string) (string, error)
```

- 对应：`StreamService.FindStream(SubjectRequest) → Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L153)

### `StreamManager.StreamNames`

```go
func(context.Context, ...StreamListOpt) StreamNameLister
```

- 对应：`StreamService.ListStreamNames(StreamListRequest) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L165)

### `StreamManager.UpdateStream`

```go
func(ctx context.Context, cfg StreamConfig) (Stream, error)
```

- 对应：`StreamService.UpdateStream(StreamConfigRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L140)


## jetstream — StreamNameLister

### `StreamNameLister.Err`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L421)

### `StreamNameLister.Name`

```go
func() <-chan string
```

- 对应：`StreamService.ListStreamNames(StreamListRequest) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/jetstream.go#L420)


## micro — 包级函数与选项

### `AddService`

```go
func(nc *nats.Conn, config Config) (Service, error)
```

- 对应：`MicroService.Serve(stream MicroCommand) → stream MicroEvent`。
- 建议：后续候选。
- 说明：双向流 open 建立服务；业务 Handler 留在客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L325)

### `ContextHandler`

```go
func(ctx context.Context, handler func(context.Context, Request)) Handler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地包装 Handler context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L96)

### `ControlSubject`

```go
func(verb Verb, name, id string) (string, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地生成 micro 控制 subject；远程使用受限 CoreService.Request，不能任意访问系统 subject。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L940)

### `WithEndpointMetadata`

```go
func(metadata map[string]string) EndpointOpt
```

- 对应：`字段 MicroEndpoint.metadata`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L964)

### `WithEndpointMetadataKey`

```go
func(key, value string) EndpointOpt
```

- 对应：`字段 MicroEndpoint.metadata`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L974)

### `WithEndpointPendingLimits`

```go
func(msgLimit, bytesLimit int) EndpointOpt
```

- 对应：`字段 MicroEndpoint.pending_messages / pending_bytes`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L1002)

### `WithEndpointQueueGroup`

```go
func(queueGroup string) EndpointOpt
```

- 对应：`字段 MicroEndpoint.queue_group`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L984)

### `WithEndpointQueueGroupDisabled`

```go
func() EndpointOpt
```

- 对应：`字段 MicroEndpoint.disable_queue_group`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L991)

### `WithEndpointSubject`

```go
func(subject string) EndpointOpt
```

- 对应：`字段 MicroEndpoint.subject`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L957)

### `WithGroupQueueGroup`

```go
func(queueGroup string) GroupOpt
```

- 对应：`字段 MicroEndpoint.queue_group`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L1013)

### `WithGroupQueueGroupDisabled`

```go
func() GroupOpt
```

- 对应：`字段 MicroEndpoint.disable_queue_group`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L1019)

### `WithHeaders`

```go
func(headers Headers) RespondOpt
```

- 对应：`字段 MicroResponse.headers`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L164)


## micro — *NATSError

### `*NATSError.Error`

```go
func() string
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L828)

### `*NATSError.Is`

```go
func(target error) bool
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L838)

### `*NATSError.Unwrap`

```go
func() error
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L833)


## micro — Group

### `Group.AddEndpoint`

```go
func(string, Handler, ...EndpointOpt) error
```

- 对应：`字段 MicroCommand.add_endpoint`。
- 建议：后续候选。
- 说明：本地展开 Group 配置后发送。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L66)

### `Group.AddGroup`

```go
func(string, ...GroupOpt) Group
```

- 对应：`字段 MicroConfig.endpoints`。
- 建议：后续候选。
- 说明：本地组合 subject 前缀；不创建远程 Group 对象。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L62)


## micro — Handler

### `Handler.Handle`

```go
func(Request)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端收到 MicroRequest 后调用本地 Handler。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L28)


## micro — HandlerFunc

### `HandlerFunc.Handle`

```go
func(req Request)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端收到 MicroRequest 后调用本地 Handler。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L90)


## micro — Headers

### `Headers.Get`

```go
func(key string) string
```

- 对应：`字段 MicroRequest.message.headers`。
- 建议：客户端本地。
- 说明：本地 header 读取。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L199)

### `Headers.Values`

```go
func(key string) []string
```

- 对应：`字段 MicroRequest.message.headers`。
- 建议：客户端本地。
- 说明：本地 header 读取。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L205)


## micro — Request

### `Request.Data`

```go
func() []byte
```

- 对应：`字段 MicroRequest.message.data`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L54)

### `Request.Error`

```go
func(code, description string, data []byte, opts ...RespondOpt) error
```

- 对应：`字段 MicroResponse.error_code / error_description / data`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L51)

### `Request.Headers`

```go
func() Headers
```

- 对应：`字段 MicroRequest.message.headers`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L57)

### `Request.Reply`

```go
func() string
```

- 对应：`字段 MicroRequest.message.reply`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L63)

### `Request.Respond`

```go
func([]byte, ...RespondOpt) error
```

- 对应：`字段 MicroCommand.response`。
- 建议：后续候选。
- 说明：RespondJSON 先在客户端 JSON 编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L42)

### `Request.RespondJSON`

```go
func(any, ...RespondOpt) error
```

- 对应：`字段 MicroCommand.response`。
- 建议：后续候选。
- 说明：RespondJSON 先在客户端 JSON 编码。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L46)

### `Request.Subject`

```go
func() string
```

- 对应：`字段 MicroRequest.message.subject`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/request.go#L60)


## micro — Service

### `Service.AddEndpoint`

```go
func(string, Handler, ...EndpointOpt) error
```

- 对应：`字段 MicroCommand.add_endpoint`。
- 建议：后续候选。
- 说明：本地展开 Group 配置后发送。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L34)

### `Service.AddGroup`

```go
func(string, ...GroupOpt) Group
```

- 对应：`字段 MicroConfig.endpoints`。
- 建议：后续候选。
- 说明：本地组合 subject 前缀；不创建远程 Group 对象。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L38)

### `Service.Info`

```go
func() Info
```

- 对应：`MicroService.GetInfo(MicroRef) → MicroInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L41)

### `Service.Reset`

```go
func()
```

- 对应：`MicroService.ResetStats(MicroRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L47)

### `Service.Stats`

```go
func() Stats
```

- 对应：`MicroService.GetStats(MicroRef) → MicroStats`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L44)

### `Service.Stop`

```go
func() error
```

- 对应：`字段 MicroCommand.stop`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L50)

### `Service.Stopped`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地服务流状态。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L53)


## micro — Verb

### `Verb.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/micro/service.go#L306)


## nats — 包级函数与选项

### `APIPrefix`

```go
func(pre string) JSOpt
```

- 对应：`无 RPC`。
- 建议：服务端配置。
- 说明：NATS API prefix/domain 属于服务端配置。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L397)

### `AckAll`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.ack_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2761)

### `AckExplicit`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.ack_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2769)

### `AckNone`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.ack_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2752)

### `AckWait`

```go
type AckWait time.Duration
```

- 对应：`字段 NatsConsumerConfig.ack_wait；或 gRPC deadline`。
- 建议：随所属能力。
- 说明：订阅时是服务端 ACK 等待时长；发布/同步 ACK 时是本次确认等待预算。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1267)

### `BackOff`

```go
func(backOff []time.Duration) SubOpt
```

- 对应：`字段 NatsConsumerConfig.back_off`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2818)

### `Bind`

```go
func(stream, consumer string) SubOpt
```

- 对应：`字段 ConsumerRef`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2846)

### `BindStream`

```go
func(stream string) SubOpt
```

- 对应：`字段 StreamRef`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2833)

### `ClientCert`

```go
func(certFile, keyFile string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1230)

### `ClientTLSConfig`

```go
func(certCB TLSCertHandler, rootCAsCB RootCAsHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1169)

### `ClientTrace`

```go
type ClientTrace struct
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端请求/响应观测回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L360)

### `ClosedHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1451)

### `Compression`

```go
func(enabled bool) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1705)

### `Connect`

```go
func(url string, options ...Option) (*Conn, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：构造客户端连接/默认选项/本地编码包装。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1115)

### `ConnectHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1427)

### `ConsumerFilterSubjects`

```go
func(subjects ...string) SubOpt
```

- 对应：`字段 NatsConsumerConfig.filter_subjects`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2982)

### `ConsumerMemoryStorage`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.memory_storage`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2964)

### `ConsumerName`

```go
func(name string) SubOpt
```

- 对应：`字段 NatsConsumerConfig.name`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2972)

### `ConsumerReplicas`

```go
func(replicas int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.replicas`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2953)

### `Context`

```go
func(ctx context.Context) ContextOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：context 变为 gRPC deadline/cancellation，不是消息字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1316)

### `ContextOpt`

```go
type ContextOpt struct
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：传播 gRPC deadline / cancellation，不序列化 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1285)

### `CustomInboxPrefix`

```go
func(p string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1726)

### `CustomReconnectDelay`

```go
func(cb ReconnectDelayHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1311)

### `DecodeHeadersMsg`

```go
func(data []byte) (Header, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4504)

### `DecodeObjectDigest`

```go
func(data string) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端对象摘要格式工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L536)

### `DeleteMarkersOlderThan`

```go
type DeleteMarkersOlderThan time.Duration
```

- 对应：`字段 KeyValuePurgeDeletesRequest.delete_markers_older_than_ns`。
- 建议：随所属能力。
- 说明：清理删除标记的年龄阈值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L232)

### `DeliverAll`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2697)

### `DeliverLast`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2706)

### `DeliverLastPerSubject`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2715)

### `DeliverNew`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2724)

### `DeliverSubject`

```go
func(subject string) SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_subject`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2894)

### `Description`

```go
func(description string) SubOpt
```

- 对应：`字段 NatsConsumerConfig.description`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2668)

### `Dialer`

```go
func(dialer *net.Dialer) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1635)

### `DirectGet`

```go
func() JSOpt
```

- 对应：`字段 GetMessageRequest.direct`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L415)

### `DirectGetNext`

```go
func(subject string) JSOpt
```

- 对应：`字段 GetMessageRequest.next_subject`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L428)

### `DisconnectErrHandler`

```go
func(cb ConnErrHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1410)

### `DisconnectHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1419)

### `DiscoveredServersHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1459)

### `Domain`

```go
func(domain string) JSOpt
```

- 对应：`无 RPC`。
- 建议：服务端配置。
- 说明：NATS API prefix/domain 属于服务端配置。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L372)

### `DontRandomize`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1264)

### `DrainTimeout`

```go
func(t time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1402)

### `Durable`

```go
func(consumer string) SubOpt
```

- 对应：`字段 NatsConsumerConfig.durable`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2678)

### `EnableFlowControl`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.flow_control`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2871)

### `EncoderForType`

```go
func(encType string) Encoder
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L95)

### `ErrorHandler`

```go
func(cb ErrHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1467)

### `ExpectLastMsgId`

```go
func(id string) PubOpt
```

- 对应：`字段 PublishOptions.expected_last_message_id`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1199)

### `ExpectLastSequence`

```go
func(seq uint64) PubOpt
```

- 对应：`字段 PublishOptions.expected_last_sequence`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1183)

### `ExpectLastSequencePerSubject`

```go
func(seq uint64) PubOpt
```

- 对应：`字段 PublishOptions.expected_last_subject_sequence`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1191)

### `ExpectStream`

```go
func(stream string) PubOpt
```

- 对应：`字段 PublishOptions.expected_stream`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1175)

### `FlusherTimeout`

```go
func(t time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1382)

### `GetDefaultOptions`

```go
func() Options
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：构造客户端连接/默认选项/本地编码包装。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L164)

### `GetObjectDigestValue`

```go
func(data hash.Hash) string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端对象摘要格式工具。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L530)

### `GetObjectInfoShowDeleted`

```go
func() GetObjectInfoOpt
```

- 对应：`字段 ObjectGetRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L930)

### `GetObjectShowDeleted`

```go
func() GetObjectOpt
```

- 对应：`字段 ObjectGetRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L575)

### `HeadersOnly`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.headers_only`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2902)

### `IdleHeartbeat`

```go
func(duration time.Duration) SubOpt
```

- 对应：`字段 NatsConsumerConfig.idle_heartbeat`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2880)

### `IgnoreAuthErrorAbort`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1738)

### `IgnoreDeletes`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.ignore_deletes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L202)

### `IgnoreDiscoveredServers`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1817)

### `InProcessServer`

```go
func(server InProcessConnProvider) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1140)

### `InactiveThreshold`

```go
func(threshold time.Duration) SubOpt
```

- 对应：`字段 NatsConsumerConfig.inactive_threshold`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2942)

### `IncludeHistory`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.include_history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L180)

### `IsJSControlMessage`

```go
func(msg *Msg) (bool, int)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/testing_internal.go#L48)

### `LameDuckModeHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1686)

### `LastRevision`

```go
func(revision uint64) DeleteOpt
```

- 对应：`字段 KeyValueDeleteRequest.expected_revision`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L264)

### `ListObjectsShowDeleted`

```go
func() ListObjectsOpt
```

- 对应：`字段 ObjectListRequest.show_deleted`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1154)

### `ManualAck`

```go
func() SubOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地设置收到消息后是否自动调用 ConsumerService.Ack；默认建议显式 ACK。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2660)

### `MaxAckPending`

```go
func(n int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_ack_pending`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2786)

### `MaxDeliver`

```go
func(n int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_deliver`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2777)

### `MaxPingsOutstanding`

```go
func(max int) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1343)

### `MaxReconnects`

```go
func(max int) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1292)

### `MaxRequestBatch`

```go
func(max int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_request_batch`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2911)

### `MaxRequestExpires`

```go
func(max time.Duration) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_request_expires`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2920)

### `MaxRequestMaxBytes`

```go
func(bytes int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_request_max_bytes`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2929)

### `MaxWait`

```go
type MaxWait time.Duration
```

- 对应：`字段 FetchOptions.max_wait_ns；或 gRPC deadline`。
- 建议：随所属能力。
- 说明：按所在操作区分 pull 等待和管理请求预算。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1254)

### `MetaOnly`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.metadata_only`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L210)

### `MsgId`

```go
func(id string) PubOpt
```

- 对应：`字段 PublishOptions.message_id`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1167)

### `MsgTTL`

```go
func(dur time.Duration) PubOpt
```

- 对应：`字段 PublishOptions.message_ttl_ns`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1235)

### `MultipathTCP`

```go
func(enabled bool) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1647)

### `Name`

```go
func(name string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1131)

### `NewEncodedConn`

```go
func(c *Conn, encType string) (*EncodedConn, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：构造客户端连接/默认选项/本地编码包装。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L69)

### `NewInbox`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4926)

### `NewMsg`

```go
func(subject string) *Msg
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4479)

### `Nkey`

```go
func(pubKey string, sigCB SignatureHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1611)

### `NkeyOptionFromSeed`

```go
func(seedFile string) (Option, error)
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6808)

### `NoCallbacksAfterClientClose`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1675)

### `NoEcho`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1273)

### `NoReconnect`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1256)

### `OrderedConsumer`

```go
func() SubOpt
```

- 对应：`ConsumerService.ReadOrdered(OrderedReadRequest) → stream Delivery`。
- 建议：后续候选。
- 说明：有序临时会话。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2652)

### `PermissionErrOnSubscribe`

```go
func(enabled bool) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1753)

### `PingInterval`

```go
func(t time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1333)

### `ProxyPath`

```go
func(path string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1718)

### `PublishAsyncErrHandler`

```go
func(cb MsgErrHandler) JSOpt
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L986)

### `PublishAsyncMaxPending`

```go
func(max int) JSOpt
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L994)

### `PublishAsyncTimeout`

```go
func(dur time.Duration) JSOpt
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1006)

### `PullHeartbeat`

```go
type PullHeartbeat time.Duration
```

- 对应：`字段 FetchOptions.heartbeat_ns`。
- 建议：随所属能力。
- 说明：旧 Fetch 的 heartbeat。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3032)

### `PullMaxBytes`

```go
type PullMaxBytes int
```

- 对应：`字段 FetchRequest.max_bytes_limit`。
- 建议：随所属能力。
- 说明：旧 Fetch 同时支持 batch 与字节限制。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3043)

### `PullMaxWaiting`

```go
func(n int) SubOpt
```

- 对应：`字段 NatsConsumerConfig.max_waiting`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3025)

### `RateLimit`

```go
func(n uint64) SubOpt
```

- 对应：`字段 NatsConsumerConfig.rate_limit`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2810)

### `ReconnectBufSize`

```go
func(size int) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1352)

### `ReconnectErrHandler`

```go
func(cb ConnErrHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1443)

### `ReconnectHandler`

```go
func(cb ConnHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1435)

### `ReconnectJitter`

```go
func(jitter, jitterForTLS time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1301)

### `ReconnectOnFlusherError`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1393)

### `ReconnectToServer`

```go
func(cb ReconnectToServerHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1324)

### `ReconnectWait`

```go
func(t time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1282)

### `RegisterEncoder`

```go
func(encType string, enc Encoder)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端消息、header、编码注册或控制帧工具；代理自行处理 NATS 控制帧。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L86)

### `ReplayInstant`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.replay_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2802)

### `ReplayOriginal`

```go
func() SubOpt
```

- 对应：`字段 NatsConsumerConfig.replay_policy`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2794)

### `RetryAttempts`

```go
func(num int) PubOpt
```

- 对应：`字段 PublishOptions.retry_attempts`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1215)

### `RetryOnFailedConnect`

```go
func(retry bool) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1696)

### `RetryWait`

```go
func(dur time.Duration) PubOpt
```

- 对应：`字段 PublishOptions.retry_wait_ns`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1207)

### `RootCAs`

```go
func(file ...string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1200)

### `Secure`

```go
func(tls ...*tls.Config) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1150)

### `SetCustomDialer`

```go
func(dialer CustomDialer) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1657)

### `SkipConsumerLookup`

```go
func() SubOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：旧客户端兼容策略，不应成为新的跨语言服务契约。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2642)

### `SkipHostLookup`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1746)

### `SkipSubjectValidation`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1808)

### `StallWait`

```go
func(ttl time.Duration) PubOpt
```

- 对应：`字段 PublishOptions.stall_wait_ns`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L1223)

### `StartSequence`

```go
func(seq uint64) SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy / opt_start_seq`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2733)

### `StartTime`

```go
func(startTime time.Time) SubOpt
```

- 对应：`字段 NatsConsumerConfig.deliver_policy / opt_start_time`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2743)

### `StreamInfoRequest`

```go
type StreamInfoRequest struct
```

- 对应：`字段 StreamInfoRequest`。
- 建议：随所属能力。
- 说明：旧 info 选项映射对应请求字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L959)

### `StreamListFilter`

```go
func(subject string) JSOpt
```

- 对应：`字段 StreamListRequest.subject_filter`。
- 建议：随所属能力。
- 说明：旧选项映射同一 typed protobuf 字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L440)

### `StreamPurgeRequest`

```go
type StreamPurgeRequest struct
```

- 对应：`字段 PurgeStreamRequest`。
- 建议：随所属能力。
- 说明：旧 purge 选项映射对应请求字段。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L1513)

### `SyncQueueLen`

```go
func(max int) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1625)

### `TLSHandshakeFirst`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1763)

### `Timeout`

```go
func(t time.Duration) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1374)

### `Token`

```go
func(token string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1494)

### `TokenHandler`

```go
func(cb AuthTokenHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1507)

### `UpdatesOnly`

```go
func() WatchOpt
```

- 对应：`字段 WatchOptions.updates_only`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L191)

### `UseLegacyDurableConsumers`

```go
func() JSOpt
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：旧客户端兼容策略，不应成为新的跨语言服务契约。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L352)

### `UseOldRequestStyle`

```go
func() Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1665)

### `UserCredentialBytes`

```go
func(userOrChainedFileBytes []byte, seedFiles ...[]byte) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1538)

### `UserCredentials`

```go
func(userOrChainedFile string, seedFiles ...string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1519)

### `UserInfo`

```go
func(user, password string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1476)

### `UserInfoHandler`

```go
func(cb UserInfoCB) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1484)

### `UserJWT`

```go
func(userCB UserJWTHandler, sigCB SignatureHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1589)

### `UserJWTAndSeed`

```go
func(jwt string, seed string) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1566)

### `WebSocketConnectionHeaders`

```go
func(headers http.Header) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1776)

### `WebSocketConnectionHeadersHandler`

```go
func(cb WebSocketHeadersHandler) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1791)

### `WriteBufferSize`

```go
func(size int) Option
```

- 对应：`无 RPC`。
- 建议：服务端/客户端配置。
- 说明：NATS 连接、认证、TLS、重连、回调或缓冲配置；由网关部署配置或代理 SDK 本地选项承担，不能由租户操纵共享连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1365)


## nats — *APIError

### `*APIError.APIError`

```go
func() *APIError
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jserrors.go#L226)

### `*APIError.Error`

```go
func() string
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jserrors.go#L221)

### `*APIError.Is`

```go
func(err error) bool
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jserrors.go#L231)


## nats — *AckPolicy

### `*AckPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3920)


## nats — *Conn

### `*Conn.AddMsgFilter`

```go
func(subject string, filter msgFilter)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地拦截过滤，不能序列化 Go 回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/testing_internal.go#L24)

### `*Conn.AuthRequired`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6532)

### `*Conn.Barrier`

```go
func(f func()) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6552)

### `*Conn.Buffered`

```go
func() (int, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6080)

### `*Conn.ChanQueueSubscribe`

```go
func(subj, group string, ch chan *Msg) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5023)

### `*Conn.ChanSubscribe`

```go
func(subj string, ch chan *Msg) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5013)

### `*Conn.Close`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：关闭/排空当前代理客户端及其在途调用；不能关闭共享 NATS 连接或 embedded server。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6258)

### `*Conn.CloseTCPConn`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只作用当前 gRPC channel；禁止操纵网关共享 NATS TCP 连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/testing_internal.go#L54)

### `*Conn.ClosedHandler`

```go
func() ConnHandler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1908)

### `*Conn.ConnectedAddr`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2690)

### `*Conn.ConnectedClusterName`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2811)

### `*Conn.ConnectedDomain`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2750)

### `*Conn.ConnectedServerId`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2720)

### `*Conn.ConnectedServerJetStream`

```go
func() (bool, int)
```

- 对应：`CoreService.GetConnectionInfo(Scope) → ConnectionInfo`。
- 建议：后续候选。
- 说明：只返回允许租户观察的能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2829)

### `*Conn.ConnectedServerName`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2735)

### `*Conn.ConnectedServerVersion`

```go
func() string
```

- 对应：`CoreService.GetConnectionInfo(Scope) → ConnectionInfo`。
- 建议：后续候选。
- 说明：只返回允许租户观察的能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2796)

### `*Conn.ConnectedUrl`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2660)

### `*Conn.ConnectedUrlRedacted`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2675)

### `*Conn.DisconnectErrHandler`

```go
func() ConnErrHandler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1848)

### `*Conn.DiscoveredServers`

```go
func() []string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6456)

### `*Conn.DiscoveredServersHandler`

```go
func() ConnHandler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1888)

### `*Conn.Drain`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：关闭/排空当前代理客户端及其在途调用；不能关闭共享 NATS 连接或 embedded server。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6400)

### `*Conn.ErrorHandler`

```go
func() ErrHandler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1928)

### `*Conn.Flush`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6074)

### `*Conn.FlushTimeout`

```go
func(timeout time.Duration) (err error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6017)

### `*Conn.FlushWithContext`

```go
func(ctx context.Context) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/context.go#L174)

### `*Conn.ForceReconnect`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只作用当前 gRPC channel；禁止操纵网关共享 NATS TCP 连接。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2622)

### `*Conn.GetClientID`

```go
func() (uint64, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6710)

### `*Conn.GetClientIP`

```go
func() (net.IP, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6692)

### `*Conn.HeadersSupported`

```go
func() bool
```

- 对应：`CoreService.GetConnectionInfo(Scope) → ConnectionInfo`。
- 建议：后续候选。
- 说明：只返回允许租户观察的能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6525)

### `*Conn.IsClosed`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6271)

### `*Conn.IsConnected`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6285)

### `*Conn.IsDraining`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6423)

### `*Conn.IsReconnecting`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6278)

### `*Conn.IsSystemAccount`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2845)

### `*Conn.JetStream`

```go
func(opts ...JSOpt) (JetStreamContext, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：构造本地 JetStream 代理句柄，不为每次获取句柄请求服务器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L309)

### `*Conn.LastError`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4355)

### `*Conn.LocalAddr`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2705)

### `*Conn.MaxPayload`

```go
func() int64
```

- 对应：`CoreService.GetConnectionInfo(Scope) → ConnectionInfo`。
- 建议：后续候选。
- 说明：只返回允许租户观察的能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6518)

### `*Conn.NewInbox`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端创建逻辑 reply inbox；网关映射并校验租户范围。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4936)

### `*Conn.NewRespInbox`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：客户端创建逻辑 reply inbox；网关映射并校验租户范围。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4980)

### `*Conn.NumSubscriptions`

```go
func() int
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5180)

### `*Conn.Publish`

```go
func(subj string, data []byte) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：PublishRequest 的 reply inbox 需网关签发/映射；不可透传内部 inbox。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4433)

### `*Conn.PublishMsg`

```go
func(m *Msg) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：PublishRequest 的 reply inbox 需网关签发/映射；不可透传内部 inbox。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4572)

### `*Conn.PublishRequest`

```go
func(subj, reply string, data []byte) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：PublishRequest 的 reply inbox 需网关签发/映射；不可透传内部 inbox。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4587)

### `*Conn.QueueSubscribe`

```go
func(subj, queue string, cb MsgHandler) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5045)

### `*Conn.QueueSubscribeSync`

```go
func(subj, queue string) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5053)

### `*Conn.QueueSubscribeSyncWithChan`

```go
func(subj, queue string, ch chan *Msg) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5068)

### `*Conn.RTT`

```go
func() (time.Duration, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6058)

### `*Conn.ReconnectHandler`

```go
func() ConnHandler
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1868)

### `*Conn.RemoveMsgFilter`

```go
func(subject string)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地拦截过滤，不能序列化 Go 回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/testing_internal.go#L35)

### `*Conn.RemoveStatusListener`

```go
func(ch chan (Status))
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6741)

### `*Conn.Request`

```go
func(subj string, data []byte, timeout time.Duration) (*Msg, error)
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4833)

### `*Conn.RequestMsg`

```go
func(msg *Msg, timeout time.Duration) (*Msg, error)
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4819)

### `*Conn.RequestMsgWithContext`

```go
func(ctx context.Context, msg *Msg) (*Msg, error)
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/context.go#L23)

### `*Conn.RequestWithContext`

```go
func(ctx context.Context, subj string, data []byte) (*Msg, error)
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/context.go#L36)

### `*Conn.ServerPool`

```go
func() []Server
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6597)

### `*Conn.Servers`

```go
func() []string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6447)

### `*Conn.SetClosedHandler`

```go
func(cb ConnHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1898)

### `*Conn.SetDisconnectErrHandler`

```go
func(dcb ConnErrHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1838)

### `*Conn.SetDisconnectHandler`

```go
func(dcb ConnHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1828)

### `*Conn.SetDiscoveredServersHandler`

```go
func(dscb ConnHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1878)

### `*Conn.SetErrorHandler`

```go
func(cb ErrHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1918)

### `*Conn.SetReconnectHandler`

```go
func(rcb ConnHandler)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地连接事件回调；不可通过 protobuf 传 Go 函数。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1858)

### `*Conn.SetServerPool`

```go
func(servers []string) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6629)

### `*Conn.Stats`

```go
func() Statistics
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6500)

### `*Conn.Status`

```go
func() Status
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6463)

### `*Conn.StatusChanged`

```go
func(statuses ...Status) chan Status
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6724)

### `*Conn.Subscribe`

```go
func(subj string, cb MsgHandler) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5006)

### `*Conn.SubscribeSync`

```go
func(subj string) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5029)

### `*Conn.TLSConnectionState`

```go
func() (tls.ConnectionState, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L2585)

### `*Conn.TLSRequired`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：底层 NATS 运维/连接状态属于服务端；当前 gRPC 连接信息由客户端传输层提供，不做普通租户 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L6539)


## nats — *DeliverPolicy

### `*DeliverPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4046)


## nats — *DiscardPolicy

### `*DiscardPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4177)


## nats — *EncodedConn

### `*EncodedConn.BindRecvChan`

```go
func(subject string, channel any) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：本地编解码和 channel 适配，不传 Go channel。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/netchan.go#L68)

### `*EncodedConn.BindRecvQueueChan`

```go
func(subject, queue string, channel any) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：本地编解码和 channel 适配，不传 Go channel。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/netchan.go#L75)

### `*EncodedConn.BindSendChan`

```go
func(subject string, channel any) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：本地编解码和 channel 适配，不传 Go channel。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/netchan.go#L28)

### `*EncodedConn.Close`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：关闭/排空当前代理客户端及其在途调用；不能关闭共享 NATS 连接或 embedded server。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L283)

### `*EncodedConn.Drain`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：关闭/排空当前代理客户端及其在途调用；不能关闭共享 NATS 连接或 embedded server。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L294)

### `*EncodedConn.Flush`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L275)

### `*EncodedConn.FlushTimeout`

```go
func(timeout time.Duration) (err error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地代理等待先前写入或回调；不是持久确认。若需要跨 RPC 有序屏障，需另设计有序 session，不能用共享 NATS Flush 冒充。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L267)

### `*EncodedConn.LastError`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：只观察当前客户端的 gRPC 会话/缓冲；不是共享 NATS 连接的全局指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L301)

### `*EncodedConn.Publish`

```go
func(subject string, v any) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：PublishRequest 的 reply inbox 需网关签发/映射；不可透传内部 inbox。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L105)

### `*EncodedConn.PublishRequest`

```go
func(subject, reply string, v any) error
```

- 对应：`CoreService.Publish(CorePublishRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：PublishRequest 的 reply inbox 需网关签发/映射；不可透传内部 inbox。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L118)

### `*EncodedConn.QueueSubscribe`

```go
func(subject, queue string, cb Handler) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L204)

### `*EncodedConn.Request`

```go
func(subject string, v any, vPtr any, timeout time.Duration) error
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L131)

### `*EncodedConn.RequestWithContext`

```go
func(ctx context.Context, subject string, v any, vPtr any) error
```

- 对应：`CoreService.Request(CoreRequest) → Message`。
- 建议：后续候选。
- 说明：一次响应；ctx/timeout 限制等待，不承诺撤销服务端操作。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/context.go#L222)

### `*EncodedConn.Subscribe`

```go
func(subject string, cb Handler) (*Subscription, error)
```

- 对应：`CoreService.Subscribe(stream SubscriptionCommand) → stream SubscriptionEvent`。
- 建议：后续候选。
- 说明：queue_group 控制竞争消费；callback/channel/sync 仅是客户端 API 形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L195)


## nats — *ErrConsumerSequenceMismatch

### `*ErrConsumerSequenceMismatch.Error`

```go
func() string
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2129)


## nats — *KeyValueBucketStatus

### `*KeyValueBucketStatus.BackingStore`

```go
func() string
```

- 对应：`字段 KeyValueStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1271)

### `*KeyValueBucketStatus.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1259)

### `*KeyValueBucketStatus.Bytes`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1277)

### `*KeyValueBucketStatus.Config`

```go
func() KeyValueConfig
```

- 对应：`字段 KeyValueStatus.config`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1282)

### `*KeyValueBucketStatus.History`

```go
func() int64
```

- 对应：`字段 KeyValueStatus.history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1265)

### `*KeyValueBucketStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 KeyValueStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1280)

### `*KeyValueBucketStatus.StreamInfo`

```go
func() *StreamInfo
```

- 对应：`字段 KeyValueStatus.stream_info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1274)

### `*KeyValueBucketStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1268)

### `*KeyValueBucketStatus.Values`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.values`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L1262)


## nats — *Msg

### `*Msg.Ack`

```go
func(opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：ACK。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3815)

### `*Msg.AckSync`

```go
func(opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：ACK + confirm=true。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3821)

### `*Msg.Equal`

```go
func(msg *Msg) bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地比较或计算消息大小。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L828)

### `*Msg.InProgress`

```go
func(opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：IN_PROGRESS。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3851)

### `*Msg.Metadata`

```go
func() (*MsgMetadata, error)
```

- 对应：`字段 Delivery.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3868)

### `*Msg.Nak`

```go
func(opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：NAK。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3828)

### `*Msg.NakWithDelay`

```go
func(delay time.Duration, opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：NAK + nak_delay_ns。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3836)

### `*Msg.Respond`

```go
func(data []byte) error
```

- 对应：`CoreService.Reply(CoreReplyRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：使用消息的签名 reply_token。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5933)

### `*Msg.RespondMsg`

```go
func(msg *Msg) error
```

- 对应：`CoreService.Reply(CoreReplyRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- 说明：使用消息的签名 reply_token。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5948)

### `*Msg.Size`

```go
func() int
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地比较或计算消息大小。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L859)

### `*Msg.Term`

```go
func(opts ...AckOpt) error
```

- 对应：`ConsumerService.Ack(AckRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：TERM。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3845)


## nats — *ObjectBucketStatus

### `*ObjectBucketStatus.BackingStore`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1238)

### `*ObjectBucketStatus.Bucket`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1217)

### `*ObjectBucketStatus.Description`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.description`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1220)

### `*ObjectBucketStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1247)

### `*ObjectBucketStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 ObjectStoreStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1241)

### `*ObjectBucketStatus.Replicas`

```go
func() int
```

- 对应：`字段 ObjectStoreStatus.replicas`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1229)

### `*ObjectBucketStatus.Sealed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.sealed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1232)

### `*ObjectBucketStatus.Size`

```go
func() uint64
```

- 对应：`字段 ObjectStoreStatus.size`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1235)

### `*ObjectBucketStatus.Storage`

```go
func() StorageType
```

- 对应：`字段 ObjectStoreStatus.storage`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1226)

### `*ObjectBucketStatus.StreamInfo`

```go
func() *StreamInfo
```

- 对应：`字段 ObjectStoreStatus.stream_info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1244)

### `*ObjectBucketStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 ObjectStoreStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L1223)


## nats — *ReplayPolicy

### `*ReplayPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3983)


## nats — *RetentionPolicy

### `*RetentionPolicy.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4141)


## nats — *StorageType

### `*StorageType.UnmarshalJSON`

```go
func(data []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4226)


## nats — *StoreCompression

### `*StoreCompression.UnmarshalJSON`

```go
func(b []byte) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4269)


## nats — *Subscription

### `*Subscription.AutoUnsubscribe`

```go
func(max int) error
```

- 对应：`字段 SubscriptionCommand.auto_unsubscribe_after`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5477)

### `*Subscription.ClearMaxPending`

```go
func() error
```

- 对应：`字段 SubscriptionCommand.clear_max_pending`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5840)

### `*Subscription.ConsumerInfo`

```go
func() (*ConsumerInfo, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2989)

### `*Subscription.Delivered`

```go
func() (int64, error)
```

- 对应：`字段 SubscriptionCommand.stats → SubscriptionEvent.stats`。
- 建议：随所属能力。
- 说明：查询本次订阅，非共享连接指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5904)

### `*Subscription.Drain`

```go
func() error
```

- 对应：`字段 SubscriptionCommand.drain`。
- 建议：随所属能力。
- 说明：发送已缓冲消息后关闭。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5276)

### `*Subscription.Dropped`

```go
func() (int, error)
```

- 对应：`字段 SubscriptionCommand.stats → SubscriptionEvent.stats`。
- 建议：随所属能力。
- 说明：查询本次订阅，非共享连接指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5920)

### `*Subscription.Fetch`

```go
func(batch int, opts ...PullOpt) ([]*Msg, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3126)

### `*Subscription.FetchBatch`

```go
func(batch int, opts ...PullOpt) (MessageBatch, error)
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3425)

### `*Subscription.InitialConsumerPending`

```go
func() (uint64, error)
```

- 对应：`字段 NatsConsumerInfo.num_pending`。
- 建议：随所属能力。
- 说明：订阅创建时保存快照。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L2076)

### `*Subscription.IsDraining`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5292)

### `*Subscription.IsValid`

```go
func() bool
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5256)

### `*Subscription.MaxPending`

```go
func() (int, int, error)
```

- 对应：`字段 SubscriptionCommand.stats → SubscriptionEvent.stats`。
- 建议：随所属能力。
- 说明：查询本次订阅，非共享连接指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5824)

### `*Subscription.Msgs`

```go
func() iter.Seq2[*Msg, error]
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有 gRPC 订阅流；iterator/channel 适配留在客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats_iter.go#L31)

### `*Subscription.MsgsTimeout`

```go
func(timeout time.Duration) iter.Seq2[*Msg, error]
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有 gRPC 订阅流；iterator/channel 适配留在客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats_iter.go#L55)

### `*Subscription.NextMsg`

```go
func(timeout time.Duration) (*Msg, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有 gRPC 订阅流；iterator/channel 适配留在客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5573)

### `*Subscription.NextMsgWithContext`

```go
func(ctx context.Context) (*Msg, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有 gRPC 订阅流；iterator/channel 适配留在客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/context.go#L167)

### `*Subscription.Pending`

```go
func() (int, int, error)
```

- 对应：`字段 SubscriptionCommand.stats → SubscriptionEvent.stats`。
- 建议：随所属能力。
- 说明：查询本次订阅，非共享连接指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5808)

### `*Subscription.PendingLimits`

```go
func() (int, int, error)
```

- 对应：`字段 SubscriptionCommand.stats → SubscriptionEvent.stats`。
- 建议：随所属能力。
- 说明：查询本次订阅，非共享连接指标。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5867)

### `*Subscription.QueuedMsgs`

```go
func() (int, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5802)

### `*Subscription.SetClosedHandler`

```go
func(handler func(subject string))
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5493)

### `*Subscription.SetPendingLimits`

```go
func(msgLimit, bytesLimit int) error
```

- 对应：`字段 SubscriptionCommand.limits`。
- 建议：随所属能力。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5884)

### `*Subscription.StatusChanged`

```go
func(statuses ...SubStatus) <-chan SubStatus
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5306)

### `*Subscription.Type`

```go
func() SubscriptionType
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地订阅状态、队列或回调。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5238)

### `*Subscription.Unsubscribe`

```go
func() error
```

- 对应：`字段 SubscriptionCommand.stop`。
- 建议：随所属能力。
- 说明：停止对应流；JetStream 自动创建临时 consumer 的删除行为须单独跟踪，不能删共享 durable。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L5391)


## nats — AckPolicy

### `AckPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3937)

### `AckPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3952)


## nats — CustomDialer

### `CustomDialer.Dial`

```go
func(network, address string) (net.Conn, error)
```

- 对应：`无 RPC`。
- 建议：服务端配置。
- 说明：服务端连接建立/嵌入 NATS 配置，不暴露给业务客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L302)


## nats — DeliverPolicy

### `DeliverPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4065)


## nats — DiscardPolicy

### `DiscardPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4166)

### `DiscardPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4155)


## nats — Encoder

### `Encoder.Decode`

```go
func(subject string, data []byte, vPtr any) error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地编解码接口，不把 Go interface 值作为远程对象。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L34)

### `Encoder.Encode`

```go
func(subject string, v any) ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地编解码接口，不把 Go interface 值作为远程对象。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/enc.go#L33)


## nats — Header

### `Header.Add`

```go
func(key, value string)
```

- 对应：`字段 Message.headers`。
- 建议：客户端本地。
- 说明：本地 map 读写，保留同名多值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4443)

### `Header.Del`

```go
func(key string)
```

- 对应：`字段 Message.headers`。
- 建议：客户端本地。
- 说明：本地 map 读写，保留同名多值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4474)

### `Header.Get`

```go
func(key string) string
```

- 对应：`字段 Message.headers`。
- 建议：客户端本地。
- 说明：本地 map 读写，保留同名多值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4456)

### `Header.Set`

```go
func(key, value string)
```

- 对应：`字段 Message.headers`。
- 建议：客户端本地。
- 说明：本地 map 读写，保留同名多值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4450)

### `Header.Values`

```go
func(key string) []string
```

- 对应：`字段 Message.headers`。
- 建议：客户端本地。
- 说明：本地 map 读写，保留同名多值。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L4468)


## nats — InProcessConnProvider

### `InProcessConnProvider.InProcessConn`

```go
func() (net.Conn, error)
```

- 对应：`无 RPC`。
- 建议：服务端配置。
- 说明：服务端连接建立/嵌入 NATS 配置，不暴露给业务客户端。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L306)


## nats — JetStream

### `JetStream.ChanQueueSubscribe`

```go
func(subj, queue string, ch chan *Msg, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L109)

### `JetStream.ChanSubscribe`

```go
func(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L105)

### `JetStream.CleanupPublisher`

```go
func()
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：旧 JetStream API：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L72)

### `JetStream.Publish`

```go
func(subj string, data []byte, opts ...PubOpt) (*PubAck, error)
```

- 对应：`PublisherService.Publish(PublishRequest) → NatsPubAck`。
- 建议：首版建议。
- 说明：旧 JetStream API：确认持久发布；只改变消息构造形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L42)

### `JetStream.PublishAsync`

```go
func(subj string, data []byte, opts ...PubOpt) (PubAckFuture, error)
```

- 对应：`PublisherService.PublishAsync(stream PublishRequest) → stream PublishResult`。
- 建议：首版建议。
- 说明：旧 JetStream API：双向流水线；按 correlation_id 关联结果，不提供原子批量。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L49)

### `JetStream.PublishAsyncComplete`

```go
func() <-chan struct{}
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：旧 JetStream API：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L59)

### `JetStream.PublishAsyncPending`

```go
func() int
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：旧 JetStream API：本地统计、等待或取消本客户端未完成发布；取消无法撤销已持久写入。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L56)

### `JetStream.PublishMsg`

```go
func(m *Msg, opts ...PubOpt) (*PubAck, error)
```

- 对应：`PublisherService.Publish(PublishRequest) → NatsPubAck`。
- 建议：首版建议。
- 说明：旧 JetStream API：确认持久发布；只改变消息构造形式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L45)

### `JetStream.PublishMsgAsync`

```go
func(m *Msg, opts ...PubOpt) (PubAckFuture, error)
```

- 对应：`PublisherService.PublishAsync(stream PublishRequest) → stream PublishResult`。
- 建议：首版建议。
- 说明：旧 JetStream API：双向流水线；按 correlation_id 关联结果，不提供原子批量。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L53)

### `JetStream.PullSubscribe`

```go
func(subj, durable string, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L124)

### `JetStream.QueueSubscribe`

```go
func(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L114)

### `JetStream.QueueSubscribeSync`

```go
func(subj, queue string, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L118)

### `JetStream.Subscribe`

```go
func(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L97)

### `JetStream.SubscribeSync`

```go
func(subj string, opts ...SubOpt) (*Subscription, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo; ConsumerService.Consume(stream ConsumeCommand) → stream ConsumeEvent`。
- 建议：后续候选。
- 说明：旧 JetStream API：旧便利订阅：先解析 Bind/配置，再建立 pull 或 push 消费；不能直接当作 Core 订阅。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L101)


## nats — JetStreamContext

### `JetStreamContext.JetStream`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L134)

### `JetStreamContext.JetStreamManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L135)

### `JetStreamContext.KeyValueManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L136)

### `JetStreamContext.ObjectStoreManager`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L137)


## nats — JetStreamError

### `JetStreamError.APIError`

```go
func() *APIError
```

- 对应：`字段 NatsError`。
- 建议：随所属能力。
- 说明：API 错误通过 gRPC status details；流水线单项错误通过 NatsError；字符串匹配不是协议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jserrors.go#L247)

### `JetStreamError.error`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jserrors.go#L248)


## nats — JetStreamManager

### `JetStreamManager.AccountInfo`

```go
func(opts ...JSOpt) (*AccountInfo, error)
```

- 对应：`StreamService.GetAccountInfo(Scope) → NatsAccountInfo`。
- 建议：后续候选。
- 说明：旧 JetStream API：需账户级租户隔离或过滤，不可返回其他租户统计。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L99)

### `JetStreamManager.AddConsumer`

```go
func(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error)
```

- 对应：`ConsumerService.CreateOrUpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L77)

### `JetStreamManager.AddStream`

```go
func(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error)
```

- 对应：`StreamService.CreateStream(StreamConfigRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L29)

### `JetStreamManager.ConsumerInfo`

```go
func(stream, name string, opts ...JSOpt) (*ConsumerInfo, error)
```

- 对应：`ConsumerService.GetConsumer(ConsumerRef) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L86)

### `JetStreamManager.ConsumerNames`

```go
func(stream string, opts ...JSOpt) <-chan string
```

- 对应：`ConsumerService.ListConsumerNames(StreamRef) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L96)

### `JetStreamManager.Consumers`

```go
func(stream string, opts ...JSOpt) <-chan *ConsumerInfo
```

- 对应：`ConsumerService.ListConsumers(StreamRef) → stream NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L93)

### `JetStreamManager.ConsumersInfo`

```go
func(stream string, opts ...JSOpt) <-chan *ConsumerInfo
```

- 对应：`ConsumerService.ListConsumers(StreamRef) → stream NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L90)

### `JetStreamManager.DeleteConsumer`

```go
func(stream, consumer string, opts ...JSOpt) error
```

- 对应：`ConsumerService.DeleteConsumer(ConsumerRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L83)

### `JetStreamManager.DeleteMsg`

```go
func(name string, seq uint64, opts ...JSOpt) error
```

- 对应：`StreamService.DeleteMessage(DeleteMessageRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：旧 JetStream API：secure=false / true，保持原生擦除语义。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L66)

### `JetStreamManager.DeleteStream`

```go
func(name string, opts ...JSOpt) error
```

- 对应：`StreamService.DeleteStream(StreamRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L35)

### `JetStreamManager.GetLastMsg`

```go
func(name, subject string, opts ...JSOpt) (*RawStreamMsg, error)
```

- 对应：`StreamService.GetMessage(GetMessageRequest) → NatsRawStreamMsg`。
- 建议：首版建议。
- 说明：旧 JetStream API：分别使用 sequence / last_subject 选择器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L63)

### `JetStreamManager.GetMsg`

```go
func(name string, seq uint64, opts ...JSOpt) (*RawStreamMsg, error)
```

- 对应：`StreamService.GetMessage(GetMessageRequest) → NatsRawStreamMsg`。
- 建议：首版建议。
- 说明：旧 JetStream API：分别使用 sequence / last_subject 选择器。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L57)

### `JetStreamManager.PurgeStream`

```go
func(name string, opts ...JSOpt) error
```

- 对应：`StreamService.PurgeStream(PurgeStreamRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L41)

### `JetStreamManager.SecureDeleteMsg`

```go
func(name string, seq uint64, opts ...JSOpt) error
```

- 对应：`StreamService.DeleteMessage(DeleteMessageRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- 说明：旧 JetStream API：secure=false / true，保持原生擦除语义。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L70)

### `JetStreamManager.StreamInfo`

```go
func(stream string, opts ...JSOpt) (*StreamInfo, error)
```

- 对应：`StreamService.GetStream(StreamInfoRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L38)

### `JetStreamManager.StreamNameBySubject`

```go
func(string, ...JSOpt) (string, error)
```

- 对应：`StreamService.FindStream(SubjectRequest) → Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L102)

### `JetStreamManager.StreamNames`

```go
func(opts ...JSOpt) <-chan string
```

- 对应：`StreamService.ListStreamNames(StreamListRequest) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L51)

### `JetStreamManager.Streams`

```go
func(opts ...JSOpt) <-chan *StreamInfo
```

- 对应：`StreamService.ListStreams(StreamListRequest) → stream NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L48)

### `JetStreamManager.StreamsInfo`

```go
func(opts ...JSOpt) <-chan *StreamInfo
```

- 对应：`StreamService.ListStreams(StreamListRequest) → stream NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L45)

### `JetStreamManager.UpdateConsumer`

```go
func(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error)
```

- 对应：`ConsumerService.UpdateConsumer(ConsumerConfigRequest) → NatsConsumerInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L80)

### `JetStreamManager.UpdateStream`

```go
func(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error)
```

- 对应：`StreamService.UpdateStream(StreamConfigRequest) → NatsStreamInfo`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/jsm.go#L32)


## nats — KeyLister

### `KeyLister.Error`

```go
func() <-chan error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L148)

### `KeyLister.Keys`

```go
func() <-chan string
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L142)

### `KeyLister.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L143)


## nats — KeyValue

### `KeyValue.Bucket`

```go
func() string
```

- 对应：`字段 BucketRef.bucket`。
- 建议：随所属能力。
- 说明：本地句柄已有的名称。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L91)

### `KeyValue.Create`

```go
func(key string, value []byte) (revision uint64, err error)
```

- 对应：`KeyValueService.Create(KeyValueCreateRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L56)

### `KeyValue.Delete`

```go
func(key string, opts ...DeleteOpt) error
```

- 对应：`KeyValueService.Delete(KeyValueDeleteRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L66)

### `KeyValue.Get`

```go
func(key string) (entry KeyValueEntry, err error)
```

- 对应：`KeyValueService.Get(KeyValueGetRequest) → KeyValueEntry`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L48)

### `KeyValue.GetRevision`

```go
func(key string, revision uint64) (entry KeyValueEntry, err error)
```

- 对应：`KeyValueService.Get(KeyValueGetRequest) → KeyValueEntry`。
- 建议：首版建议。
- 说明：设置 revision。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L50)

### `KeyValue.History`

```go
func(key string, opts ...WatchOpt) ([]KeyValueEntry, error)
```

- 对应：`KeyValueService.History(KeyValueHistoryRequest) → stream KeyValueEntry`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L89)

### `KeyValue.Keys`

```go
func(opts ...WatchOpt) ([]string, error)
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：首版建议。
- 说明：客户端收集列表；大列表建议流式 ListKeys。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L83)

### `KeyValue.ListKeys`

```go
func(opts ...WatchOpt) (KeyLister, error)
```

- 对应：`KeyValueService.ListKeys(KeyValueListKeysRequest) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L87)

### `KeyValue.Purge`

```go
func(key string, opts ...DeleteOpt) error
```

- 对应：`KeyValueService.Purge(KeyValueDeleteRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L71)

### `KeyValue.PurgeDeletes`

```go
func(opts ...PurgeOpt) error
```

- 对应：`KeyValueService.PurgeDeletes(KeyValuePurgeDeletesRequest) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L93)

### `KeyValue.Put`

```go
func(key string, value []byte) (revision uint64, err error)
```

- 对应：`KeyValueService.Put(KeyValuePutRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L52)

### `KeyValue.PutString`

```go
func(key string, value string) (revision uint64, err error)
```

- 对应：`KeyValueService.Put(KeyValuePutRequest) → Revision`。
- 建议：首版建议。
- 说明：客户端转换 UTF-8 字节。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L54)

### `KeyValue.Status`

```go
func() (KeyValueStatus, error)
```

- 对应：`KeyValueService.GetBucket(BucketRef) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L95)

### `KeyValue.Update`

```go
func(key string, value []byte, last uint64) (revision uint64, err error)
```

- 对应：`KeyValueService.Update(KeyValueUpdateRequest) → Revision`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L61)

### `KeyValue.Watch`

```go
func(keys string, opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L74)

### `KeyValue.WatchAll`

```go
func(opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- 说明：keys=[">"]，匹配全部 key。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L76)

### `KeyValue.WatchFiltered`

```go
func(keys []string, opts ...WatchOpt) (KeyWatcher, error)
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：首版建议。
- 说明：keys 为多个过滤表达式。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L79)


## nats — KeyValueEntry

### `KeyValueEntry.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueEntry.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L333)

### `KeyValueEntry.Created`

```go
func() time.Time
```

- 对应：`字段 KeyValueEntry.created`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L341)

### `KeyValueEntry.Delta`

```go
func() uint64
```

- 对应：`字段 KeyValueEntry.delta`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L343)

### `KeyValueEntry.Key`

```go
func() string
```

- 对应：`字段 KeyValueEntry.key`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L335)

### `KeyValueEntry.Operation`

```go
func() KeyValueOp
```

- 对应：`字段 KeyValueEntry.operation`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L345)

### `KeyValueEntry.Revision`

```go
func() uint64
```

- 对应：`字段 KeyValueEntry.revision`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L339)

### `KeyValueEntry.Value`

```go
func() []byte
```

- 对应：`字段 KeyValueEntry.value`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L337)


## nats — KeyValueManager

### `KeyValueManager.CreateKeyValue`

```go
func(cfg *KeyValueConfig) (KeyValue, error)
```

- 对应：`KeyValueService.CreateBucket(KeyValueConfigRequest) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L36)

### `KeyValueManager.DeleteKeyValue`

```go
func(bucket string) error
```

- 对应：`KeyValueService.DeleteBucket(BucketRef) → google.protobuf.Empty`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L38)

### `KeyValueManager.KeyValue`

```go
func(bucket string) (KeyValue, error)
```

- 对应：`KeyValueService.GetBucket(BucketRef) → KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L34)

### `KeyValueManager.KeyValueStoreNames`

```go
func() <-chan string
```

- 对应：`KeyValueService.ListBucketNames(Scope) → stream Name`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L40)

### `KeyValueManager.KeyValueStores`

```go
func() <-chan KeyValueStatus
```

- 对应：`KeyValueService.ListBuckets(Scope) → stream KeyValueStatus`。
- 建议：首版建议。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L42)


## nats — KeyValueOp

### `KeyValueOp.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L317)


## nats — KeyValueStatus

### `KeyValueStatus.BackingStore`

```go
func() string
```

- 对应：`字段 KeyValueStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L113)

### `KeyValueStatus.Bucket`

```go
func() string
```

- 对应：`字段 KeyValueStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L101)

### `KeyValueStatus.Bytes`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.bytes`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L116)

### `KeyValueStatus.Config`

```go
func() KeyValueConfig
```

- 对应：`字段 KeyValueStatus.config`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L122)

### `KeyValueStatus.History`

```go
func() int64
```

- 对应：`字段 KeyValueStatus.history`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L107)

### `KeyValueStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 KeyValueStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L119)

### `KeyValueStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 KeyValueStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L110)

### `KeyValueStatus.Values`

```go
func() uint64
```

- 对应：`字段 KeyValueStatus.values`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L104)


## nats — KeyWatcher

### `KeyWatcher.Context`

```go
func() context.Context
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：返回客户端订阅 context。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L128)

### `KeyWatcher.Error`

```go
func() <-chan error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L137)

### `KeyWatcher.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L132)

### `KeyWatcher.Updates`

```go
func() <-chan KeyValueEntry
```

- 对应：`KeyValueService.Watch(KeyValueWatchRequest) → stream KeyValueWatchEvent`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/kv.go#L130)


## nats — MessageBatch

### `MessageBatch.Done`

```go
func() <-chan struct{}
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：现有 Fetch 流的完成通知。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3386)

### `MessageBatch.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：读取既有列表/订阅 RPC 的终态错误。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3383)

### `MessageBatch.Messages`

```go
func() <-chan *Msg
```

- 对应：`ConsumerService.Fetch(FetchRequest) → stream Delivery`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3380)


## nats — ObjectResult

### `ObjectResult.Error`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：对象下载流终态错误；校验失败不能返回成功 EOF。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L226)

### `ObjectResult.Info`

```go
func() (*ObjectInfo, error)
```

- 对应：`字段 ObjectGetPart.info`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L225)

### `ObjectResult.io.ReadCloser`

- 对应：`接口展开`。
- 建议：类型关系。
- 说明：继承对应接口的全部方法；io.ReadCloser 的 Read/Close 对应读取/取消对象下载流，error.Error 对应本地错误格式化。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L224)


## nats — ObjectStore

### `ObjectStore.AddBucketLink`

```go
func(name string, bucket ObjectStore) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.AddLink(ObjectLinkRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L85)

### `ObjectStore.AddLink`

```go
func(name string, obj *ObjectInfo) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.AddLink(ObjectLinkRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L82)

### `ObjectStore.Delete`

```go
func(name string) error
```

- 对应：`ObjectStoreService.Delete(ObjectRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L79)

### `ObjectStore.Get`

```go
func(name string, opts ...GetObjectOpt) (ObjectResult, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L56)

### `ObjectStore.GetBytes`

```go
func(name string, opts ...GetObjectOpt) ([]byte, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L61)

### `ObjectStore.GetFile`

```go
func(name, file string, opts ...GetObjectOpt) error
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- 说明：文件路径只在客户端使用，传输文件字节；不会让服务器读写任意路径。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L71)

### `ObjectStore.GetInfo`

```go
func(name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.GetInfo(ObjectGetRequest) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L74)

### `ObjectStore.GetString`

```go
func(name string, opts ...GetObjectOpt) (string, error)
```

- 对应：`ObjectStoreService.Get(ObjectGetRequest) → stream ObjectGetPart`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L66)

### `ObjectStore.List`

```go
func(opts ...ListObjectsOpt) ([]*ObjectInfo, error)
```

- 对应：`ObjectStoreService.List(ObjectListRequest) → stream NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L94)

### `ObjectStore.Put`

```go
func(obj *ObjectMeta, reader io.Reader, opts ...ObjectOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L54)

### `ObjectStore.PutBytes`

```go
func(name string, data []byte, opts ...ObjectOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L59)

### `ObjectStore.PutFile`

```go
func(file string, opts ...ObjectOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- 说明：文件路径只在客户端使用，传输文件字节；不会让服务器读写任意路径。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L69)

### `ObjectStore.PutString`

```go
func(name string, data string, opts ...ObjectOpt) (*ObjectInfo, error)
```

- 对应：`ObjectStoreService.Put(stream ObjectPutPart) → NatsObjectInfo`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L64)

### `ObjectStore.Seal`

```go
func() error
```

- 对应：`ObjectStoreService.Seal(BucketRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L88)

### `ObjectStore.Status`

```go
func() (ObjectStoreStatus, error)
```

- 对应：`ObjectStoreService.GetBucket(BucketRef) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L97)

### `ObjectStore.UpdateMeta`

```go
func(name string, meta *ObjectMeta) error
```

- 对应：`ObjectStoreService.UpdateMeta(ObjectUpdateMetaRequest) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L76)

### `ObjectStore.Watch`

```go
func(opts ...WatchOpt) (ObjectWatcher, error)
```

- 对应：`ObjectStoreService.Watch(ObjectWatchRequest) → stream ObjectWatchEvent`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L91)


## nats — ObjectStoreManager

### `ObjectStoreManager.CreateObjectStore`

```go
func(cfg *ObjectStoreConfig) (ObjectStore, error)
```

- 对应：`ObjectStoreService.CreateBucket(ObjectStoreConfigRequest) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L41)

### `ObjectStoreManager.DeleteObjectStore`

```go
func(bucket string) error
```

- 对应：`ObjectStoreService.DeleteBucket(BucketRef) → google.protobuf.Empty`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L43)

### `ObjectStoreManager.ObjectStore`

```go
func(bucket string) (ObjectStore, error)
```

- 对应：`ObjectStoreService.GetBucket(BucketRef) → ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L39)

### `ObjectStoreManager.ObjectStoreNames`

```go
func(opts ...ObjectOpt) <-chan string
```

- 对应：`ObjectStoreService.ListBucketNames(Scope) → stream Name`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L45)

### `ObjectStoreManager.ObjectStores`

```go
func(opts ...ObjectOpt) <-chan ObjectStoreStatus
```

- 对应：`ObjectStoreService.ListBuckets(Scope) → stream ObjectStoreStatus`。
- 建议：后续候选。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L47)


## nats — ObjectStoreStatus

### `ObjectStoreStatus.BackingStore`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.backing_store`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L177)

### `ObjectStoreStatus.Bucket`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.bucket`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L163)

### `ObjectStoreStatus.Description`

```go
func() string
```

- 对应：`字段 ObjectStoreStatus.description`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L165)

### `ObjectStoreStatus.IsCompressed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.compressed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L181)

### `ObjectStoreStatus.Metadata`

```go
func() map[string]string
```

- 对应：`字段 ObjectStoreStatus.metadata`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L179)

### `ObjectStoreStatus.Replicas`

```go
func() int
```

- 对应：`字段 ObjectStoreStatus.replicas`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L171)

### `ObjectStoreStatus.Sealed`

```go
func() bool
```

- 对应：`字段 ObjectStoreStatus.sealed`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L173)

### `ObjectStoreStatus.Size`

```go
func() uint64
```

- 对应：`字段 ObjectStoreStatus.size`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L175)

### `ObjectStoreStatus.Storage`

```go
func() StorageType
```

- 对应：`字段 ObjectStoreStatus.storage`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L169)

### `ObjectStoreStatus.TTL`

```go
func() time.Duration
```

- 对应：`字段 ObjectStoreStatus.ttl_ns`。
- 建议：随所属能力。
- 说明：字段读取或选项设置，不增加 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L167)


## nats — ObjectWatcher

### `ObjectWatcher.Stop`

```go
func() error
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：取消对应 gRPC 流；不删除 durable consumer / bucket。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L119)

### `ObjectWatcher.Updates`

```go
func() <-chan *ObjectInfo
```

- 对应：`ObjectStoreService.Watch(ObjectWatchRequest) → stream ObjectWatchEvent`。
- 建议：随所属能力。
- 说明：读取既有 RPC 的响应流，不再次发起 RPC。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/object.go#L117)


## nats — Options

### `Options.Connect`

```go
func() (*Conn, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：创建 gRPC 客户端；NATS 连接配置留在网关。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L1953)


## nats — PubAckFuture

### `PubAckFuture.Err`

```go
func() <-chan error
```

- 对应：`字段 PublishResult.ack / error`。
- 建议：随所属能力。
- 说明：读取 correlation_id 对应结果。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L647)

### `PubAckFuture.Msg`

```go
func() *Msg
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：保留客户端最初提交的消息快照。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L650)

### `PubAckFuture.Ok`

```go
func() <-chan *PubAck
```

- 对应：`字段 PublishResult.ack / error`。
- 建议：随所属能力。
- 说明：读取 correlation_id 对应结果。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L644)


## nats — ReplayPolicy

### `ReplayPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L3996)


## nats — RetentionPolicy

### `RetentionPolicy.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4128)

### `RetentionPolicy.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4115)


## nats — Status

### `Status.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L199)


## nats — StorageType

### `StorageType.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4215)

### `StorageType.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4204)


## nats — StoreCompression

### `StoreCompression.MarshalJSON`

```go
func() ([]byte, error)
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4256)

### `StoreCompression.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/js.go#L4245)


## nats — SubStatus

### `SubStatus.String`

```go
func() string
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：枚举/数据的本地 JSON 或字符串转换。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/nats.go#L782)


## util — 包级函数与选项

### `CloneTLSConfig`

```go
func(c *tls.Config) *tls.Config
```

- 对应：`无 RPC`。
- 建议：客户端本地。
- 说明：本地 TLS 配置复制。
- [源码](https://github.com/nats-io/nats.go/blob/v1.54.0/util/tls.go#L19)

