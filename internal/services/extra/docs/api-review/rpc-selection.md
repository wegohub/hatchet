# gRPC 候选接口选择表

“建议”不是最终决定；选择栏留空。请求/响应的全部字段见 [nats_proxy.proto](nats_proxy.proto) 和 [nats_models.proto](nats_models.proto)。原生方法的所有重载、选项见 [完整对照](method-mapping.md)。

## CoreService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | Publish | CorePublishRequest → google.protobuf.Empty | Conn.Publish, Conn.PublishMsg, Conn.PublishRequest | 瞬时发布 | 可选 |
| ☐ | Request | CoreRequest → Message | Conn.Request, Conn.RequestMsg, Conn.RequestMsgWithContext, Conn.RequestWithContext | 请求并等待一次响应 | 可选 |
| ☐ | Reply | CoreReplyRequest → google.protobuf.Empty | Msg.Respond, Msg.RespondMsg | 回复收到的请求 | 可选 |
| ☐ | Subscribe | stream SubscriptionCommand → stream SubscriptionEvent | Conn.ChanQueueSubscribe, Conn.ChanSubscribe, Conn.QueueSubscribe, Conn.QueueSubscribeSync, Conn.QueueSubscribeSyncWithChan, Conn.Subscribe, Conn.SubscribeSync | 订阅、queue group、停止/排空/限制控制 | 可选 |
| ☐ | GetConnectionInfo | Scope → ConnectionInfo | Conn.ConnectedServerJetStream, Conn.ConnectedServerVersion, Conn.HeadersSupported, Conn.MaxPayload | 租户可见连接能力 | 可选 |

## StreamService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | CreateStream | StreamConfigRequest → NatsStreamInfo | StreamManager.CreateStream | 创建流，已有不同配置报错 | 可选 |
| ☐ | UpdateStream | StreamConfigRequest → NatsStreamInfo | StreamManager.UpdateStream | 更新已有流 | 可选 |
| ☐ | CreateOrUpdateStream | StreamConfigRequest → NatsStreamInfo | StreamManager.CreateOrUpdateStream | 创建或更新 | 最小首版 |
| ☐ | GetStream | StreamInfoRequest → NatsStreamInfo | Stream.Info, StreamManager.Stream | 配置、消息数、首尾序号等状态 | 最小首版 |
| ☐ | ListStreams | StreamListRequest → stream NatsStreamInfo | StreamManager.ListStreams | 列出流及状态 | 最小首版 |
| ☐ | ListStreamNames | StreamListRequest → stream Name | StreamManager.StreamNames | 只列出名称 | 可选 |
| ☐ | FindStream | SubjectRequest → Name | StreamManager.StreamNameBySubject | 按 subject 查所属流 | 可选 |
| ☐ | DeleteStream | StreamRef → google.protobuf.Empty | StreamManager.DeleteStream | 删除流及其数据 | 最小首版 |
| ☐ | PurgeStream | PurgeStreamRequest → google.protobuf.Empty | Stream.Purge | 按 subject/序号/保留条数清理 | 可选 |
| ☐ | GetMessage | GetMessageRequest → NatsRawStreamMsg | Stream.GetLastMsgForSubject, Stream.GetMsg | 按序号或 subject 读取 | 最小首版 |
| ☐ | DeleteMessage | DeleteMessageRequest → google.protobuf.Empty | Stream.DeleteMsg, Stream.SecureDeleteMsg | 删除或安全擦除指定消息 | 可选 |
| ☐ | GetAccountInfo | Scope → NatsAccountInfo | JetStream.AccountInfo | 隔离账户统计 | 可选 |

## PublisherService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | Publish | PublishRequest → NatsPubAck | Publisher.Publish, Publisher.PublishMsg | 等待单条发布确认 | 最小首版 |
| ☐ | PublishAsync | stream PublishRequest → stream PublishResult | Publisher.PublishAsync, Publisher.PublishMsgAsync | 多条流水线，每条独立确认 | 最小首版 |

## ConsumerService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | CreateConsumer | ConsumerConfigRequest → NatsConsumerInfo | ConsumerManager.CreateConsumer, ConsumerManager.CreatePushConsumer | 创建 pull/push consumer | 可选 |
| ☐ | UpdateConsumer | ConsumerConfigRequest → NatsConsumerInfo | ConsumerManager.UpdateConsumer, ConsumerManager.UpdatePushConsumer | 更新已有 consumer | 可选 |
| ☐ | CreateOrUpdateConsumer | ConsumerConfigRequest → NatsConsumerInfo | ConsumerManager.CreateOrUpdateConsumer, ConsumerManager.CreateOrUpdatePushConsumer | 创建或更新持久消费者 | 最小首版 |
| ☐ | GetConsumer | ConsumerRef → NatsConsumerInfo | Consumer.Info, ConsumerManager.Consumer, ConsumerManager.PushConsumer, PushConsumer.Info | 投递/ACK/重投/积压状态 | 最小首版 |
| ☐ | ListConsumers | StreamRef → stream NatsConsumerInfo | ConsumerManager.ListConsumers | 列出消费者及状态 | 最小首版 |
| ☐ | ListConsumerNames | StreamRef → stream Name | ConsumerManager.ConsumerNames | 只列出消费者名称 | 可选 |
| ☐ | DeleteConsumer | ConsumerRef → google.protobuf.Empty | ConsumerManager.DeleteConsumer | 删除消费者状态 | 最小首版 |
| ☐ | PauseConsumer | PauseConsumerRequest → NatsConsumerPauseResponse | ConsumerManager.PauseConsumer | 暂停到指定时间 | 可选 |
| ☐ | ResumeConsumer | ConsumerRef → NatsConsumerPauseResponse | ConsumerManager.ResumeConsumer | 恢复暂停消费者 | 可选 |
| ☐ | ResetConsumer | ResetConsumerRequest → NatsConsumerResetResponse | ConsumerManager.ResetConsumer, ConsumerManager.ResetConsumerToSequence | 重置消费起点 | 可选 |
| ☐ | UnpinConsumer | UnpinConsumerRequest → google.protobuf.Empty | ConsumerManager.UnpinConsumer | 释放优先级组的固定客户端 | 可选 |
| ☐ | Fetch | FetchRequest → stream Delivery | Consumer.Fetch, Consumer.FetchBytes, Consumer.FetchNoWait, Consumer.Next | 按数量或字节拉取有限批次 | 最小首版 |
| ☐ | Consume | stream ConsumeCommand → stream ConsumeEvent | Consumer.Consume, Consumer.Messages, PushConsumer.Consume | 连续拉取或 push 转发 | 可选 |
| ☐ | ReadOrdered | OrderedReadRequest → stream Delivery | ConsumerManager.OrderedConsumer | 临时有序读取，会话内自动恢复 | 可选 |
| ☐ | Ack | AckRequest → google.protobuf.Empty | Msg.Ack, Msg.DoubleAck, Msg.InProgress, Msg.Nak, Msg.NakWithDelay, Msg.Term, Msg.TermWithReason | 确认、双重确认、拒绝、延期、终止 | 最小首版 |

## KeyValueService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | CreateBucket | KeyValueConfigRequest → KeyValueStatus | KeyValueManager.CreateKeyValue | 创建 bucket | 可选 |
| ☐ | UpdateBucket | KeyValueConfigRequest → KeyValueStatus | KeyValueManager.UpdateKeyValue | 更新 bucket | 可选 |
| ☐ | CreateOrUpdateBucket | KeyValueConfigRequest → KeyValueStatus | KeyValueManager.CreateOrUpdateKeyValue | 创建或更新 bucket | 最小首版 |
| ☐ | GetBucket | BucketRef → KeyValueStatus | KeyValue.Status, KeyValueManager.KeyValue | bucket 配置与容量状态 | 最小首版 |
| ☐ | ListBuckets | Scope → stream KeyValueStatus | KeyValueManager.KeyValueStores | 列出 bucket 状态 | 最小首版 |
| ☐ | ListBucketNames | Scope → stream Name | KeyValueManager.KeyValueStoreNames | 只列出 bucket 名称 | 可选 |
| ☐ | DeleteBucket | BucketRef → google.protobuf.Empty | KeyValueManager.DeleteKeyValue | 删除 bucket | 最小首版 |
| ☐ | Put | KeyValuePutRequest → Revision | KeyValue.Put, KeyValue.PutString | 写入/覆盖 | 最小首版 |
| ☐ | Create | KeyValueCreateRequest → Revision | KeyValue.Create | 不存在时写入，可带 key TTL | 最小首版 |
| ☐ | Update | KeyValueUpdateRequest → Revision | KeyValue.Update | 基于 revision 的 CAS | 最小首版 |
| ☐ | Get | KeyValueGetRequest → KeyValueEntry | KeyValue.Get, KeyValue.GetRevision | 读取最新值或历史 revision | 最小首版 |
| ☐ | Delete | KeyValueDeleteRequest → google.protobuf.Empty | KeyValue.Delete | 记录删除标记 | 最小首版 |
| ☐ | Purge | KeyValueDeleteRequest → google.protobuf.Empty | KeyValue.Purge | 移除历史并留下 purge 标记 | 可选 |
| ☐ | PurgeDeletes | KeyValuePurgeDeletesRequest → google.protobuf.Empty | KeyValue.PurgeDeletes | 清理删除标记 | 可选 |
| ☐ | ListKeys | KeyValueListKeysRequest → stream Name | KeyValue.Keys, KeyValue.ListKeys, KeyValue.ListKeysFiltered | 列出 key，可过滤 | 可选 |
| ☐ | History | KeyValueHistoryRequest → stream KeyValueEntry | KeyValue.History | 读取 key 历史 | 最小首版 |
| ☐ | Watch | KeyValueWatchRequest → stream KeyValueWatchEvent | KeyValue.Watch, KeyValue.WatchAll, KeyValue.WatchFiltered | 持续观察，可指定起始 revision | 最小首版 |

## ObjectStoreService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | CreateBucket | ObjectStoreConfigRequest → ObjectStoreStatus | ObjectStoreManager.CreateObjectStore | 创建对象 bucket | 可选 |
| ☐ | UpdateBucket | ObjectStoreConfigRequest → ObjectStoreStatus | ObjectStoreManager.UpdateObjectStore | 更新对象 bucket | 可选 |
| ☐ | CreateOrUpdateBucket | ObjectStoreConfigRequest → ObjectStoreStatus | ObjectStoreManager.CreateOrUpdateObjectStore | 创建或更新对象 bucket | 可选 |
| ☐ | GetBucket | BucketRef → ObjectStoreStatus | ObjectStore.Status, ObjectStoreManager.ObjectStore | 对象 bucket 状态 | 可选 |
| ☐ | ListBuckets | Scope → stream ObjectStoreStatus | ObjectStoreManager.ObjectStores | 列出对象 bucket | 可选 |
| ☐ | ListBucketNames | Scope → stream Name | ObjectStoreManager.ObjectStoreNames | 只列出名称 | 可选 |
| ☐ | DeleteBucket | BucketRef → google.protobuf.Empty | ObjectStoreManager.DeleteObjectStore | 删除对象 bucket | 可选 |
| ☐ | Put | stream ObjectPutPart → NatsObjectInfo | ObjectStore.Put, ObjectStore.PutBytes, ObjectStore.PutFile, ObjectStore.PutString | 流式上传对象 | 可选 |
| ☐ | Get | ObjectGetRequest → stream ObjectGetPart | ObjectStore.Get, ObjectStore.GetBytes, ObjectStore.GetFile, ObjectStore.GetString | 流式下载对象 | 可选 |
| ☐ | GetInfo | ObjectGetRequest → NatsObjectInfo | ObjectStore.GetInfo | 读取对象元数据 | 可选 |
| ☐ | List | ObjectListRequest → stream NatsObjectInfo | ObjectStore.List | 列出对象 | 可选 |
| ☐ | Delete | ObjectRef → google.protobuf.Empty | ObjectStore.Delete | 删除对象 | 可选 |
| ☐ | UpdateMeta | ObjectUpdateMetaRequest → google.protobuf.Empty | ObjectStore.UpdateMeta | 更新元数据 | 可选 |
| ☐ | AddLink | ObjectLinkRequest → NatsObjectInfo | ObjectStore.AddBucketLink, ObjectStore.AddLink | 对象链接或 bucket 链接 | 可选 |
| ☐ | Seal | BucketRef → google.protobuf.Empty | ObjectStore.Seal | 封存 bucket | 可选 |
| ☐ | Watch | ObjectWatchRequest → stream ObjectWatchEvent | ObjectStore.Watch | 观察对象元数据变化 | 可选 |

## MicroService

| 采用 | RPC | 请求 → 响应 | 原生入口 | 用途 | 建议 |
|---|---|---|---|---|---|
| ☐ | Serve | stream MicroCommand → stream MicroEvent | .AddService | 远程托管 micro handler 的双向会话 | 可选 |
| ☐ | GetInfo | MicroRef → MicroInfo | Service.Info | 服务信息 | 可选 |
| ☐ | GetStats | MicroRef → MicroStats | Service.Stats | 内置服务统计 | 可选 |
| ☐ | ResetStats | MicroRef → google.protobuf.Empty | Service.Reset | 重置服务统计 | 可选 |

共 71 个候选 RPC；其中最小首版建议 24 个。未列入最小组合的能力仍可独立选择。
