package extra

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// key 在加载 KV 桶前验证 key，避免无效路径触发后端 I/O。
func (s *Service) key(ctx context.Context, r *pb.KeyRef) (address, jetstream.KeyValue, string, error) {
	if r == nil || !validKey(r.Key, false) {
		return address{}, nil, "", invalid("invalid key")
	}
	a, n, err := s.bucket(ctx, r.Bucket)
	if err != nil {
		return a, nil, "", err
	}
	kv, err := s.js.KeyValue(ctx, n)
	return a, kv, r.Key, rpcError(err)
}

// kvStatus 复制桶配置和元数据，将存储状态转换为协议响应。
func (a address) kvStatus(st jetstream.KeyValueStatus) *pb.KeyValueStatus {
	p := kvStatus(st)
	p.Bucket = a.logicalName(p.Bucket)
	p.Config.Bucket = ptr(p.Bucket)
	return p
}

// kvEntry 复制业务字节，保留 revision、创建时间和删除操作标记。
func (a address) kvEntry(e jetstream.KeyValueEntry) *pb.KeyValueEntry {
	p := kvEntry(e)
	p.Bucket = a.logicalName(p.Bucket)
	return p
}

// CreateOrUpdateBucket 创建或更新租户内的 KV 桶，限制历史版本和值大小。
func (s *Service) CreateOrUpdateBucket(ctx context.Context, r *pb.KeyValueConfigRequest) (*pb.KeyValueStatus, error) {
	if r == nil || r.Config == nil {
		return nil, invalid("KV config required")
	}
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return nil, err
	}
	c := decodeKeyValueConfig(r.Config)
	if c.Replicas > 1 {
		return nil, reasonError(codes.FailedPrecondition, ReasonReplicasUnsupported, "single node supports one replica")
	}
	budget := s.cfg.MaxStoreBytes
	if c.Storage == jetstream.MemoryStorage {
		budget = s.cfg.MaxMemoryBytes
	}
	if c.MaxBytes > budget {
		return nil, exhausted(ReasonStoreExhausted, "bucket byte limit exceeds account budget")
	}

	if c.Storage < jetstream.FileStorage ||
		c.Storage > jetstream.MemoryStorage ||
		c.Replicas < 0 ||
		c.Replicas > 5 ||
		c.TTL < 0 ||
		c.MaxBytes < -1 ||
		c.LimitMarkerTTL < 0 ||
		c.MaxValueSize < 0 {
		return nil, invalid("invalid KV config")
	}
	if c.History > 64 || r.Config.GetHistory() > 64 {
		return nil, invalid("KV history exceeds 64")
	}
	// KV 专用 subject 前缀由官方 SDK 管理，source/republish 需要单独的 KV 寻址规则。
	if c.Mirror != nil || len(c.Sources) > 0 || c.RePublish != nil {
		return nil, invalid("KV mirror, sources and republish are not enabled")
	}
	if c.Bucket, err = a.name(c.Bucket); err != nil {
		return nil, err
	}
	if c.MaxValueSize == 0 || int64(c.MaxValueSize) > int64(s.cfg.MaxMessageBytes) {
		// #nosec G115 -- New 将 MaxMessageBytes 限定在 1..64 MiB，必定能表示为 int32。
		c.MaxValueSize = int32(s.cfg.MaxMessageBytes)
	}
	release, err := s.configurationWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	var kv jetstream.KeyValue
	if r.CreateOnly {
		if _, err := s.js.KeyValue(ctx, c.Bucket); err == nil {
			return nil, status.Error(codes.AlreadyExists, "bucket already exists")
		} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, rpcError(err)
		}
		kv, err = s.js.CreateKeyValue(ctx, c)
	} else {
		kv, err = s.js.CreateOrUpdateKeyValue(ctx, c)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.kvStatus(st), nil
}

// GetBucket 返回当前作用域内 KV 桶的配置和存储状态。
func (s *Service) GetBucket(ctx context.Context, r *pb.BucketRef) (*pb.KeyValueStatus, error) {
	a, n, err := s.bucket(ctx, r)
	if err != nil {
		return nil, err
	}
	kv, err := s.js.KeyValue(ctx, n)
	if err != nil {
		return nil, rpcError(err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.kvStatus(st), nil
}

// ListBuckets 流式列出当前租户和 namespace 的 KV 桶，不暴露其他作用域资源。
func (s *Service) ListBuckets(r *pb.Scope, out grpc.ServerStreamingServer[pb.KeyValueStatus]) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	a, err := addressFor(ctx, r)
	if err != nil {
		return err
	}
	list := s.js.KeyValueStores(ctx)
	for st := range list.Status() {
		if !strings.HasPrefix(st.Bucket(), a.names) {
			continue
		}
		if err = out.Send(a.kvStatus(st)); err != nil {
			return rpcError(err)
		}
	}
	return rpcError(list.Error())
}

// DeleteBucket 删除当前作用域的 KV 桶及其中所有版本。
func (s *Service) DeleteBucket(ctx context.Context, r *pb.BucketRef) (*emptypb.Empty, error) {
	_, n, err := s.bucket(ctx, r)
	if err != nil {
		return nil, err
	}
	err = s.js.DeleteKeyValue(ctx, n)
	return &emptypb.Empty{}, rpcError(err)
}

// Put 覆盖写入业务字节并返回新 revision；不存在的 key 会创建。
func (s *Service) Put(ctx context.Context, r *pb.KeyValuePutRequest) (*pb.Revision, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	if len(r.Value) > s.cfg.MaxMessageBytes {
		return nil, statusTooLarge()
	}
	_, kv, k, err := s.key(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	rev, err := kv.Put(ctx, k, r.Value)
	return &pb.Revision{Revision: rev}, rpcError(err)
}

// Create 仅在 key 不存在时写入，可设置单 key TTL；冲突返回 AlreadyExists。
func (s *Service) Create(ctx context.Context, r *pb.KeyValueCreateRequest) (*pb.Revision, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	if len(r.Value) > s.cfg.MaxMessageBytes {
		return nil, statusTooLarge()
	}
	var opts []jetstream.KVCreateOpt
	if r.TtlNs != nil {
		if *r.TtlNs <= 0 {
			return nil, invalid("TTL must be positive")
		}
		opts = append(opts, jetstream.KeyTTL(time.Duration(*r.TtlNs)))
	}
	_, kv, k, err := s.key(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	rev, err := kv.Create(ctx, k, r.Value, opts...)
	return &pb.Revision{Revision: rev}, rpcError(err)
}

// Update 按 expected_revision 执行 CAS；并发写入冲突返回 Aborted。
func (s *Service) Update(ctx context.Context, r *pb.KeyValueUpdateRequest) (*pb.Revision, error) {
	if r == nil || r.ExpectedRevision == 0 {
		return nil, invalid("positive expected revision required")
	}
	if len(r.Value) > s.cfg.MaxMessageBytes {
		return nil, statusTooLarge()
	}
	_, kv, k, err := s.key(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	rev, err := kv.Update(ctx, k, r.Value, r.ExpectedRevision)
	if err != nil {
		return nil, casError(err)
	}
	return &pb.Revision{Revision: rev}, nil
}

// Get 读取最新或指定 revision 的业务字节，空值与不存在的 key 分开处理。
func (s *Service) Get(ctx context.Context, r *pb.KeyValueGetRequest) (*pb.KeyValueEntry, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	if r.Revision != nil && *r.Revision == 0 {
		return nil, invalid("revision must be positive")
	}
	a, kv, k, err := s.key(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	var e jetstream.KeyValueEntry
	if r.Revision != nil {
		e, err = kv.GetRevision(ctx, k, *r.Revision)
	} else {
		e, err = kv.Get(ctx, k)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	return a.kvEntry(e), nil
}

// Delete 写入删除标记，可按 revision 条件删除；不隐式清空历史。
func (s *Service) Delete(ctx context.Context, r *pb.KeyValueDeleteRequest) (*emptypb.Empty, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	if r.PurgeTtlNs != nil {
		return nil, invalid("purge TTL is not a Delete option")
	}
	var opts []jetstream.KVDeleteOpt
	if r.ExpectedRevision != nil {
		if *r.ExpectedRevision == 0 {
			return nil, invalid("revision must be positive")
		}
		opts = append(opts, jetstream.LastRevision(*r.ExpectedRevision))
	}
	_, kv, k, err := s.key(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	err = kv.Delete(ctx, k, opts...)
	if r.ExpectedRevision != nil {
		err = casError(err)
	} else {
		err = rpcError(err)
	}
	return &emptypb.Empty{}, err
}

// History 交付 key 的有限历史快照，保留删除标记和每条 revision。
func (s *Service) History(r *pb.KeyValueHistoryRequest, out grpc.ServerStreamingServer[pb.KeyValueEntry]) error {
	if r == nil {
		return invalid("request required")
	}
	opts, err := watchOptions(r.Options)
	if err != nil {
		return err
	}
	if r.Options != nil && (r.Options.GetUpdatesOnly() || r.Options.ResumeFromRevision != nil) {
		return invalid("history cannot use a live-only start")
	}
	a, kv, k, err := s.key(out.Context(), r.Key)
	if err != nil {
		return err
	}
	// History 使用有限快照；读取期间取消仍传给官方 SDK，不转成后台下载。
	history, err := kv.History(out.Context(), k, opts...)
	if err != nil {
		return rpcError(err)
	}
	for _, e := range history {
		if err = out.Send(a.kvEntry(e)); err != nil {
			return rpcError(err)
		}
	}
	return nil
}

// Watch 订阅 key 变更，单独发送历史初始化结束标记；取消时回收订阅。
func (s *Service) Watch(r *pb.KeyValueWatchRequest, out grpc.ServerStreamingServer[pb.KeyValueWatchEvent]) error {
	if r == nil {
		return invalid("request required")
	}
	opts, err := watchOptions(r.Options)
	if err != nil {
		return err
	}
	keys := r.Keys
	if len(keys) == 0 {
		keys = []string{">"}
	}
	if len(keys) > 64 {
		return invalid("too many key filters")
	}
	for _, k := range keys {
		if !validKey(k, true) {
			return invalid("invalid key filter")
		}
	}
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	a, n, err := s.bucket(ctx, r.Bucket)
	if err != nil {
		return err
	}
	kv, err := s.js.KeyValue(ctx, n)
	if err != nil {
		return rpcError(err)
	}
	watcher, err := kv.WatchFiltered(ctx, keys, opts...)
	if err != nil {
		return rpcError(err)
	}
	defer func() {
		// context 取消和连接关闭均会回收订阅，重复清理不改变 RPC 的最终错误。
		_ = watcher.Stop()
	}()
	for {
		select {
		case <-ctx.Done():
			return rpcError(ctx.Err())
		case e, ok := <-watcher.Updates():
			if !ok {
				if ctx.Err() != nil {
					return rpcError(ctx.Err())
				}
				return statusUnavailable()
			}
			event := &pb.KeyValueWatchEvent{}
			if e == nil {
				event.Event = &pb.KeyValueWatchEvent_Initialized{Initialized: &emptypb.Empty{}}
			} else {
				event.Event = &pb.KeyValueWatchEvent_Entry{Entry: a.kvEntry(e)}
			}
			if err = out.Send(event); err != nil {
				return rpcError(err)
			}
		}
	}
}
