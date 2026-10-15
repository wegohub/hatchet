package extra

import "context"

// configurationWrite 在等待配置写入名额时遵守取消，所有创建/upsert 共用同一名额。
// embedded NATS 只有本实例可访问，查询存在性与创建之间不会有其他配置写入者。
func (s *Service) configurationWrite(ctx context.Context) (func(), error) {
	select {
	case s.resourceSlots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-s.resourceSlots
			return nil, rpcError(err)
		}
		return func() { <-s.resourceSlots }, nil
	case <-ctx.Done():
		return nil, rpcError(ctx.Err())
	}
}
