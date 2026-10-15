package extra

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// checkTimes 在调用 NATS 前拒绝非法 Timestamp；AsTime 本身会规范化而不是报错。
func checkTimes(m protoreflect.Message) error {
	var err error
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.Kind() != protoreflect.MessageKind {
			return true
		}
		check := func(m protoreflect.Message) bool {
			if t, ok := m.Interface().(*timestamppb.Timestamp); ok {
				if t.CheckValid() != nil {
					err = invalid("invalid timestamp")
					return false
				}
			} else {
				err = checkTimes(m)
			}
			return err == nil
		}
		switch {
		case f.IsMap():
			v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
				if f.MapValue().Kind() == protoreflect.MessageKind {
					return check(v.Message())
				}
				return true
			})
		case f.IsList():
			for i := 0; i < v.List().Len(); i++ {
				if !check(v.List().Get(i).Message()) {
					break
				}
			}
		default:
			check(v.Message())
		}
		return err == nil
	})
	return err
}

// mapStreamConfig 映射所有会产生订阅或发布的嵌套 subject，拒绝外部账户与 domain。
func (a address) mapStreamConfig(c *jetstream.StreamConfig) error {
	var err error
	if c.Retention < jetstream.LimitsPolicy ||
		c.Retention > jetstream.WorkQueuePolicy ||
		c.Storage < jetstream.FileStorage ||
		c.Storage > jetstream.MemoryStorage ||
		c.Discard < jetstream.DiscardOld ||
		c.Discard > jetstream.DiscardNew ||
		c.Compression > jetstream.S2Compression ||
		c.PersistMode < jetstream.DefaultPersistMode ||
		c.PersistMode > jetstream.AsyncPersistMode {
		return invalid("invalid stream policy")
	}
	if c.MaxAge < 0 ||
		c.Duplicates < 0 ||
		c.MaxMsgs < -1 ||
		c.MaxBytes < -1 ||
		c.MaxConsumers < -1 ||
		c.MaxMsgsPerSubject < -1 ||
		c.MaxMsgSize < 0 ||
		c.Replicas < 0 ||
		c.Replicas > 5 ||
		c.SubjectDeleteMarkerTTL < 0 {
		return invalid("invalid stream limits")
	}
	if c.Template != "" || c.NoAck {
		return invalid("template and no_ack are not supported")
	}
	if c.Name, err = a.name(c.Name); err != nil {
		return err
	}
	for i, v := range c.Subjects {
		if c.Subjects[i], err = a.subject(v, true); err != nil {
			return err
		}
	}
	for _, src := range append([]*jetstream.StreamSource{c.Mirror}, c.Sources...) {
		if src == nil {
			continue
		}
		// 跨账户 API 前缀会绕过租户映射；首版只允许本实例、本租户的 source/mirror。
		if src.External != nil || src.Domain != "" || src.Consumer != nil {
			return invalid("external stream source is not supported")
		}
		if src.Name, err = a.name(src.Name); err != nil {
			return err
		}
		if src.FilterSubject != "" {
			if src.FilterSubject, err = a.subject(src.FilterSubject, true); err != nil {
				return err
			}
		}
		for i := range src.SubjectTransforms {
			if err = a.mapTransform(&src.SubjectTransforms[i]); err != nil {
				return err
			}
		}
	}
	if c.SubjectTransform != nil {
		if err = a.mapTransform(c.SubjectTransform); err != nil {
			return err
		}
	}
	if c.RePublish != nil {
		if c.RePublish.Source, err = a.subject(c.RePublish.Source, true); err != nil {
			return err
		}
		if c.RePublish.Destination, err = a.subject(c.RePublish.Destination, true); err != nil {
			return err
		}
	}
	return nil
}

// mapTransform 把转换源与目标都限定在当前租户作用域。
func (a address) mapTransform(t *jetstream.SubjectTransformConfig) error {
	var err error
	if t.Source, err = a.subject(t.Source, true); err != nil {
		return err
	}
	t.Destination, err = a.subject(t.Destination, true)
	return err
}

// streamInfo 将 stream 配置和状态中的物理名称还原为业务逻辑名称。
func (a address) streamInfo(i *jetstream.StreamInfo) *pb.NatsStreamInfo {
	p := encodeStreamInfo(i)
	p.Config.Name = ptr(a.logicalName(i.Config.Name))
	for k, sub := range p.Config.Subjects {
		p.Config.Subjects[k] = a.logicalSubject(sub)
	}
	logicalSource := func(src *pb.NatsStreamSource) {
		if src == nil {
			return
		}
		src.Name = ptr(a.logicalName(src.GetName()))
		if src.FilterSubject != nil {
			src.FilterSubject = ptr(a.logicalSubject(src.GetFilterSubject()))
		}
		for _, t := range src.SubjectTransforms {
			t.Source = ptr(a.logicalSubject(t.GetSource()))
			t.Destination = ptr(a.logicalSubject(t.GetDestination()))
		}
	}
	logicalSource(p.Config.Mirror)
	for _, src := range p.Config.Sources {
		logicalSource(src)
	}
	if t := p.Config.SubjectTransform; t != nil {
		t.Source = ptr(a.logicalSubject(t.GetSource()))
		t.Destination = ptr(a.logicalSubject(t.GetDestination()))
	}
	if r := p.Config.RePublish; r != nil {
		r.Source = ptr(a.logicalSubject(r.GetSource()))
		r.Destination = ptr(a.logicalSubject(r.GetDestination()))
	}
	if p.Mirror != nil {
		p.Mirror.Name = ptr(a.logicalName(p.Mirror.GetName()))
		p.Mirror.FilterSubject = ptr(a.logicalSubject(p.Mirror.GetFilterSubject()))
		a.logicalTransforms(p.Mirror.SubjectTransforms)
	}
	for _, src := range p.Sources {
		src.Name = ptr(a.logicalName(src.GetName()))
		src.FilterSubject = ptr(a.logicalSubject(src.GetFilterSubject()))
		a.logicalTransforms(src.SubjectTransforms)
	}
	if p.State != nil && p.State.Subjects != nil {
		subjects := make(map[string]uint64, len(p.State.Subjects))
		for k, v := range p.State.Subjects {
			subjects[a.logicalSubject(k)] = v
		}
		p.State.Subjects = subjects
	}
	return p
}

// logicalTransforms 将运行状态中每个转换地址还原为逻辑 subject，不暴露物理租户前缀。
func (a address) logicalTransforms(transforms []*pb.NatsSubjectTransformConfig) {
	for _, transform := range transforms {
		if transform != nil {
			transform.Source = ptr(a.logicalSubject(transform.GetSource()))
			transform.Destination = ptr(a.logicalSubject(transform.GetDestination()))
		}
	}
}

// consumerConfig 校验 pull consumer 策略并映射所有过滤器，禁止 push 和 pinned 会话。
func (a address) consumerConfig(c *jetstream.ConsumerConfig) error {
	if !validName(c.Name) && !validName(c.Durable) {
		return invalid("named durable consumer required")
	}
	if c.Name != "" && !validName(c.Name) || c.Durable != "" && !validName(c.Durable) {
		return invalid("invalid consumer name")
	}
	if c.Name != "" && c.Durable != "" && c.Name != c.Durable {
		return invalid("consumer name and durable differ")
	}
	if c.Durable == "" {
		c.Durable = c.Name
	}
	if c.Name == "" {
		c.Name = c.Durable
	}
	if c.DeliverSubject != "" || c.DeliverGroup != "" || c.FlowControl || c.IdleHeartbeat != 0 {
		return invalid("push consumer is not enabled")
	}
	if c.DeliverPolicy < jetstream.DeliverAllPolicy ||
		c.DeliverPolicy > jetstream.DeliverLastPerSubjectPolicy ||
		c.AckPolicy < jetstream.AckExplicitPolicy ||
		c.AckPolicy > jetstream.AckFlowControlPolicy ||
		c.ReplayPolicy < jetstream.ReplayInstantPolicy ||
		c.ReplayPolicy > jetstream.ReplayOriginalPolicy {
		return invalid("invalid consumer policy")
	}
	if c.AckPolicy == jetstream.AckFlowControlPolicy {
		return invalid("flow-control ACK requires push")
	}
	if c.PriorityPolicy < jetstream.PriorityPolicyNone || c.PriorityPolicy > jetstream.PriorityPolicyPrioritized {
		return invalid("invalid priority policy")
	}
	if c.PriorityPolicy == jetstream.PriorityPolicyPinned {
		return invalid("pinned consumers are not enabled")
	}
	if c.Replicas < 0 ||
		c.Replicas > 5 ||
		c.MaxDeliver < -1 ||
		c.MaxAckPending < -1 ||
		c.AckWait < 0 ||
		c.MaxRequestExpires < 0 ||
		c.InactiveThreshold < 0 ||
		c.PinnedTTL < 0 ||
		c.MaxWaiting < 0 ||
		c.MaxRequestBatch < 0 ||
		c.MaxRequestMaxBytes < 0 {
		return invalid("invalid consumer limits")
	}
	for _, d := range c.BackOff {
		if d <= 0 {
			return invalid("backoff must be positive")
		}
	}
	var err error
	if c.FilterSubject != "" {
		c.FilterSubject, err = a.subject(c.FilterSubject, true)
		if err != nil {
			return err
		}
	}
	for i, v := range c.FilterSubjects {
		c.FilterSubjects[i], err = a.subject(v, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// consumerInfo 复制原生消费者信息并还原资源名和过滤 subject。
func (a address) consumerInfo(i *jetstream.ConsumerInfo) *pb.NatsConsumerInfo {
	p := encodeConsumerInfo(i)
	p.Stream = ptr(a.logicalName(i.Stream))
	if p.Config.FilterSubject != nil {
		p.Config.FilterSubject = ptr(a.logicalSubject(p.Config.GetFilterSubject()))
	}
	for k, v := range p.Config.FilterSubjects {
		p.Config.FilterSubjects[k] = a.logicalSubject(v)
	}
	return p
}

// validKey 限制 key 长度和字符；通配符仅用于 Watch 过滤器。
func validKey(k string, wild bool) bool {
	if k == "" || len(k) > 512 {
		return false
	}
	for _, c := range k {
		valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_/=.", c) || wild && (c == '*' || c == '>')
		if !valid {
			return false
		}
	}
	if wild {
		return validSubject(k, true)
	}
	return validSubject(k, false)
}

// watchOptions 校验互斥的历史、实时和 revision 起点选项，再构造原生订阅参数。
func watchOptions(p *pb.WatchOptions) ([]jetstream.WatchOpt, error) {
	var o []jetstream.WatchOpt
	if p == nil {
		return o, nil
	}
	if p.IncludeHistory && p.UpdatesOnly || p.ResumeFromRevision != nil && (p.IncludeHistory || p.UpdatesOnly) {
		return nil, invalid("watch start options conflict")
	}
	if p.IncludeHistory {
		o = append(o, jetstream.IncludeHistory())
	}
	if p.IgnoreDeletes {
		o = append(o, jetstream.IgnoreDeletes())
	}
	if p.UpdatesOnly {
		o = append(o, jetstream.UpdatesOnly())
	}
	if p.MetadataOnly {
		o = append(o, jetstream.MetaOnly())
	}
	if p.ResumeFromRevision != nil {
		if *p.ResumeFromRevision == 0 {
			return nil, invalid("resume revision must be positive")
		}
		o = append(o, jetstream.ResumeFromRevision(*p.ResumeFromRevision))
	}
	return o, nil
}

// 非业务的长等待也必须有界，例如单次 Fetch 最多等待 30 秒；连续消费由客户端循环。
const maxFetchWait = 30 * time.Second
