package backend

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// decodeArgument 根据 handler 参数分配目标；*protobuf 与普通 Go 指针均保持实际签名。
func decodeArgument(ctx context.Context, raw any, typ reflect.Type, config spec.Runtime) (reflect.Value, error) {
	envelope, err := wire.AsEnvelope(raw)
	if err != nil {
		return reflect.Value{}, err
	}
	value := reflect.New(typ)
	target := value.Interface()
	if typ.Kind() == reflect.Pointer && typ.Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
		value.Elem().Set(reflect.New(typ.Elem()))
		target = value.Elem().Interface()
	}
	limits := config.PayloadLimits
	budget, cancel := context.WithTimeout(ctx, limits.DecodeTimeout)
	defer cancel()
	if err := wire.DecodeValue(budget, "", envelope, target, config, limits.MaxMessageBytes, limits.MaxEncodedBytes); err != nil {
		return reflect.Value{}, err
	}
	return value.Elem(), nil
}

// encodeOutput 普通任务与 protobuf 任务统一上报封包，不把业务 JSON 交给引擎再次浮点转换。
func encodeOutput(ctx context.Context, value any, config spec.Runtime, name string) (wire.Envelope, error) {
	limits := config.PayloadLimits
	plain, typeName, err := wire.MarshalValue(value, limits.MaxMessageBytes)
	if err != nil {
		return wire.Envelope{}, err
	}
	envelope := wire.NewEnvelope(ctx, "", typeName, plain, nil)

	budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
	defer cancel()
	return wire.Output(budget, envelope, config, limits.MaxMessageBytes, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
}

// invokePayload 单条输入先恢复上下文；Batch 则逐成员还原，不能把聚合 map 当作一个业务消息。
func invokePayload(fn any, ctx context.Context, input any, config spec.Runtime, name string, policy spec.Task, workflowName string) (any, error) {
	if err := validateHandler(fn); err != nil {
		return nil, err
	}
	function := reflect.ValueOf(fn)
	typ := function.Type().In(1)
	// argument 在所有成员完成解码后才交给业务；失败不执行部分 Batch。
	var argument reflect.Value
	// memberContracts 保存一次解析得到的响应模式，结果阶段不再读取或猜测原始输入。
	memberContracts := map[string]bool{}
	if policy.Batch == nil {
		envelope, err := wire.AsEnvelope(input)
		if err != nil {
			return nil, err
		}
		if err := validateNativeBinding(envelope); err != nil {
			return nil, err
		}
		// 执行预算包含 codec I/O，已过期任务不再下载输入。
		incoming, closeIncoming := wire.Incoming(ctx, envelope)
		defer closeIncoming()
		ctx = incoming
		budget, cancel := context.WithTimeout(ctx, config.PayloadLimits.DecodeTimeout)
		opened, err := wire.Open(budget, "", envelope, config, config.PayloadLimits.MaxMessageBytes, config.PayloadLimits.MaxEncodedBytes)
		cancel()
		if err != nil {
			return nil, err
		}
		argument, err = decodeArgument(ctx, opened, typ, config)
		if err != nil {
			return nil, err
		}
	} else {
		// err 保存整批解码错误；失败时不执行部分成员。
		var err error
		argument, memberContracts, err = decodeBatchArguments(ctx, input, typ, config)
		if err != nil {
			return nil, err
		}
	}
	results := function.Call([]reflect.Value{reflect.ValueOf(ctx), argument})
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}
	output := results[0].Interface()
	if policy.Batch == nil {
		return encodeOutput(ctx, output, config, name)
	}
	mapped, err := batchResults(input, output, policy.Batch.BroadcastOutput)
	if err != nil {
		return nil, err
	}
	encoded := make(map[string]any, len(mapped))
	for id, value := range mapped {
		memberCtx := wire.WithResponse(ctx, memberContracts[id])
		envelope, err := encodeOutput(memberCtx, value, config, name)
		if err != nil {
			return nil, err
		}
		encoded[id] = envelope
	}
	return encoded, nil
}

// decodeBatchArguments 在调用 handler 前还原全部成员，按真实任务 ID 保存各自响应契约。
func decodeBatchArguments(ctx context.Context, input any, typ reflect.Type, config spec.Runtime) (reflect.Value, map[string]bool, error) {
	contracts := make(map[string]bool)
	members, ok := input.(map[string]any)
	if !ok || typ.Kind() != reflect.Map || typ.Key().Kind() != reflect.String {
		return reflect.Value{}, nil, fmt.Errorf("wego: batch handler requires a string-keyed member map")
	}
	argument := reflect.MakeMapWithSize(typ, len(members))
	for id, raw := range members {
		packet, err := wire.AsEnvelope(raw)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		if err := validateNativeBinding(packet); err != nil {
			return reflect.Value{}, nil, err
		}
		contracts[id] = packet.Response == "wego"
		value, err := decodeArgument(ctx, packet, typ.Elem(), config)
		if err != nil {
			return reflect.Value{}, nil, fmt.Errorf("wego: batch member %s: %w", id, err)
		}
		argument.SetMapIndex(reflect.ValueOf(id).Convert(typ.Key()), value)
	}
	return argument, contracts, nil
}

// validateNativeBinding 只检查本地显式绑定；消息目标由实际 workflow/action 决定。
func validateNativeBinding(packet wire.Envelope) error {
	if packet.Method != "" || packet.Stream != "" {
		return fmt.Errorf("wego: ordinary task cannot consume a local RPC submission")
	}
	return nil
}
