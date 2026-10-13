package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/message"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// staticInput 保存逻辑摘要和已准备的密文封包；schema 不保存解密后的业务值。
type staticInput struct {
	// Digest 根据 codec 前数据和稳定格式计算，随机 nonce、对象地址不参与。
	Digest string `json:"digest"`
	// Packet 是注册时真正使用的载体，多副本复用以保持 checksum 稳定。
	Packet wire.Envelope `json:"packet"`
}

// hasStaticInputs 判断定义是否需要静态载体复用，不为没有触发输入的定义增加 API 查询。
func hasStaticInputs(req *v1.CreateWorkflowVersionRequest) bool {
	return req.CronInput != nil || len(req.DefaultFilters) != 0
}

// reuseStaticInputs 从当前定义复用相同 Cron/Filter 封包，不能用固定加密 nonce 稳定定义。
// 官方没有原子比较注册接口，并发首次注册仍可能生成多个版本。
func (b *Backend) reuseStaticInputs(ctx context.Context, req *v1.CreateWorkflowVersionRequest) error {
	if !hasStaticInputs(req) {
		return nil
	}
	// previous 只来自当前版本，按名称缓存不能发现定义删除或变更。
	previous := map[string]staticInput{}
	id, err := b.currentWorkflowID(ctx, req.Name)
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	if err == nil {
		version, err := b.raw.API().WorkflowVersionGetWithResponse(ctx, id, nil)
		if err != nil {
			return Normalize(err)
		}
		if err := responseStatus(version.StatusCode(), version.JSON200 != nil); err != nil {
			return err
		}
		if version.JSON200.InputJsonSchema != nil {
			data, err := json.Marshal((*version.JSON200.InputJsonSchema)["x-wego-static-inputs"])
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &previous); err != nil {
				return fmt.Errorf("wego: malformed registered static inputs: %w", err)
			}
		}
	}
	prepared := map[string]staticInput{}
	prepare := func(key, data string) (string, error) {
		return b.prepareStaticInput(ctx, key, data, previous, prepared)
	}
	if req.CronInput != nil {
		encoded, err := prepare("cron", *req.CronInput)
		if err != nil {
			return err
		}
		req.CronInput = &encoded
	}
	for index, filter := range req.DefaultFilters {
		encoded, err := prepare("filter:"+strconv.Itoa(index), string(filter.Payload))
		if err != nil {
			return err
		}
		filter.Payload = []byte(encoded)
	}
	// schema 与触发配置引用同一份载体，之后的重连读取这些已准备字节。
	var schema map[string]any
	if err := json.Unmarshal(req.InputJsonSchema, &schema); err != nil {
		return err
	}
	schema["x-wego-static-inputs"] = prepared
	req.InputJsonSchema, err = json.Marshal(schema)
	return err
}

// prepareStaticInput 复用逻辑相同且真实可解码的历史封包；随机 nonce 不参与逻辑比较。
func (b *Backend) prepareStaticInput(ctx context.Context, key, data string, previous, prepared map[string]staticInput) (string, error) {
	packet, err := wire.AsEnvelope(data)
	if err != nil {
		return "", err
	}
	digest, err := b.staticDigest(ctx, packet)
	if err != nil {
		return "", err
	}
	if saved, ok := previous[key]; ok && saved.Digest == digest {
		// 摘要字段不构成认证；旧对象必须真实可还原，并重新核验业务身份。
		actual, err := b.staticDigest(ctx, saved.Packet)
		if err != nil {
			return "", err
		}
		if actual != digest {
			return "", status.Error(codes.DataLoss, "wego: registered static input digest differs")
		}
		packet = saved.Packet
	}
	prepared[key] = staticInput{Digest: digest, Packet: packet}
	encoded, err := json.Marshal(packet)
	return string(encoded), err
}

// staticDigest 使用 JSON 数值保真解码生成稳定摘要，并纳入 codec 顺序与 metadata。
func (b *Backend) staticDigest(ctx context.Context, packet wire.Envelope) (string, error) {
	limits := b.config.PayloadLimits
	budget, cancel := context.WithTimeout(ctx, limits.DecodeTimeout)
	defer cancel()
	opened, err := wire.Open(budget, packet.Method, packet, b.config, limits.MaxMessageBytes, limits.MaxEncodedBytes)
	if err != nil {
		return "", err
	}
	// value 是业务 JSON 的规范快照，9007199254740993 不转换为 float64。
	var value any
	decoder := json.NewDecoder(bytes.NewReader(opened.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	ids, err := wire.PayloadCodecIDs(packet)
	if err != nil {
		return "", err
	}
	if len(opened.Metadata) == 0 {
		opened.Metadata = nil
	}
	if len(opened.Routing) == 0 {
		opened.Routing = nil
	}
	canonical, err := json.Marshal(map[string]any{"value": value, "routing": opened.Routing, "metadata": opened.Metadata, "codecs": ids})
	if err != nil {
		return "", err
	}
	canonical, err = message.Canonical(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
