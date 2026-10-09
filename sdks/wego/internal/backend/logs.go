package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	eventpb "github.com/hatchet-dev/hatchet/internal/services/ingestor/contracts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
)

// logRecord 保留时间、结构化属性和原始 retry，通过已有连接调用官方 PutLog。
func (b *Backend) logRecord(ctx context.Context, id string, retry int, timeout time.Duration, record slog.Record) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.BeginLogIO != nil {
		tracked, done, err := b.BeginLogIO(ctx)
		if err != nil {
			return err
		}
		defer done()
		ctx = tracked
	}
	// 官方 message 限制为 1..10000 个字符；不截断，避免改变业务日志含义。
	if !utf8.ValidString(record.Message) || utf8.RuneCountInString(record.Message) < 1 || utf8.RuneCountInString(record.Message) > 10000 || strings.ContainsRune(record.Message, 0) {
		return status.Error(codes.InvalidArgument, "wego: log message must contain 1..10000 valid characters without NUL")
	}
	metadata, err := logging.Metadata(record)
	if err != nil {
		return fmt.Errorf("wego: log metadata: %w", err)
	}
	// PostgreSQL JSONB 不接受字符串中的 NUL，JSON 解码后再检查，不误拒绝普通反斜杠文本。
	decoder := json.NewDecoder(strings.NewReader(string(metadata)))
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("wego: log metadata: %w", err)
		}
		if text, ok := token.(string); ok && strings.ContainsRune(text, 0) {
			return status.Error(codes.InvalidArgument, "wego: log metadata contains NUL")
		}
	}
	level, count := record.Level.String(), int32(retry)
	req := &eventpb.PutLogRequest{TaskRunExternalId: id, CreatedAt: timestamppb.New(record.Time), Message: record.Message, Level: &level, Metadata: string(metadata), TaskRetryCount: &count}
	if proto.Size(req) > b.config.BackendMessageLimit {
		return status.Error(codes.ResourceExhausted, "wego: log request exceeds backend message limit")
	}
	_, err = eventpb.NewEventsServiceClient(b.rpcConn).PutLog(b.auth(ctx), req)
	return Normalize(err)
}
