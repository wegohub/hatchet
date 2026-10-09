//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
)

// TestMinIOCodec 与独立示例执行相同真实断言，必须连接 Hatchet 和 S3，缺少服务或凭证明确失败
func TestMinIOCodec(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn, err := client.New(client.WithRuntime(scenarios.Runtime("codec_preflight")...))
	if err != nil {
		t.Fatal(err)
	}
	info, infoErr := conn.Info(ctx)
	closeErr := conn.Close()
	if infoErr != nil || closeErr != nil {
		t.Fatalf("engine version/connection cleanup: %v / %v", infoErr, closeErr)
	}
	report := &scenarios.Report{}
	if err := scenarios.MinIOCodec(ctx, report); err != nil {
		t.Fatal(err)
	}
	if report.Count() < 12 {
		t.Fatalf("codec evidence incomplete: %d records", report.Count())
	}
	// 只在全部断言及 Worker/连接清理完成后保存成功证据；对象随持久历史保留
	writeStreamEvidence(t, "minio-codec", map[string]any{
		"records":        report.Records,
		"server_version": info.Version,
		"storage_client": "AWS SDK for Go v2 / service/s3",
		"cleanup":        "workers and connections stopped; definitions and standalone probe deleted; referenced S3 objects retained with run/topic history",
	})
}
