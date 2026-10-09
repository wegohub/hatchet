package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
)

// main 运行真实 MinIO codec 示例；成功输出不含凭证的断言和 RunID，失败返回非零退出码
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	report := &scenarios.Report{}
	err := scenarios.MinIOCodec(ctx, report)
	data, encodeErr := json.MarshalIndent(report.Records, "", "  ")
	if encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	fmt.Println(string(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
