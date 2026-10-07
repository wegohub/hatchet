package scenarios

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// TaskName 使用稳定的方法映射，保证示例中的管理查询与实际任务注册名称一致。
func TaskName(method string) string {
	// hash 对实际数据计算稳定摘要，用于关联内容或显式断言身份。
	hash := sha256.Sum256([]byte(method))
	return "wego-rpc-" + strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "-")) + "-" + hex.EncodeToString(hash[:6])
}
