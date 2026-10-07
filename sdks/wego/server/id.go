package server

import (
	"crypto/rand"
	"encoding/hex"
)

// randomID 生成随机实例标识，避免不同 Worker 共用会话 owner 标签。
func randomID() string {
	// data 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
	data := make([]byte, 6)
	_, _ = rand.Read(data)
	return hex.EncodeToString(data)
}
