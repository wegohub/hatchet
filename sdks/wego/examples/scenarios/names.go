package scenarios

import "strings"

// TaskName 使用公开命名规则，保留 protobuf 服务和方法的大小写
func TaskName(method string) string {
	return strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "-")
}
