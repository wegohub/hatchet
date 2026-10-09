package main

import (
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
)

// main 运行 mergent 场景，业务断言失败时退出码非零，成功时输出验收结果
func main() {
	scenarios.Main("mergent")
}
