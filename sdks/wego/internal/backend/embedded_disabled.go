//go:build !wego_embedded

package backend

import (
	"context"
	"fmt"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// startEmbedded 未启用 wego_embedded build tag 时拒绝嵌入配置，错误明确说明所需构建条件
func startEmbedded(c spec.Runtime) (spec.Runtime, func(context.Context) error, error) {
	return c, nil, fmt.Errorf("wego: embedded mode requires the wego_embedded build tag")
}
