#!/bin/sh
set -eu
# 解析脚本位置，输入始终来自仓库，输出可指向质量测试的临时目录。
sdk_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo_dir=$(CDPATH= cd -- "$sdk_dir/../.." && pwd)
export PATH="$sdk_dir/.tools/bin:$PATH"
# 三个工具版本必须精确匹配；不允许用系统中的其他版本静默生成。
[ "$(protoc --version)" = 'libprotoc 29.6' ]
[ "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.12' ]
[ "$(protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc 1.5.1' ]
# WEGO_GENERATE_OUTPUT 用于复现检查，不覆盖仓库中待比较的文件。
output_dir=${WEGO_GENERATE_OUTPUT:-$repo_dir}
mkdir -p "$output_dir"
cd "$repo_dir"
# 只生成 wego 流协议和示例业务桩，避免把后端协议引入公开 API。
protoc --go_out="$output_dir" --go_opt=module=github.com/hatchet-dev/hatchet sdks/wego/internal/wire/stream.proto
protoc --go_out="$output_dir" --go_opt=module=github.com/hatchet-dev/hatchet --go-grpc_out="$output_dir" --go-grpc_opt=module=github.com/hatchet-dev/hatchet sdks/wego/examples/proto/demo.proto

# 业务说明由 proto 生成；运行时缓存与适配器说明由固定后处理补齐，质量门禁同时校验。
python3 "$sdk_dir/scripts/comment-generated.py" \
  "$output_dir/sdks/wego/internal/wire/stream.pb.go" \
  "$output_dir/sdks/wego/examples/proto/demo.pb.go" \
  "$output_dir/sdks/wego/examples/proto/demo_grpc.pb.go"
gofmt -w "$output_dir/sdks/wego/internal/wire/stream.pb.go" \
  "$output_dir/sdks/wego/examples/proto/demo.pb.go" \
  "$output_dir/sdks/wego/examples/proto/demo_grpc.pb.go"
