#!/bin/sh
set -eu
# 输出目录可指向临时位置，用于复现检查，不修改待比较文件。
extra_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=$(python3 -c 'import os,sys;print(os.environ.get("EXTRA_GENERATE_OUTPUT",sys.argv[1]))' "$extra_dir")
repo_dir=$(CDPATH= cd -- "$extra_dir/../../.." && pwd)
if [ -n "${EXTRA_GENERATE_OUTPUT:-}" ]; then
    public_output=${EXTRA_GENERATE_PUBLIC_OUTPUT:-"$output/pkg/extra/v1"}
else
    public_output=${EXTRA_GENERATE_PUBLIC_OUTPUT:-"$repo_dir/pkg/extra/v1"}
fi
mkdir -p "$public_output"
[ "$(protoc --version)" = 'libprotoc 29.6' ]
[ "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.12' ]
[ "$(protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc 1.5.1' ]
protoc -I "$extra_dir/proto/v1" \
 --go_out="$public_output" --go_opt=paths=source_relative \
 --go-grpc_out="$public_output" --go-grpc_opt=paths=source_relative \
 "$extra_dir/proto/v1/extra.proto"
python3 "$extra_dir/scripts/generate-convert.py"
