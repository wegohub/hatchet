#!/bin/sh
set -eu
# SDK 目录与调用时的工作目录无关，工具统一安装到 .tools/bin。
sdk_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# 匹配操作系统和架构，例如 Darwin/arm64 选择 osx-aarch_64 的固定发行包。
case "$(uname -s)" in Darwin) platform=osx;; Linux) platform=linux;; *) echo 'Unsupported OS' >&2; exit 1;; esac
case "$(uname -m)" in arm64|aarch64) arch=aarch_64;; x86_64) arch=x86_64;; *) echo 'Unsupported architecture' >&2; exit 1;; esac
# 下载目录只用于本次安装，成功或失败退出都通过 trap 清理。
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT
# 固定 protoc 与两个 Go 插件版本，避免不同机器生成不同的协议代码。
curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v29.6/protoc-29.6-$platform-$arch.zip" -o "$temp_dir/protoc.zip"
unzip -q "$temp_dir/protoc.zip" -d "$temp_dir/protoc"
mkdir -p "$sdk_dir/.tools/bin"
cp "$temp_dir/protoc/bin/protoc" "$sdk_dir/.tools/bin/"
GOBIN="$sdk_dir/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$sdk_dir/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
