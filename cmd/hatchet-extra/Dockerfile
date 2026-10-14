# syntax=docker/dockerfile:1
# 构建上下文只包含源码；密钥在部署初始化时创建，不进入镜像层。
FROM golang:1.26.7-alpine@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api ./api
COPY api-contracts ./api-contracts
COPY internal ./internal
COPY pkg ./pkg
COPY cmd/hatchet-extra ./cmd/hatchet-extra
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOWORK=off CGO_ENABLED=0 go build -p=4 -trimpath -ldflags='-s -w' \
      -o /out/hatchet-extra ./cmd/hatchet-extra

FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
RUN apk add --no-cache ca-certificates openssl \
    && adduser -D -u 10001 extra \
    && mkdir /data /extra-config \
    && chown extra:extra /data /extra-config
COPY --from=build /out/hatchet-extra /usr/local/bin/hatchet-extra
COPY --chmod=755 cmd/hatchet-extra/entrypoint.sh /usr/local/bin/extra-entrypoint
USER extra
EXPOSE 7080
ENTRYPOINT ["/usr/local/bin/extra-entrypoint"]
