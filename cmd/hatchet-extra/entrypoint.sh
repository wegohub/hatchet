#!/bin/sh
set -eu

# 初始化容器才接触 Hatchet 加密配置；服务容器仅挂载导出的公钥与自身密钥。
if [ "${1:-}" = "init" ]; then
    umask 077
    mkdir -p /extra-config /data
    hatchet-extra export-public-key \
        --config /hatchet/config/server.yaml \
        --output /extra-config/jwt-public.key.tmp
    mv /extra-config/jwt-public.key.tmp /extra-config/jwt-public.key
    # ACK 密钥跨重启保持不变，否则已投递消息的确认 token 将失效。
    if [ ! -s /extra-config/ack.key ]; then
        openssl rand -base64 32 > /extra-config/ack.key.tmp
        mv /extra-config/ack.key.tmp /extra-config/ack.key
    fi
    if [ ! -s /extra-config/tls.key ] || [ ! -s /extra-config/tls.crt ]; then
        openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1095 \
            -subj '/CN=hatchet-extra' \
            -addext 'subjectAltName=DNS:localhost,DNS:hatchet-extra,IP:127.0.0.1' \
            -keyout /extra-config/tls.key.tmp -out /extra-config/tls.crt.tmp 2>/dev/null
        mv /extra-config/tls.key.tmp /extra-config/tls.key
        mv /extra-config/tls.crt.tmp /extra-config/tls.crt
    fi
    chmod 600 /extra-config/*
    chown 10001:10001 /extra-config /extra-config/* /data
    exit 0
fi

# exec 使 SIGTERM 直接交给 Go 进程，由其按预算排空；不输出密钥内容。
export EXTRA_ACK_KEY="$(cat /extra-config/ack.key)"
exec hatchet-extra "$@"
