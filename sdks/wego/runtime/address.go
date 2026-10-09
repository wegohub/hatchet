package runtime

import (
	"net"
	"strconv"
)

// fmtHostPort 把主机与端口组成 gRPC 地址；例如 ::1 和 7077 得到 [::1]:7077
func fmtHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}
