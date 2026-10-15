package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestLimitFlagsBeforeInitialization 以独立进程解析参数，未提供密钥或数据库也应先报告数值错误。
func TestLimitFlagsBeforeInitialization(t *testing.T) {
	if os.Getenv("EXTRA_LIMITS_HELPER") == "1" {
		flag.CommandLine = flag.NewFlagSet("hatchet-extra", flag.ContinueOnError)
		flag.CommandLine.SetOutput(io.Discard)
		os.Args = append([]string{"hatchet-extra"}, strings.Fields(os.Getenv("EXTRA_LIMITS_ARGS"))...)
		fmt.Println(run())
		return
	}
	for _, test := range []struct{ name, args, environment, want string }{
		{"zero_streams", "--max-concurrent-streams=0", "", "invalid RPC admission limit"},
		{"commit_exceeds_abandon", "--batch-commit-timeout=11s", "", "less than batch abandon timeout"},
		{"batch_too_small", "--max-publish-batch-bytes=1000", "", "invalid batch limits"},
		{"legacy_conflict", "--max-concurrent-rpcs=10 --max-concurrent-unary=11", "", "cannot be combined"},
		{"environment_conflict", "--max-concurrent-rpcs=10", "EXTRA_MAX_CONCURRENT_STREAMS=12", "cannot be combined"},
		{"invalid_environment", "", "EXTRA_BATCH_COMMIT_TIMEOUT=bad", "invalid EXTRA_BATCH_COMMIT_TIMEOUT"},
		{"environment_value", "", "EXTRA_MAX_CONCURRENT_READS=0", "invalid RPC admission limit"},
		{"valid_legacy", "--max-concurrent-rpcs=10", "", "EXTRA_ACK_KEY"},
		{"consumer_unlimited", "--max-consumers=0", "", "EXTRA_ACK_KEY"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestLimitFlagsBeforeInitialization$")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "EXTRA_") && !strings.HasPrefix(entry, "DATABASE_") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "EXTRA_LIMITS_HELPER=1", "EXTRA_LIMITS_ARGS="+test.args, "SERVER_SECURITY_CHECK_ENABLED=false")
			if test.environment != "" {
				cmd.Env = append(cmd.Env, test.environment)
			}
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), test.want) {
				t.Fatalf("result=%s error=%v want=%s", out, err, test.want)
			}
		})
	}
}
