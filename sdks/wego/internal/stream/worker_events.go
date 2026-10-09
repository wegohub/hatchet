package stream

// BroadcastTopic 是同租户、namespace 的共享广播日志，不使用 workflow 或额外 Worker
const BroadcastTopic = "workers.broadcast"

// WorkerTopic 使用当前随机进程逻辑身份生成定向地址
func WorkerTopic(workerKey string) string { return "worker." + workerKey }
