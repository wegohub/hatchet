// Package backend 是唯一允许依赖 Hatchet 实现的包
// 调度、执行上下文和资源管理在此转换，返回值及错误不得携带 Hatchet 类型
package backend
