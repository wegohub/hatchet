// Package features 适配 Cron、Schedule、Event 等管理入口，并将运行预算交给内部引擎公开 features 包只导出业务类型；例如发布事件前先登记在途编码，关闭连接会等待或取消这次编码与提交
package features
