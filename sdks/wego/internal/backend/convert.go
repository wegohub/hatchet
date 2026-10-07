package backend

import (
	"encoding/json"
)

// convert 通过 JSON 复制值，隔离后端 DTO，避免动态结果泄露 Hatchet 对象。
func convert(source, dest any) error {
	// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, dest)
}
