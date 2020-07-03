package dagflow

import (
	"github.com/hashicorp/terraform/dag"
)

type JobNode interface {
	// 事件处理
	Exec()
	// 事件处理完成后调用
	Complete()
	// 函数唯一编号
	dag.Hashable
	// 是否完成
	IsFinished() bool
	// 设置结果
	SetFinished(bo bool)
	// for test
	GetTaskID() uint64
}
