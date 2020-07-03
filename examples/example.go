package examples

import (
	"fmt"
	"time"
)

type Job struct {
	// 节点的唯一标志
	taskID     uint64
	isFinished bool
}

func NewJob(taskID uint64) Job {
	return Job{
		taskID: taskID,
	}
}

func (j *Job) GetTaskID() uint64 {
	return j.taskID
}

func (j *Job) Exec() {
	time.Sleep(time.Second)
	fmt.Printf("taskID[%d] here is exec module\n", j.taskID)
}

func (j *Job) Complete() {
	time.Sleep(time.Second)
	fmt.Printf("taskID[%d] here is complete module\n", j.taskID)
}

func (j *Job) Hashcode() interface{} {
	return j.taskID
}

func (j *Job) IsFinished() bool {
	return j.isFinished
}

func (j *Job) SetFinished(bo bool) {
	j.isFinished = bo
}
