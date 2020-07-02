package dagflow

type Job struct {
	node       Node
	preJobs    *[]Job
	nextJobs   *[]Job
	isFinished bool
}

type Node interface {
	// 事件处理
	Exec()
	// 事件处理完成后调用
	Complete()
}

func (j *Job) init() {
	if j.preJobs == nil {
		j.preJobs = new([]Job)
	}

	if j.nextJobs == nil {
		j.nextJobs = new([]Job)
	}
}

func (j *Job) GetNode() Node {
	return j.node
}

func (j *Job) GetPreJobs() *[]Job {
	return j.preJobs
}

func (j *Job) GetNextJobs() *[]Job {
	return j.nextJobs
}

func (j *Job) IsFinished() bool {
	return j.isFinished
}

func (j *Job) AddNode(node Node) {
	j.init()

	j.node = node
}

func (j *Job) AddPreJob(preJob *Job) {
	j.init()

	*j.preJobs = append(*j.preJobs, *preJob)
}

func (j *Job) AddNextJob(nextJob *Job) {
	j.init()

	*j.nextJobs = append(*j.nextJobs, *nextJob)
}

func (j *Job) IncludePreJob(job *Job) bool {
	j.init()

	flag := false
	for _, v := range *j.preJobs {
		if v == *job {
			flag = true
			break
		}
	}

	return flag
}

func (j *Job) IncludeNextJob(job *Job) bool {
	j.init()

	flag := false
	for _, v := range *j.nextJobs {
		if v == *job {
			flag = true
			break
		}
	}

	return flag
}
