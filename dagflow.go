package dagflow

type DagFlow struct {
	jobs Set
}

func (df *DagFlow) init() {
	if df.jobs == nil {
		df.jobs = make(Set)
	}
}

func (df *DagFlow) Add(job Job) {
	df.init()
	df.jobs.Add(job)
}

func (df *DagFlow) Remove(job Job) {
	df.init()
	df.jobs.Delete(job)
	// 是否还需要对删除后的数据内容进行重新排序
	// todo
}

func (df *DagFlow) Connect(srcJob, dstJob Job) {
	df.init()

	sourceCode := hashcode(srcJob)
	targetCode := hashcode(dstJob)

	if !df.jobs.Include(sourceCode) ||
		!df.jobs.Include(targetCode) {
		return
	}

	job1 := df.jobs[sourceCode].(Job)
	if !job1.IncludeNextJob(&dstJob) {
		job1.AddNextJob(&dstJob)
		df.jobs[sourceCode] = job1
	}

	job2 := df.jobs[targetCode].(Job)
	if !job2.IncludePreJob(&srcJob) {
		job2.AddPreJob(&srcJob)
		df.jobs[targetCode] = job2
	}

	return
}

func (df *DagFlow) Run() {
	// todo
}
