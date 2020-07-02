package dagflow

import (
	"github.com/hashicorp/terraform/dag"
	"log"
	"sync"
	"time"
)

type DagFlow struct {
	rwl          *sync.RWMutex
	acyclicGraph dag.AcyclicGraph
}

func (df *DagFlow) init() {
	if df.rwl == nil {
		df.rwl = new(sync.RWMutex)
	}
}

func (df *DagFlow) Root() (Job, error) {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	job, err := df.acyclicGraph.Root()
	return job.(Job), err
}

func (df *DagFlow) Validate() error {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.Validate()
}

func (df *DagFlow) TransitiveReduction() {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	df.acyclicGraph.TransitiveReduction()
}

func (df *DagFlow) Replace(original, replacement Job) bool {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	return df.acyclicGraph.Replace(original, replacement)
}

func (df *DagFlow) DownEdgesList(v Job) []interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.DownEdges(v).List()
}

func (df *DagFlow) DownEdgesLen(v Job) int {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.DownEdges(v).Len()
}

func (df *DagFlow) UpEdgesList(v Job) []interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.UpEdges(v).List()
}

func (df *DagFlow) UpEdgesLen(v Job) int {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.UpEdges(v).Len()
}

func (df *DagFlow) Add(v Job) {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	df.acyclicGraph.Add(v)
}

func (df *DagFlow) Connect(source, target Job) {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	df.acyclicGraph.Connect(dag.BasicEdge(source, target))
}

func (df *DagFlow) fetchWaitJobs() map[Job][]interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	waitJobs := make(map[Job][]interface{})
	for _, v := range df.acyclicGraph.Vertices() {
		job := v.(Job)
		if df.UpEdgesLen(job) > 1 {
			waitJobs[job] = df.UpEdgesList(job)
		}
	}

	return waitJobs
}

func (df *DagFlow) allJobs() []Job {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	jobs := make([]Job, 0, len(df.acyclicGraph.Vertices()))
	for _, v := range df.acyclicGraph.Vertices() {
		jobs = append(jobs, v.(Job))
	}
	return jobs
}

func (df *DagFlow) fetchJob(job Job) (Job, bool) {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	for _, v := range df.acyclicGraph.Vertices() {
		if v == job {
			return v.(Job), true
		}
	}

	return Job{}, false
}

func (df *DagFlow) Run() error {
	df.init()

	// 基本校验
	if err := df.Validate(); err != nil {
		return err
	}

	df.TransitiveReduction()

	waitJobs := df.fetchWaitJobs()

	root, _ := df.Root()
	roots := []dag.Vertex{root}
	for {
		rn := len(roots)
		if rn == 0 {
			break
		} else {
			wg := &sync.WaitGroup{}
			wg.Add(rn)
			newRoots := make([]dag.Vertex, 0)
			for _, root := range roots {
				go func(job Job) {
					defer wg.Done()

					if job.IsFinished() {
						return
					}

					// 判断是否需要前置任务完成才能执行
					if _, ok := waitJobs[job]; ok {
						for _, wait := range waitJobs[job] {
							// 实时去判断这些值的状态
							t, b := df.fetchJob(wait.(Job))
							if !b {
								log.Printf("cannot find %+v", wait.(Job))
								time.Sleep(time.Second)
								continue
							}
							if !t.IsFinished() {
								log.Printf("job : %+v not finish,wait", wait.(Job))
								time.Sleep(time.Second)
								continue
							}
						}
					}

					t := job
					t.GetNode().Exec()
					t.GetNode().Complete()
					t.SetFinished(true)
					df.Replace(job, t)
				}(root.(Job))

				for _, v := range df.DownEdgesList(root.(Job)) {
					newRoots = append(newRoots, dag.Vertex(v))
				}
			}
			wg.Wait()
			roots = newRoots
			continue
		}
	}

	return nil
}
