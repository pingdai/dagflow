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

func (df *DagFlow) Root() (JobNode, error) {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	job, err := df.acyclicGraph.Root()
	return job.(JobNode), err
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

func (df *DagFlow) Replace(original, replacement JobNode) bool {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	return df.acyclicGraph.Replace(original, replacement)
}

func (df *DagFlow) DownEdgesList(v JobNode) []interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.DownEdges(v).List()
}

func (df *DagFlow) DownEdgesLen(v JobNode) int {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.DownEdges(v).Len()
}

func (df *DagFlow) UpEdgesList(v JobNode) []interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.UpEdges(v).List()
}

func (df *DagFlow) UpEdgesLen(v JobNode) int {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	return df.acyclicGraph.UpEdges(v).Len()
}

func (df *DagFlow) Add(v ...JobNode) {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	for _, n := range v {
		df.acyclicGraph.Add(n)
	}
}

func (df *DagFlow) Connect(from, to JobNode) {
	df.init()

	df.rwl.Lock()
	defer df.rwl.Unlock()

	df.acyclicGraph.Connect(dag.BasicEdge(from, to))
}

func (df *DagFlow) fetchWaitJobs() map[JobNode][]interface{} {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	waitJobs := make(map[JobNode][]interface{})
	for _, v := range df.acyclicGraph.Vertices() {
		job := v.(JobNode)
		if df.UpEdgesLen(job) > 1 {
			waitJobs[job] = df.UpEdgesList(job)
		}
	}

	return waitJobs
}

func (df *DagFlow) allJobs() []JobNode {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	jobs := make([]JobNode, 0, len(df.acyclicGraph.Vertices()))
	for _, v := range df.acyclicGraph.Vertices() {
		jobs = append(jobs, v.(JobNode))
	}
	return jobs
}

func (df *DagFlow) fetchJob(job JobNode) (JobNode, bool) {
	df.init()

	df.rwl.RLock()
	defer df.rwl.RUnlock()

	for _, v := range df.acyclicGraph.Vertices() {
		if v == job {
			return v.(JobNode), true
		}
	}

	return nil, false
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
	var rootsMap RootsMap
	rootsMap.Add(root)
	for {
		// 判断是否都已处理完成
		rn := rootsMap.UnfinishedLen()
		if rn == 0 {
			break
		} else {
			wg := &sync.WaitGroup{}
			wg.Add(rn)
			for _, root := range rootsMap.UnfinishedList() {
				go func(job JobNode) {
					defer wg.Done()

					if job.IsFinished() {
						return
					}

					// 判断是否需要前置任务完成才能执行
					if _, ok := waitJobs[job]; ok {
						for _, wait := range waitJobs[job] {
							// 实时去判断这些值的状态
							t, b := df.fetchJob(wait.(JobNode))
							if !b {
								log.Printf("cannot find %+v", wait.(JobNode))
								time.Sleep(time.Second)
								continue
							}
							if !t.IsFinished() {
								log.Printf("want to do %d ,but job: %d not finish,wait", job.GetTaskID(), wait.(JobNode).GetTaskID())
								time.Sleep(time.Second)
								continue
							}
						}
					}

					t := job
					t.Exec()
					t.Complete()
					t.SetFinished(true)
					df.Replace(job, t)
					rootsMap.Add(job)
				}(root)

				for _, v := range df.DownEdgesList(root) {
					rootsMap.Add(v.(JobNode))
				}
			}
			wg.Wait()
			continue
		}
	}

	return nil
}

type RootsMap struct {
	rootsMap    map[interface{}]JobNode
	rootsMapRWL *sync.RWMutex
}

func (rm *RootsMap) init() {
	if rm.rootsMapRWL == nil {
		rm.rootsMapRWL = new(sync.RWMutex)
	}
	if rm.rootsMap == nil {
		rm.rootsMap = make(map[interface{}]JobNode)
	}
}

func (rm *RootsMap) Add(jobNode JobNode) {
	rm.init()
	rm.rootsMapRWL.Lock()
	defer rm.rootsMapRWL.Unlock()

	rm.rootsMap[dag.Hashable(jobNode)] = jobNode
}

func (rm *RootsMap) List() []JobNode {
	rm.init()
	rm.rootsMapRWL.RLock()
	defer rm.rootsMapRWL.RUnlock()

	list := make([]JobNode, 0, len(rm.rootsMap))
	for _, v := range rm.rootsMap {
		list = append(list, v)
	}

	return list
}

func (rm *RootsMap) Len() int {
	rm.init()
	rm.rootsMapRWL.RLock()
	defer rm.rootsMapRWL.RUnlock()

	return len(rm.rootsMap)
}

func (rm *RootsMap) UnfinishedList() []JobNode {
	rm.init()
	rm.rootsMapRWL.RLock()
	defer rm.rootsMapRWL.RUnlock()

	list := make([]JobNode, 0)
	for _, v := range rm.rootsMap {
		if !v.IsFinished() {
			list = append(list, v)
		}
	}

	return list
}

func (rm *RootsMap) UnfinishedLen() int {
	rm.init()
	rm.rootsMapRWL.RLock()
	defer rm.rootsMapRWL.RUnlock()

	len := 0
	for _, v := range rm.rootsMap {
		if !v.IsFinished() {
			len++
		}
	}

	return len
}
