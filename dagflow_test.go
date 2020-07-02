package dagflow

import (
	"fmt"
	"testing"
)

func TestAdd(t *testing.T) {
	var df DagFlow
	var job1, job2, job3 Job

	job1.AddNode(&NodeTest{Name: "node1"})
	job2.AddNode(&NodeTest{Name: "node2"})
	job3.AddNode(&NodeTest{Name: "node3"})
	df.Add(job1)
	df.Add(job2)
	df.Add(job3)

	for _, v := range df.jobs {
		job := v.(Job)
		job.GetNode().Exec()
		job.GetNode().Complete()
	}
}

func TestConnect(t *testing.T) {
	var df DagFlow
	var job1, job2, job3, job4, job5, job6 Job

	job1.AddNode(&NodeTest{Name: "node1"})
	job2.AddNode(&NodeTest{Name: "node2"})
	job3.AddNode(&NodeTest{Name: "node3"})
	job4.AddNode(&NodeTest{Name: "node4"})
	job5.AddNode(&NodeTest{Name: "node5"})
	job6.AddNode(&NodeTest{Name: "node6"})
	df.Add(job1)
	df.Add(job2)
	df.Add(job3)
	df.Add(job4)
	df.Add(job5)
	df.Add(job6)
	df.Connect(job1, job2)
	df.Connect(job1, job3)
	df.Connect(job2, job4)
	df.Connect(job3, job5)
	df.Connect(job4, job6)
	df.Connect(job5, job6)

	for _, v := range df.jobs {
		job := v.(Job)
		t.Logf("node")
		t.Logf("preJob:%+v", *job.GetPreJobs())
		t.Logf("nextJob:%+v", *job.GetNextJobs())
		t.Logf("===========")
	}
}

func TestRun(t *testing.T) {
	var df DagFlow
	var job1, job2, job3, job4 Job

	job1.AddNode(&NodeTest{Name: "node1"})
	job2.AddNode(&NodeTest{Name: "node2"})
	job3.AddNode(&NodeTest{Name: "node3"})
	job4.AddNode(&NodeTest{Name: "node4"})
	df.Add(job1)
	df.Add(job2)
	df.Add(job3)
	df.Add(job4)
	df.Connect(job1, job2)
	df.Connect(job1, job3)
	df.Connect(job2, job4)
	df.Connect(job3, job4)
	df.Run()
}

type NodeTest struct {
	Name string
}

func (n *NodeTest) Exec() {
	fmt.Printf("name[%s] here is exec module\n", n.Name)
}

func (n *NodeTest) Complete() {
	fmt.Printf("name[%s] here is complete module\n", n.Name)
}
