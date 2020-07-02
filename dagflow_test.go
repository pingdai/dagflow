package dagflow

import (
	"fmt"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	var df DagFlow
	var job1, job2, job3, job4 /*, job5, job6, job7*/ Job
	job1 = NewJob(&NodeTest{Name: "node1"})
	job2 = NewJob(&NodeTest{Name: "node2"})
	job3 = NewJob(&NodeTest{Name: "node3"})
	job4 = NewJob(&NodeTest{Name: "node4"})
	df.Add(job1)
	df.Add(job2)
	df.Add(job3)
	df.Add(job4)
	// df.Add(job5)
	// df.Add(job6)
	// df.Add(job7)
	df.Connect(job1, job2)
	df.Connect(job1, job3)
	df.Connect(job2, job4)
	df.Connect(job3, job4)
	// df.Connect(job3, job6)
	// df.Connect(job3, job7)
	err := df.Run()
	if err != nil {
		t.Errorf("run fail:%v", err)
	} else {
		t.Logf("run ok")
	}
}

type NodeTest struct {
	Name string
}

func (n *NodeTest) Exec() {
	time.Sleep(2 * time.Second)
	fmt.Printf("name[%s] here is exec module\n", n.Name)
}

func (n *NodeTest) Complete() {
	time.Sleep(2 * time.Second)
	fmt.Printf("name[%s] here is complete module\n", n.Name)
}
