package main

import (
	"github.com/pingdai/dagflow"
	"github.com/pingdai/dagflow/examples"
	"log"
)

func main() {
	var df dagflow.DagFlow
	job1 := examples.NewJob(1)
	job2 := examples.NewJob(2)
	job3 := examples.NewJob(3)
	job4 := examples.NewJob(4)
	job5 := examples.NewJob(5)
	job6 := examples.NewJob(6)
	df.Add(&job1)
	df.Add(&job2)
	df.Add(&job3)
	df.Add(&job4)
	df.Add(&job5)
	df.Add(&job6)
	df.Connect(&job1, &job2)
	df.Connect(&job1, &job3)
	df.Connect(&job1, &job4)
	df.Connect(&job2, &job5)
	df.Connect(&job3, &job5)
	df.Connect(&job5, &job6)
	df.Connect(&job4, &job6)

	err := df.Run()
	if err != nil {
		log.Fatalf("run failed,err:%v", err)
	}
	log.Printf("run ok")
}
