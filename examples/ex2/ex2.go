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
	df.Add(&job1)
	df.Add(&job2)
	df.Add(&job3)
	df.Connect(&job1, &job3)
	df.Connect(&job2, &job3)
	df.Connect(&job1, &job1)

	err := df.Validate()
	if err != nil {
		log.Fatalf("validate failed,err:%v", err)
	}
	log.Printf("validate ok")
}
