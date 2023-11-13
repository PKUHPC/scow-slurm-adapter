package main

import (
	"context"
	pb "scow-slurm-adapter/gen/go"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestSubmitScriptAsJob(t *testing.T) {

	// Set up a connection to the server
	conn, err := grpc.Dial("localhost:8972", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewJobServiceClient(conn)

	// Call the Add RPC with test data
	// qos := "normal"
	// timeLimitMinutes := uint32(1)
	// memoryMb := uint64(200)
	// stdout := "slurm-%j.out"
	// stderr := "slurm-%j.out"
	req := &pb.SubmitScriptAsJobRequest{
		UserId: "root",
		Script: "sssss",
	}
	res, err := client.SubmitScriptAsJob(context.Background(), req)
	if err != nil {
		t.Fatalf("SubmitFileAsJob failed: %v", err)
	}

	// Check the result, 通过判断错误为nil 来决定是否执行成功
	// assert.Empty(t, err)
	assert.IsType(t, uint32(1), res.JobId)
}
