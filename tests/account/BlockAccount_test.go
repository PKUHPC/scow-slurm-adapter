package main

import (
	"context"
	pb "scow-slurm-adapter/pb"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestBlockAccount(t *testing.T) {

	// Set up a connection to the server
	conn, err := grpc.Dial("localhost:8972", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewAccountServiceClient(conn)

	// Call the Add RPC with test data
	req := &pb.BlockAccountRequest{
		AccountName: "dddddsss",
	}
	_, err = client.BlockAccount(context.Background(), req)
	if err != nil {
		t.Fatalf("BlockAccount failed: %v", err)
	}

	// Check the result, 通过判断错误为nil 来决定是否执行成功
	assert.Empty(t, err)
	// log.Println(res)
}