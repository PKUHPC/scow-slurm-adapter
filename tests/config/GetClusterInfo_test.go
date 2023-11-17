package main

import (
	"context"
	pb "scow-slurm-adapter/gen/go"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestGetClusterInfo(t *testing.T) {
	// Set up a connection to the server
	conn, err := grpc.Dial("localhost:8972", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewConfigServiceClient(conn)

	// Call the Add RPC with test data
	req := &pb.GetClusterInfoRequest{}
	res, err := client.GetClusterInfo(context.Background(), req)
	if err != nil {
		t.Fatalf("GetClusterConfig failed: %v", err)
	}

	// Check the result
	assert.IsType(t, []*pb.PartitionInfo{}, res.Partitions)
}
