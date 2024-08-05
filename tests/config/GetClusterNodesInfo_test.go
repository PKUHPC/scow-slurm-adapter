package main

import (
	"context"
	"fmt"
	pb "scow-slurm-adapter/gen/go"
	"testing"

	"google.golang.org/grpc"
)

func TestGetClusterNodesInfo(t *testing.T) {
	// Set up a connection to the server
	var nodeList []string
	conn, err := grpc.Dial("localhost:8972", grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewConfigServiceClient(conn)

	nodeList = append(nodeList, "compute02")
	nodeList = append(nodeList, "compute01")

	// Call the Add RPC with test data
	req := &pb.GetClusterNodesInfoRequest{
		NodeNames: nodeList,
	}
	res, err := client.GetClusterNodesInfo(context.Background(), req)
	if err != nil {
		t.Fatalf("GetClusterConfig failed: %v", err)
	}
	fmt.Println(res)
	// Check the result
	// assert.IsType(t, []*pb.PartitionInfo{}, res.Partitions)
}
