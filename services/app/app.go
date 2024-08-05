package app

import (
	"context"
	pb "scow-slurm-adapter/gen/go"
)

type ServerAppServer struct {
	pb.UnimplementedAppServiceServer
}

// appservice
func (s *ServerAppServer) GetAppConnectionInfo(ctx context.Context, in *pb.GetAppConnectionInfoRequest) (*pb.GetAppConnectionInfoResponse, error) {
	return &pb.GetAppConnectionInfoResponse{}, nil
}
