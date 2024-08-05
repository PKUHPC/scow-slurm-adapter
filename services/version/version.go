package version

import (
	"context"
	pb "scow-slurm-adapter/gen/go"
)

type ServerVersion struct {
	pb.UnimplementedVersionServiceServer
}

func (s *ServerVersion) GetVersion(ctx context.Context, in *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	return &pb.GetVersionResponse{Major: 1, Minor: 6, Patch: 0}, nil
}
