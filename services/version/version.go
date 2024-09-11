package version

import (
	"context"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
)

type ServerVersion struct {
	pb.UnimplementedVersionServiceServer
}

func (s *ServerVersion) GetVersion(ctx context.Context, in *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	caller.Logger.Tracef("Adapter Version is: %v", &pb.GetVersionResponse{Major: 1, Minor: 6, Patch: 0})
	return &pb.GetVersionResponse{Major: 1, Minor: 6, Patch: 0}, nil
}
