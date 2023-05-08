package main

import (
	"context"
	"flag"
	"log"
	"time"

	// "fmt"

	"scow-slurm-adapter-client/pb"
	// "google.golang.org/grpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// hello_client

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "127.0.0.1:8972", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// 连接到server端，此处禁用安全传输
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	// c := pb.NewUserServiceClient(conn)
	// c := pb.NewAccountServiceClient(conn)
	// c := pb.NewConfigServiceClient(conn)
	c := pb.NewJobServiceClient(conn)

	// 执行RPC调用并打印收到的响应数据
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
	defer cancel()
	// r, err := c.QueryJobLimitTime(ctx, &pb.QueryJobLimitTimeRequest{JobId: 3})
	// r, err := c.ChangeJobLimitTime(ctx, &pb.ChangeJobLimitTimeRequest{JobId: 8, Minute: -30})
	// r, err := c.AddUserToAccount(ctx, &pb.AddUserToAccountRequest{UserId: "test07", AccountName: "a_admin985"})
	// r, err := c.GetAllAccountsWithUsers(ctx, &pb.GetAllAccountsWithUsersRequest{})
	// account_name 映射到gRPC上是 AccountName
	// r, err := c.UnblockAccount(ctx, &pb.UnblockAccountRequest{AccountName: []string{"yangjie", "yangjie", "yangjie"}}) // repeated的用法，就是一个切片,可以用0个或者多个
	// r, err := c.GetAllAccountsWithUsers(ctx, &pb.GetAllAccountsWithUsersRequest{Replay: "ddddd"})
	// r, err := c.ListAccounts(ctx, &pb.ListAccountsRequest{UserId: "root"})

	// resp, err := c.ListAccounts(ctx, &pb.ListAccountsRequest{UserId: "root"})
	// r, err := c.BlockUserInAccount(ctx, &pb.BlockUserInAccountRequest{UserId: "test02", AccountName: "p_admin"})
	// r, err := c.UnblockUserInAccount(ctx, &pb.UnblockUserInAccountRequest{UserId: "test02", AccountName: "p_admin"})
	// r, err := c.AddUserToAccount(ctx, &pb.AddUserToAccountRequest{UserId: "test05", AccountName: "g_admin"})
	// r, err := c.QueryUserInAccountBlockStatus(ctx, &pb.QueryUserInAccountBlockStatusRequest{UserId: "test02", AccountName: "p_admin"})
	// r, err := c.RemoveUserFromAccount(ctx, &pb.RemoveUserFromAccountRequest{UserId: "test05", AccountName: "dfffd"})
	// r, err := c.ListAccounts(ctx, &pb.ListAccountsRequest{UserId: "test05"})
	// r, err := c.BlockAccount(ctx, &pb.BlockAccountRequest{AccountName: "dfffd"})
	// r, err := c.UnblockAccount(ctx, &pb.UnblockAccountRequest{AccountName: "dfffddddd"})
	// r, err := c.CreateAccount(ctx, &pb.CreateAccountRequest{AccountName: "zlb", OwnerUserId: "test06"})
	// r, err := c.QueryAccountBlockStatus(ctx, &pb.QueryAccountBlockStatusRequest{AccountName: "dff222fdd"})
	// r, err := c.GetAllAccountsWithUsers(ctx, &pb.GetAllAccountsWithUsersRequest{})
	// r, err := c.GetClusterConfig(ctx, &pb.GetClusterConfigRequest{})
	// r, err := c.QueryJobTimeLimit(ctx, &pb.QueryJobTimeLimitRequest{JobId: "1261118"})
	// r, err := c.ChangeJobTimeLimit(ctx, &pb.ChangeJobTimeLimitRequest{JobId: "1269", DeltaMinutes: 10})
	// r, err := c.CancelJob(ctx, &pb.CancelJobRequest{UserId: "test0d5", JobId: 1268})
	r, err := c.GetJobById(ctx, &pb.GetJobByIdRequest{JobId: 1200})

	// user := "test03"
	// account := "c_admin"
	// state := "CANCELLED"
	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{Fields: []string{"account", "job_id"}, Filter: &pb.GetJobsRequest_Filter{User: &user, Account: &account, State: &state, EndTime: &pb.TimeRange{StartTime: &timestamppb.Timestamp{Seconds: 1682066342}, EndTime: &timestamppb.Timestamp{Seconds: 1682586485}}}, PageInfo: &pb.PageInfo{Page: 1, PageSize: 10}})

	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{})
	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{Filter: &pb.GetJobsRequest_Filter{State: &state, EndTime: &pb.TimeRange{StartTime: &timestamppb.Timestamp{Seconds: 1681970685}, EndTime: &timestamppb.Timestamp{Seconds: 1682316286}}}})
	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{Filter: &pb.GetJobsRequest_Filter{User: &user, Account: &account}, PageInfo: &pb.PageInfo{Page: 1, PageSize: 10}})

	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{Filter: &pb.GetJobsRequest_Filter{User: &user, Account: &account, State: &state, EndTime: &pb.TimeRange{StartTime: &timestamppb.Timestamp{Seconds: 1682066342}, EndTime: &timestamppb.Timestamp{Seconds: 1682152742}}}})
	// r, err := c.GetJobs(ctx, &pb.GetJobsRequest{Fields: []string{"ddddddd"}, PageInfo: &pb.PageInfo{Page: 2, PageSize: 20}})
	// qos := "normal"
	// timeLimitMinutes := uint32(1)
	// r, err := c.SubmitJob(ctx, &pb.SubmitJobRequest{Qos: &qos, TimeLimitMinutes: &timeLimitMinutes, UserId: "test02", JobName: "yyddyy", Account: "c_admin", NodeCount: 1, GpuCount: 0, MemoryMb: 200, CoreCount: 1, Script: "sleep 66", WorkingDirectory: "kdssssdkk", Partition: "compute"})

	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	// log.Printf("Greeting: %s", r.GetReplay())
	// log.Printf("Greeting: %t", r.GetBlocked())
	// log.Printf("Greeting: %v", r.GetAccounts())
	// log.Printf("Greeting: %v", r.GetJob())
	// log.Printf("Greeting: %v", r.GetBlocked())
	log.Printf("Greeting: %v", r)
}
