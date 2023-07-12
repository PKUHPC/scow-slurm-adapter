package main

import (
	"context"
	"database/sql"
	"io"
	"log"
	"os"
	"path/filepath"

	"fmt"
	"math"
	"net"

	"scow-slurm-adapter/utils"
	"strconv"
	"strings"
	"time"

	pb "scow-slurm-adapter/gen/go"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/wxnacy/wgo/arrays"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	configValue *utils.Config
	db          *sql.DB
	logger      *logrus.Logger
)

type serverUser struct {
	pb.UnimplementedUserServiceServer
}

type serverAccount struct {
	pb.UnimplementedAccountServiceServer
}

type serverConfig struct {
	pb.UnimplementedConfigServiceServer
}

type serverJob struct {
	pb.UnimplementedJobServiceServer
}

func init() {
	configValue = utils.ParseConfig(utils.DefaultConfigPath)
}

// UserService
func (s *serverUser) AddUserToAccount(ctx context.Context, in *pb.AddUserToAccountRequest) (*pb.AddUserToAccountResponse, error) {
	var (
		acctName string
		userName string
		qosName  string
		user     string
		qosList  []string
	)

	// 记录日志
	logger.Infof("Received request AddUserToAccount: %v", in)

	// 获取机器名和默认Qos
	clusterName := configValue.MySQLConfig.ClusterName
	defaultQos := configValue.Slurm.DefaultQOS

	// 检查用户名、账户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账号是否存在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询系统中的base Qos
	qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
	rows, err := db.Query(qosSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&qosName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		qosList = append(qosList, qosName)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	baseQos := strings.Join(qosList, ",")
	// 查询用户是否在系统中
	partitions, _ := utils.GetPatitionInfo()

	// 检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)

	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, defaultQos)
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.AddUserToAccountResponse{}, nil
	}
	// 检查账户和用户之间是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)

	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, defaultQos)
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.AddUserToAccountResponse{}, nil
	}
	// 关联已经存在的情况
	errInfo := &errdetails.ErrorInfo{
		Reason: "USER_ALREADY_EXISTS",
	}
	st := status.New(codes.AlreadyExists, "The user already exists in account.")
	st, _ = st.WithDetails(errInfo)
	return nil, st.Err()
}

func (s *serverUser) RemoveUserFromAccount(ctx context.Context, in *pb.RemoveUserFromAccountRequest) (*pb.RemoveUserFromAccountResponse, error) {
	var (
		acctName string
		userName string
		user     string
		acct     string
		jobName  string
		jobList  []string
		acctList []string
	)
	// 记录日志
	logger.Infof("Received request RemoveUserFromAccount: %v", in)
	// 获取集群名
	clusterName := configValue.MySQLConfig.ClusterName

	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账号名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户名是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户和用户之间是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "User and account assocation is not exists!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询除当前账户外的关联账户信息
	assocAcctSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE user = ? AND deleted = 0 AND acct != ?", clusterName)
	rows, err := db.Query(assocAcctSqlConfig, in.UserId, in.AccountName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&acct)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		acctList = append(acctList, acct)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询用户uid
	uid, _, err := utils.GetUserUidGid(in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户是否有未结束的作业
	jobSqlConfig := fmt.Sprintf("SELECT job_name FROM %s_job_table WHERE id_user = ? AND account = ? AND state IN (0, 1, 2)", clusterName)
	jobRows, err := db.Query(jobSqlConfig, uid, in.AccountName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer jobRows.Close()
	for jobRows.Next() {
		err := jobRows.Scan(&jobName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		jobList = append(jobList, jobName)
	}
	err = jobRows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	if len(acctList) == 0 {
		// 有作业直接出错返回
		if len(jobList) != 0 {
			errInfo := &errdetails.ErrorInfo{
				Reason: "RUNNING_JOB_EXISTS",
			}
			st := status.New(codes.Internal, "This user have running jobs!")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}

		// 没作业下直接删除用户
		// deletedUserCmd := fmt.Sprintf("sacctmgr -i delete user name=%s account=%s", in.UserId, in.AccountName)
		deletedUserCmd := fmt.Sprintf("sacctmgr -i delete user name=%s", in.UserId)
		res := utils.ExecuteShellCommand(deletedUserCmd)
		if res == 0 {
			return &pb.RemoveUserFromAccountResponse{}, nil
		}
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXECUTE_FAILED",
		}
		st := status.New(codes.Internal, "Shell command execute falied!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 更改默认账号
	if len(jobList) != 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "RUNNING_JOB_EXISTS",
		}
		st := status.New(codes.Internal, "This user have running jobs!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	updateDefaultAcctCmd := fmt.Sprintf("sacctmgr -i update user set DefaultAccount=%s where user=%s", acctList[0], in.UserId)
	utils.ExecuteShellCommand(updateDefaultAcctCmd)
	deleteUerFromAcctCmd := fmt.Sprintf("sacctmgr -i delete user name=%s account=%s", in.UserId, in.AccountName)
	utils.ExecuteShellCommand(deleteUerFromAcctCmd)
	return &pb.RemoveUserFromAccountResponse{}, nil
}

func (s *serverUser) BlockUserInAccount(ctx context.Context, in *pb.BlockUserInAccountRequest) (*pb.BlockUserInAccountResponse, error) {
	var (
		acctName string
		userName string
		user     string
	)
	// 记录日志
	logger.Infof("Received request BlockUserInAccount: %v", in)

	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := configValue.MySQLConfig.ClusterName
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户与用户是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)

	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "User and account assocation is not exists!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 关联存在的情况下直接封锁账户
	blockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name=%s account=%s set MaxSubmitJobs=0  MaxJobs=0 MaxWall=00:00:00  GrpJobs=0 GrpSubmit=0 GrpSubmitJobs=0 MaxSubmitJobs=0 GrpWall=00:00:00", in.UserId, in.AccountName)
	res := utils.ExecuteShellCommand(blockUserCmd)
	if res == 0 {
		return &pb.BlockUserInAccountResponse{}, nil
	}
	errInfo := &errdetails.ErrorInfo{
		Reason: "COMMAND_EXECUTE_FAILED",
	}
	st := status.New(codes.Internal, "Shell command execute falied!")
	st, _ = st.WithDetails(errInfo)
	return nil, st.Err()

}

func (s *serverUser) UnblockUserInAccount(ctx context.Context, in *pb.UnblockUserInAccountRequest) (*pb.UnblockUserInAccountResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)
	// 记录日志
	logger.Infof("Received request UnblockUserInAccount: %v", in)
	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := configValue.MySQLConfig.ClusterName
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	//  检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户与用户是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "User and account assocation is not exists!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 最大提交作业数为NULL表示没被封锁
	maxSubmitJobsSqlConfig := fmt.Sprintf("SELECT DISTINCT max_submit_jobs FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(maxSubmitJobsSqlConfig, in.UserId, in.AccountName).Scan(&maxSubmitJobs)
	if err != nil {
		return &pb.UnblockUserInAccountResponse{}, nil
	}
	// 用户从账户中解封的操作
	unblockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name='%s' account='%s' set MaxSubmitJobs=-1 MaxJobs=-1 MaxWall=-1  GrpJobs=-1 GrpSubmit=-1 GrpSubmitJobs=-1 MaxSubmitJobs=-1 GrpWall=-1", in.UserId, in.AccountName)
	res := utils.ExecuteShellCommand(unblockUserCmd)
	if res == 0 {
		return &pb.UnblockUserInAccountResponse{}, nil
	}
	errInfo := &errdetails.ErrorInfo{
		Reason: "COMMAND_EXECUTE_FAILED",
	}
	st := status.New(codes.Internal, "Shell command execute falied!")
	st, _ = st.WithDetails(errInfo)
	return nil, st.Err()
}

func (s *serverUser) QueryUserInAccountBlockStatus(ctx context.Context, in *pb.QueryUserInAccountBlockStatusRequest) (*pb.QueryUserInAccountBlockStatusResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)
	// 记录日志
	logger.Infof("Received request QueryUserInAccountBlockStatus: %v", in)
	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := configValue.MySQLConfig.ClusterName
	// 判断账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 判断用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户与用户在slurm中是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "User and account assocation is not exists!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 查询max_submit_jobs的值,通过max_submit_jobs来判断用户是否被封锁
	maxSubmitJobSqlConfig := fmt.Sprintf("SELECT DISTINCT max_submit_jobs FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(maxSubmitJobSqlConfig, in.UserId, in.AccountName).Scan(&maxSubmitJobs)
	if err != nil {
		return &pb.QueryUserInAccountBlockStatusResponse{Blocked: false}, nil
	}
	return &pb.QueryUserInAccountBlockStatusResponse{Blocked: true}, nil
}

// Account service
func (s *serverAccount) ListAccounts(ctx context.Context, in *pb.ListAccountsRequest) (*pb.ListAccountsResponse, error) {
	var (
		userName  string
		assocAcct string
		acctList  []string
	)
	// 记录日志
	logger.Infof("Received request ListAccounts: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取集群名
	clusterName := configValue.MySQLConfig.ClusterName

	// 判断用户在slurm中是否存在
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 查询用户相关联的所有账户信息
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE user = ? AND deleted = 0", clusterName)
	rows, err := db.Query(assocSqlConfig, in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&assocAcct)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		acctList = append(acctList, assocAcct)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	return &pb.ListAccountsResponse{Accounts: acctList}, nil
}

func (s *serverAccount) CreateAccount(ctx context.Context, in *pb.CreateAccountRequest) (*pb.CreateAccountResponse, error) {
	var (
		acctName string
		qosName  string
		qosList  []string
	)
	// 记录日志
	logger.Infof("Received request CreateAccount: %v", in)
	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	resultUser := utils.ContainsUppercase(in.OwnerUserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取系统中默认的Qos信息
	defaultQos := configValue.Slurm.DefaultQOS
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		partitions, _ := utils.GetPatitionInfo() // 获取系统中计算分区信息
		// 获取系统中Qos
		qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
		rows, err := db.Query(qosSqlConfig)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&qosName)
			if err != nil {
				errInfo := &errdetails.ErrorInfo{
					Reason: "SQL_QUERY_FAILED",
				}
				st := status.New(codes.Internal, err.Error())
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			qosList = append(qosList, qosName)
		}

		err = rows.Err()
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		baseQos := strings.Join(qosList, ",")
		createAccountCmd := fmt.Sprintf("sacctmgr -i create account name=%s", in.AccountName)
		utils.ExecuteShellCommand(createAccountCmd)
		for _, p := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name=%s partition=%s account=%s", in.OwnerUserId, p, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos=%s DefaultQOS=%s", in.OwnerUserId, baseQos, defaultQos)
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.CreateAccountResponse{}, nil
	}
	errInfo := &errdetails.ErrorInfo{
		Reason: "ACCOUNT_ALREADY_EXISTS",
	}
	st := status.New(codes.AlreadyExists, "The account is already exists.")
	st, _ = st.WithDetails(errInfo)
	return nil, st.Err()
}

func (s *serverAccount) BlockAccount(ctx context.Context, in *pb.BlockAccountRequest) (*pb.BlockAccountResponse, error) {
	var (
		acctName      string
		assocAcctName string
		acctList      []string
	)
	// 记录日志
	logger.Infof("Received request BlockAccount: %v", in)
	// 检查账户名中是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	if resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := configValue.MySQLConfig.ClusterName
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, _ := utils.GetPatitionInfo()
	// 获取计算分区AllowAccounts的值
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)
	if output == "ALL" {
		acctSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE deleted = 0 AND acct != ?", clusterName)
		rows, err := db.Query(acctSqlConfig, in.AccountName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&assocAcctName)
			if err != nil {
				errInfo := &errdetails.ErrorInfo{
					Reason: "SQL_QUERY_FAILED",
				}
				st := status.New(codes.Internal, err.Error())
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			acctList = append(acctList, assocAcctName)
		}
		err = rows.Err()
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		allowAcct := strings.Join(acctList, ",")
		for _, v := range partitions {
			updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition='%s' AllowAccounts='%s'", v, allowAcct)
			utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
		}
		return &pb.BlockAccountResponse{}, nil
	}
	// output的值包含了系统中所有的分区信息
	AllowAcctList := strings.Split(output, ",")
	// 判断账户名是否在AllowAcctList中
	index := arrays.ContainsString(AllowAcctList, in.AccountName)
	if index == -1 {
		return &pb.BlockAccountResponse{}, nil
	}
	// 账户存在AllowAcctList中，则删除账户后更新计算分区AllowAccounts
	updateAllowAcct := utils.DeleteSlice(AllowAcctList, in.AccountName)
	for _, p := range partitions {
		updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition=%s AllowAccounts=%s", p, strings.Join(updateAllowAcct, ","))
		utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
	}
	return &pb.BlockAccountResponse{}, nil
}

func (s *serverAccount) UnblockAccount(ctx context.Context, in *pb.UnblockAccountRequest) (*pb.UnblockAccountResponse, error) {
	var (
		acctName string
	)
	// 记录日志
	logger.Infof("Received request UnblockAccount: %v", in)
	// 检查用户名中是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	if resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, _ := utils.GetPatitionInfo()
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)
	if output == "ALL" {
		return &pb.UnblockAccountResponse{}, nil
	}
	AllowAcctList := strings.Split(output, ",")
	index := arrays.ContainsString(AllowAcctList, in.AccountName)
	if index == -1 {
		// 不在里面的话需要解封
		AllowAcctList = append(AllowAcctList, in.AccountName)
		for _, p := range partitions {
			updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition=%s AllowAccounts=%s", p, strings.Join(AllowAcctList, ","))
			utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
		}
		return &pb.UnblockAccountResponse{}, nil
	}
	return &pb.UnblockAccountResponse{}, nil
}

func (s *serverAccount) GetAllAccountsWithUsers(ctx context.Context, in *pb.GetAllAccountsWithUsersRequest) (*pb.GetAllAccountsWithUsersResponse, error) {
	var (
		acctName      string
		userName      string
		maxSubmitJobs int
		acctList      []string
		acctInfo      []*pb.ClusterAccountInfo
	)
	// 记录日志
	logger.Infof("Received request GetAllAccountsWithUsers: %v", in)
	// 获取集群名
	clusterName := configValue.MySQLConfig.ClusterName

	// 获取系统中所有账户信息
	acctSqlConfig := fmt.Sprintf("SELECT name FROM acct_table WHERE deleted = 0")
	rows, err := db.Query(acctSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&acctName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		acctList = append(acctList, acctName)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询allowAcct的值(ALL和具体的acct列表)
	partitions, _ := utils.GetPatitionInfo()
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)

	// 获取和每个账户关联的用户的信息
	for _, v := range acctList {
		var userInfo []*pb.ClusterAccountInfo_UserInAccount
		assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user, max_submit_jobs FROM %s_assoc_table WHERE deleted = 0 AND acct = ? AND user != ''", clusterName)
		rows, err := db.Query(assocSqlConfig, v)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&userName, &maxSubmitJobs)
			if err != nil {
				userInfo = append(userInfo, &pb.ClusterAccountInfo_UserInAccount{
					UserId:   userName,
					UserName: userName,
					Blocked:  false,
				})
			} else {
				if maxSubmitJobs == 0 {
					userInfo = append(userInfo, &pb.ClusterAccountInfo_UserInAccount{
						UserId:   userName,
						UserName: userName,
						Blocked:  true,
					})
				}
			}
		}
		err = rows.Err()
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		if output == "ALL" {
			acctInfo = append(acctInfo, &pb.ClusterAccountInfo{
				AccountName: v,
				Users:       userInfo,
				Blocked:     false,
			})
		} else {
			AllowAcctList := strings.Split(output, ",")
			index := arrays.ContainsString(AllowAcctList, v)
			if index == -1 {
				acctInfo = append(acctInfo, &pb.ClusterAccountInfo{
					AccountName: v,
					Users:       userInfo,
					Blocked:     true,
				})
			} else {
				acctInfo = append(acctInfo, &pb.ClusterAccountInfo{
					AccountName: v,
					Users:       userInfo,
					Blocked:     false,
				})
			}
		}
	}
	return &pb.GetAllAccountsWithUsersResponse{Accounts: acctInfo}, nil
}

func (s *serverAccount) QueryAccountBlockStatus(ctx context.Context, in *pb.QueryAccountBlockStatusRequest) (*pb.QueryAccountBlockStatusResponse, error) {
	var (
		acctName string
	)
	// 记录日志
	logger.Infof("Received request QueryAccountBlockStatus: %v", in)
	// 检查用户名中是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.AccountName)
	if resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, _ := utils.GetPatitionInfo()
	// 获取系统中分区AllowAccounts信息
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)
	if output == "ALL" {
		return &pb.QueryAccountBlockStatusResponse{Blocked: false}, nil
	}
	acctList := strings.Split(output, ",")
	index := arrays.ContainsString(acctList, in.AccountName)
	if index == -1 {
		return &pb.QueryAccountBlockStatusResponse{Blocked: true}, nil
	}
	return &pb.QueryAccountBlockStatusResponse{Blocked: false}, nil
}

// config service
func (s *serverConfig) GetClusterConfig(ctx context.Context, in *pb.GetClusterConfigRequest) (*pb.GetClusterConfigResponse, error) {
	var (
		parts           []*pb.Partition // 定义返回的类型
		qosName         string
		qosList         []string
		totalCpuInt     int
		totalMemInt     int
		totalNodeNumInt int
	)
	// 记录日志
	logger.Infof("Received request GetClusterConfig: %v", in)
	// 获取系统计算分区信息
	partitions, _ := utils.GetPatitionInfo()
	// 查系统中的所有qos
	qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
	rows, err := db.Query(qosSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&qosName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		qosList = append(qosList, qosName)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	for _, partition := range partitions {
		var (
			totalGpus uint32
			comment   string
			qos       []string
			totalMems int
		)

		getPartitionInfoCmd := fmt.Sprintf("scontrol show partition=%s | grep -i mem=", partition)
		output, err := utils.RunCommand(getPartitionInfoCmd)
		// 不同slurm版本的问题
		if err == nil {
			configArray := strings.Split(output, ",")
			totalCpusCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $3}'", configArray[0])
			// totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}' | awk -F'M' '{print $1}'", configArray[1])
			totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}'", configArray[1])
			totalNodesCmd := fmt.Sprintf("echo %s | awk  -F'=' '{print $2}'", configArray[2])

			totalCpus, _ := utils.RunCommand(totalCpusCmd)
			totalMemsTmp, _ := utils.RunCommand(totalMemsCmd)
			// totalMems1, _ := utils.RunCommand(totalMemsCmd1)
			if strings.Contains(totalMemsTmp, "M") {
				totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "M")[0])
				totalMems = totalMemsInt
			} else if strings.Contains(totalMemsTmp, "G") {
				totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "G")[0])
				totalMems = totalMemsInt * 1024
			} else if strings.Contains(totalMemsTmp, "T") {
				totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "T")[0])
				totalMems = totalMemsInt * 1024 * 1024
			}
			totalNodes, _ := utils.RunCommand(totalNodesCmd)

			// 将字符串转换为int
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			// totalMemInt, _ = strconv.Atoi(totalMems)
			totalMemInt = totalMems
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)
		} else {
			// 获取总cpu、总内存、总节点数
			getPartitionTotalCpusCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalCPUs | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
			totalCpus, _ := utils.RunCommand(getPartitionTotalCpusCmd)
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			getPartitionTotalNodesCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalNodes | awk '{print $3}' | awk -F'=' '{print $2}'", partition)
			totalNodes, _ := utils.RunCommand(getPartitionTotalNodesCmd)
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)

			// 取节点名，默认取第一个元素，在判断有没有[特殊符合
			getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
			nodeOutput, _ := utils.RunCommand(getPartitionNodeNameCmd)
			nodeArray := strings.Split(nodeOutput, ",")
			res := strings.Contains(nodeArray[0], "[")
			if res {
				getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
				nodeNameOutput, _ := utils.RunCommand(getNodeNameCmd)
				nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
				// getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  mem= | awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeName)
				getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  RealMemory=| awk '{print $1}' | awk -F'=' '{print $2}'", nodeName)
				memOutput, err := utils.RunCommand(getMemCmd)

				if err != nil {

				}
				nodeMem, _ := strconv.Atoi(memOutput)
				totalMemInt = nodeMem * totalNodeNumInt
			} else {
				// getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  mem=| awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeArray[0])
				getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  RealMemory=| awk '{print $1}' | awk -F'=' '{print $2}'", nodeArray[0])
				memOutput, err := utils.RunCommand(getMemCmd)

				if err != nil {

				}
				nodeMem, _ := strconv.Atoi(memOutput)
				totalMemInt = nodeMem * totalNodeNumInt
			}
		}

		// 取节点名，默认取第一个元素，在判断有没有[特殊符合
		getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
		nodeOutput, _ := utils.RunCommand(getPartitionNodeNameCmd)
		nodeArray := strings.Split(nodeOutput, ",")

		res := strings.Contains(nodeArray[0], "[")
		if res {
			getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
			nodeNameOutput, _ := utils.RunCommand(getNodeNameCmd)
			nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
			gpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeName)
			gpusOutput, _ := utils.RunCommand(gpusCmd)
			if gpusOutput == "Gres=(null)" {
				totalGpus = 0
			} else {
				// 字符串转整型
				perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
				totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
			}
		} else {
			getGpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeArray[0])
			gpusOutput, _ := utils.RunCommand(getGpusCmd)
			if gpusOutput == "Gres=(null)" {
				totalGpus = 0
			} else {
				perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
				totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
			}
		}
		getPartitionQosCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' QoS=' | awk '{print $3}'", partition)
		qosOutput, _ := utils.RunCommand(getPartitionQosCmd)
		qosArray := strings.Split(qosOutput, "=")

		// 获取AllowQos
		getPartitionAllowQosCmd := fmt.Sprintf("scontrol show partition=%s | grep AllowQos | awk '{print $3}'| awk -F'=' '{print $2}'", partition)
		// 返回的是字符串
		allowQosOutput, _ := utils.RunCommand(getPartitionAllowQosCmd)

		if qosArray[len(qosArray)-1] != "N/A" {
			qos = append(qos, qosArray[len(qosArray)-1])
		} else {
			if allowQosOutput == "ALL" {
				qos = qosList
			} else {
				qos = strings.Split(allowQosOutput, ",")
			}
		}
		parts = append(parts, &pb.Partition{
			Name:    partition,
			MemMb:   uint64(totalMemInt),
			Cores:   uint32(totalCpuInt),
			Gpus:    totalGpus,
			Nodes:   uint32(totalNodeNumInt),
			Qos:     qos,
			Comment: &comment,
		})
	}
	// 增加调度器的名字, 针对于特定的适配器做的接口
	return &pb.GetClusterConfigResponse{Partitions: parts, SchedulerName: "slurm"}, nil
}

func (s *serverConfig) GetAvailablePartitions(ctx context.Context, in *pb.GetAvailablePartitionsRequest) (*pb.GetAvailablePartitionsResponse, error) {
	var (
		parts           []*pb.Partition // 定义返回的类型
		userName        string
		user            string
		acctName        string
		qosName         string
		qosList         []string
		totalCpuInt     int
		totalMemInt     int
		totalNodeNumInt int
	)
	// 记录日志
	logger.Infof("Received request GetAvailablePartitions: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取集群名
	clusterName := configValue.MySQLConfig.ClusterName

	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "Account does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 判断用户是否存在
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户和用户之间是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = db.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "User and account assocation is not exists!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查系统中的所有qos
	qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
	rows, err := db.Query(qosSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&qosName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		qosList = append(qosList, qosName)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 关联关系存在的情况下去找用户
	partitions, _ := utils.GetPatitionInfo()
	for _, partition := range partitions {
		var (
			totalMems int
			totalGpus uint32
			comment   string
			qos       []string
		)
		getPartitionAllowAccountsCmd := fmt.Sprintf("scontrol show part=%s | grep -i AllowAccounts | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
		accouts, _ := utils.RunCommand(getPartitionAllowAccountsCmd)
		if accouts == "ALL" || strings.Contains(accouts, in.AccountName) {
			// 包含account
			getPartitionInfoCmd := fmt.Sprintf("scontrol show partition=%s | grep -i mem=", partition)
			output, err := utils.RunCommand(getPartitionInfoCmd)
			if err == nil {
				configArray := strings.Split(output, ",")
				totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}'", configArray[1])
				totalMemsTmp, _ := utils.RunCommand(totalMemsCmd)
				// totalMems1, _ := utils.RunCommand(totalMemsCmd1)
				if strings.Contains(totalMemsTmp, "M") {
					totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "M")[0])
					totalMems = totalMemsInt
				} else if strings.Contains(totalMemsTmp, "G") {
					totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "G")[0])
					totalMems = totalMemsInt * 1024
				} else if strings.Contains(totalMemsTmp, "T") {
					totalMemsInt, _ := strconv.Atoi(strings.Split(totalMemsTmp, "T")[0])
					totalMems = totalMemsInt * 1024 * 1024
				}
				totalMemInt = totalMems
			} else {
				// 取节点名，默认取第一个元素，在判断有没有[特殊符合
				getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
				nodeOutput, _ := utils.RunCommand(getPartitionNodeNameCmd)
				nodeArray := strings.Split(nodeOutput, ",")
				res := strings.Contains(nodeArray[0], "[")
				if res {
					getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
					nodeNameOutput, _ := utils.RunCommand(getNodeNameCmd)
					nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
					getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  mem= | awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeName)
					memOutput, _ := utils.RunCommand(getMemCmd)
					nodeMem, _ := strconv.Atoi(memOutput)
					totalMemInt = nodeMem * totalNodeNumInt
				} else {
					getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  mem= | awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeArray[0])
					memOutput, _ := utils.RunCommand(getMemCmd)
					nodeMem, _ := strconv.Atoi(memOutput)
					totalMemInt = nodeMem * totalNodeNumInt
				}
			}
			// 获取总cpu、总节点数
			getPartitionTotalCpusCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalCPUs | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
			totalCpus, _ := utils.RunCommand(getPartitionTotalCpusCmd)
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			getPartitionTotalNodesCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalNodes | awk '{print $3}' | awk -F'=' '{print $2}'", partition)
			totalNodes, _ := utils.RunCommand(getPartitionTotalNodesCmd)
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)

			// 取节点名，默认取第一个元素，在判断有没有[特殊符合
			getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
			nodeOutput, _ := utils.RunCommand(getPartitionNodeNameCmd)
			nodeArray := strings.Split(nodeOutput, ",")

			res := strings.Contains(nodeArray[0], "[")
			if res {
				getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
				nodeNameOutput, _ := utils.RunCommand(getNodeNameCmd)
				nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
				gpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeName)
				gpusOutput, _ := utils.RunCommand(gpusCmd)
				if gpusOutput == "Gres=(null)" {
					totalGpus = 0
				} else {
					// 字符串转整型
					perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
					totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
				}
			} else {
				getGpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeArray[0])
				gpusOutput, _ := utils.RunCommand(getGpusCmd)
				if gpusOutput == "Gres=(null)" {
					totalGpus = 0
				} else {
					perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
					totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
				}
			}
			getPartitionQosCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' QoS=' | awk '{print $3}'", partition)
			qosOutput, _ := utils.RunCommand(getPartitionQosCmd)
			qosArray := strings.Split(qosOutput, "=")

			// 获取AllowQos
			getPartitionAllowQosCmd := fmt.Sprintf("scontrol show partition=%s | grep AllowQos | awk '{print $3}'| awk -F'=' '{print $2}'", partition)
			// 返回的是字符串
			allowQosOutput, _ := utils.RunCommand(getPartitionAllowQosCmd)

			if qosArray[len(qosArray)-1] != "N/A" {
				qos = append(qos, qosArray[len(qosArray)-1])
			} else {
				if allowQosOutput == "ALL" {
					qos = qosList
				} else {
					qos = strings.Split(allowQosOutput, ",")
				}
			}
			parts = append(parts, &pb.Partition{
				Name:    partition,
				MemMb:   uint64(totalMemInt),
				Cores:   uint32(totalCpuInt),
				Gpus:    totalGpus,
				Nodes:   uint32(totalNodeNumInt),
				Qos:     qos,
				Comment: &comment,
			})
		} else {
			continue
		}
	}
	return &pb.GetAvailablePartitionsResponse{Partitions: parts}, nil
}

// job service
func (s *serverJob) CancelJob(ctx context.Context, in *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	var (
		userName string
		idJob    int
	)
	// 记录日志
	logger.Infof("Received request CancelJob: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	clusterName := configValue.MySQLConfig.ClusterName

	// 判断用户是否存在
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err := db.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	jobSqlConfig := fmt.Sprintf("SELECT id_job FROM %s_job_table WHERE id_job = ? AND state IN (0, 1, 2)", clusterName)
	err = db.QueryRow(jobSqlConfig, in.JobId).Scan(&idJob)
	if err != nil {
		// 不存在或者作业已经完成
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 取消作业
	response, err := utils.LocalCancelJob(in.UserId, int(in.JobId))
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "CANCEL_JOB_FAILED",
		}
		st := status.New(codes.Unknown, response)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	return &pb.CancelJobResponse{}, nil
}

func (s *serverJob) QueryJobTimeLimit(ctx context.Context, in *pb.QueryJobTimeLimitRequest) (*pb.QueryJobTimeLimitResponse, error) {
	var (
		timeLimit uint64
	)
	logger.Infof("Received request QueryJobTimeLimit: %v", in)
	clusterName := configValue.MySQLConfig.ClusterName

	// 通过jobId来查找作业信息
	jobSqlConfig := fmt.Sprintf("SELECT timelimit FROM %s_job_table WHERE id_job = ?", clusterName)
	err := db.QueryRow(jobSqlConfig, in.JobId).Scan(&timeLimit)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	return &pb.QueryJobTimeLimitResponse{TimeLimitMinutes: timeLimit}, nil
}

func (s *serverJob) ChangeJobTimeLimit(ctx context.Context, in *pb.ChangeJobTimeLimitRequest) (*pb.ChangeJobTimeLimitResponse, error) {
	var (
		idJob int
	)
	// 记录日志
	logger.Infof("Received request ChangeJobTimeLimit: %v", in)

	clusterName := configValue.MySQLConfig.ClusterName

	// 判断作业在不在排队、运行、暂停的状态
	jobSqlConfig := fmt.Sprintf("SELECT id_job FROM %s_job_table WHERE id_job = ? AND state IN (0, 1, 2)", clusterName)
	err := db.QueryRow(jobSqlConfig, in.JobId).Scan(&idJob)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	if in.DeltaMinutes >= 0 {
		updateTimeLimitCmd := fmt.Sprintf("scontrol update job=%d TimeLimit+=%d", in.JobId, in.DeltaMinutes)
		utils.RunCommand(updateTimeLimitCmd)
	} else {
		minitues := int64(math.Abs(float64(in.DeltaMinutes)))
		updateTimeLimitCmd := fmt.Sprintf("scontrol update job=%d TimeLimit-=%d", in.JobId, minitues)
		utils.RunCommand(updateTimeLimitCmd)
	}
	return &pb.ChangeJobTimeLimitResponse{}, nil
}

func (s *serverJob) GetJobById(ctx context.Context, in *pb.GetJobByIdRequest) (*pb.GetJobByIdResponse, error) {
	var (
		jobId            int
		jobName          string
		account          string
		partition        string
		idQos            int
		state            int
		cpusReq          int32
		memReq           int64
		nodeReq          int32
		timeLimitMinutes int64
		submitTime       int64
		startTime        int64
		timeSuspended    int64
		gresUsed         string
		elapsedSeconds   int64
		reason           string
		nodeList         string
		gpusAlloc        int32
		cpusAlloc        int32
		memAllocMb       int64
		nodesAlloc       int32
		endTime          int64
		workingDirectory string
		qosName          string
		stateString      string
		gpuId            int
		tresAlloc        string
		tresReq          string
		idUser           int
		cpuTresId        int
		memTresId        int
		nodeTresId       int
		stdoutPath       string
		stderrPath       string
		gpuIdList        []int
	)
	var fields []string = in.Fields

	// 记录日志
	logger.Infof("Received request GetJobById: %v", in)

	clusterName := configValue.MySQLConfig.ClusterName
	// 根据jobid查询作业详细信息
	jobSqlConfig := fmt.Sprintf("SELECT account, id_user, cpus_req, job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, work_dir, tres_alloc, tres_req FROM %s_job_table WHERE id_job = ?", clusterName)
	err := db.QueryRow(jobSqlConfig, in.JobId).Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &memReq, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询cputresId、memTresId、nodeTresId值
	cpuTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'cpu'"
	memTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'mem'"
	nodeTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'node'"
	db.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	db.QueryRow(memTresSqlConfig).Scan(&memTresId)
	db.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	stateString = utils.ChangeState(state)
	submitTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(submitTime, 0).Unix())}
	startTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(startTime, 0).Unix())}
	endTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(endTime, 0).Unix())}

	// username 转换，需要从ldap中拿数据
	userName, _ := utils.GetUserNameByUid(idUser)

	// 查询qos的名字
	qosSqlConfig := "SELECT name FROM qos_table WHERE id = ?"
	db.QueryRow(qosSqlConfig, idQos).Scan(&qosName)

	// 查找SelectType插件的值
	slurmConfigCmd := fmt.Sprintf("scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'")
	output, _ := utils.RunCommand(slurmConfigCmd)

	// 查询gpu对应的id信息
	gpuSqlConfig := "SELECT id FROM tres_table WHERE type = 'gres' AND deleted = 0"
	rows, err := db.Query(gpuSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&gpuId)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		gpuIdList = append(gpuIdList, gpuId)
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 状态为排队和挂起的作业信息
	if state == 0 || state == 2 {
		getReasonCmd := fmt.Sprintf("scontrol show job=%d |grep 'Reason=' | awk '{print $2}'| awk -F'=' '{print $2}'", jobId)
		output, _ := utils.RunCommand(getReasonCmd)
		reason = output
		// 获取 stdout stderr 路径信息(不用了)
		// getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
		// getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
		// StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
		// StderrPath, _ := utils.RunCommand(getStderrPathCmd)
		// stderrPath = StderrPath
		// stdoutPath = StdoutPath

		if state == 0 {
			cpusAlloc = 0
			memAllocMb = 0
			// getNodeReqCmd := fmt.Sprintf("squeue  -h | grep ' %d '  | awk '{print $7}'", jobId)
			// nodeReqOutput, _ := utils.RunCommand(getNodeReqCmd)
			// nodeNum, _ := strconv.Atoi(nodeReqOutput)
			// nodeReq = int32(nodeNum)
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
			elapsedSeconds = 0
			gpusAlloc = 0
		} else {
			cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
			memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))

			// getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId) // 优化: 用当前时间减去运行时间
			// elapsedSeconds = utils.GetElapsedSeconds(getElapsedSecondsCmd)

			elapsedSeconds = time.Now().Unix() - startTime

			if output == "cons_tres" || output == "cons_res" {
				if len(gpuIdList) == 0 {
					gpusAlloc = 0
				} else {
					gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
				}
			} else {
				gpusAlloc = 0
			}
		}
	} else if state == 1 {
		reason = "Running" // 正在运行的作业的信息
		cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
		memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
		nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
		// getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId) // 优化：用当前时间减去运行时间
		// elapsedSeconds = utils.GetElapsedSeconds(getElapsedSecondsCmd)

		elapsedSeconds = time.Now().Unix() - startTime

		// get stdout stderr path（不用了）
		// getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
		// getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
		// StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
		// StderrPath, _ := utils.RunCommand(getStderrPathCmd)
		// stderrPath = StderrPath
		// stdoutPath = StdoutPath

		if output == "cons_tres" || output == "cons_res" {
			if len(gpuIdList) == 0 {
				gpusAlloc = 0
			} else {
				// 从tres_alloc中解析出gpu对应的卡数
				gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
			}
		} else {
			gpusAlloc = 0
		}
	} else {
		reason = "end of job" // 结束状态的作业信息
		cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
		memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
		nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
		elapsedSeconds = endTime - startTime
		if output == "cons_tres" || output == "cons_res" {
			if len(gpuIdList) == 0 {
				gpusAlloc = 0
			} else {
				gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
			}
		} else {
			gpusAlloc = 0
		}
	}
	if len(fields) == 0 {
		jobInfo := &pb.JobInfo{
			JobId:            in.JobId,
			Name:             jobName,
			User:             userName,
			Reason:           &reason,
			Account:          account,
			Partition:        partition,
			Qos:              qosName,
			State:            stateString,
			CpusReq:          cpusReq,
			MemReqMb:         memReq,
			TimeLimitMinutes: timeLimitMinutes,
			SubmitTime:       submitTimeTimestamp,
			WorkingDirectory: workingDirectory,
			NodeList:         &nodeList,
			StartTime:        startTimeTimestamp,
			EndTime:          endTimeTimestamp,
			NodesAlloc:       &nodesAlloc,
			CpusAlloc:        &cpusAlloc,
			MemAllocMb:       &memAllocMb,
			NodesReq:         nodeReq,
			StdoutPath:       &stdoutPath,
			StderrPath:       &stderrPath,
			ElapsedSeconds:   &elapsedSeconds,
			GpusAlloc:        &gpusAlloc,
		}
		return &pb.GetJobByIdResponse{Job: jobInfo}, nil
	} else {
		jobInfo := &pb.JobInfo{}
		for _, field := range fields {
			switch field {
			case "job_id":
				jobInfo.JobId = in.JobId
			case "name":
				jobInfo.Name = jobName
			case "account":
				jobInfo.Account = account
			case "user":
				jobInfo.User = userName
			case "partition":
				jobInfo.Partition = partition
			case "qos":
				jobInfo.Qos = qosName
			case "state":
				jobInfo.State = stateString
			case "cpus_req":
				jobInfo.CpusReq = cpusReq
			case "mem_req_mb":
				jobInfo.MemReqMb = memReq
			case "nodes_req":
				jobInfo.NodesReq = nodeReq
			case "time_limit_minutes":
				jobInfo.TimeLimitMinutes = timeLimitMinutes
			case "submit_time":
				jobInfo.SubmitTime = submitTimeTimestamp
			case "working_directory":
				jobInfo.WorkingDirectory = workingDirectory
			case "stdout_path":
				jobInfo.StdoutPath = &stdoutPath
			case "stderr_path":
				jobInfo.StderrPath = &stderrPath
			case "start_time":
				jobInfo.StartTime = startTimeTimestamp
			case "elapsed_seconds":
				jobInfo.ElapsedSeconds = &elapsedSeconds
			case "reason":
				jobInfo.Reason = &reason
			case "node_list":
				jobInfo.NodeList = &nodeList
			case "gpus_alloc":
				jobInfo.GpusAlloc = &gpusAlloc
			case "cpus_alloc":
				jobInfo.CpusAlloc = &cpusAlloc
			case "mem_alloc_mb":
				jobInfo.MemAllocMb = &memAllocMb
			case "nodes_alloc":
				jobInfo.NodesAlloc = &nodesAlloc
			case "end_time":
				jobInfo.EndTime = endTimeTimestamp
			}
		}
		return &pb.GetJobByIdResponse{Job: jobInfo}, nil
	}
}

func (s *serverJob) GetJobs(ctx context.Context, in *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	var (
		jobId             int
		jobName           string
		account           string
		partition         string
		idQos             int
		state             int
		cpusReq           int32
		memReq            uint64
		timeLimitMinutes  int64
		idUser            int
		submitTime        int64
		startTime         int64
		timeSuspended     int64
		gresUsed          string
		nodeList          string
		nodesAlloc        int32
		endTime           int64
		workingDirectory  string
		qosName           string
		stateString       string
		gpuId             int
		tresAlloc         string
		tresReq           string
		jobSqlConfig      string
		jobSqlTotalConfig string
		startTimeFilter   int64
		endTimeFilter     int64
		submitStartTime   int64
		submitEndTime     int64
		count             int
		pageLimit         int
		totalCount        uint32
		cpuTresId         int
		memTresId         int
		nodeTresId        int
		accounts          []string
		gpuIdList         []int
		uidList           []int
		stateIdList       []int
		jobInfo           []*pb.JobInfo
		params            []interface{}
		totalParams       []interface{}
		pendingMap        map[int]string
		pendingUserMap    map[int]string
	)
	clusterName := configValue.MySQLConfig.ClusterName
	var fields []string = in.Fields

	// 记录日志
	logger.Infof("Received request GetJobs: %v", in)

	var filterStates = in.Filter.States // 这个是筛选的
	var baseStates = []string{"RUNNING", "PENDING", "SUSPEND"}
	var submitUser = in.Filter.Users
	setBool := utils.IsSubSet(baseStates, filterStates)

	pendingUserCmdTemp := fmt.Sprintf("squeue -t pending -u %s", strings.Join(submitUser, ","))
	pendingUserCmd := pendingUserCmdTemp + " --noheader --format='%i=%R' | tr '\\n' ';'"
	pendingUserResult, _ := utils.RunCommand(pendingUserCmd)
	if len(pendingUserResult) != 0 {
		pendingUserMap = utils.GetPendingMapInfo(pendingUserResult)
	}

	if setBool && len(filterStates) != 0 && len(submitUser) != 0 {
		getJobInfoCmdLine := fmt.Sprintf("squeue -u %s --noheader", strings.Join(submitUser, ","))
		getFullCmdLine := getJobInfoCmdLine + " " + "--format='%a %A %C %D %j %l %m %M %P %q %S %T %u %V %Z %n %N' | tr '\n' ','"
		runningjobInfo, _ := utils.RunCommand(getFullCmdLine)
		runningJobInfoList := strings.Split(runningjobInfo, ",")
		if len(runningJobInfoList) == 0 {
			return &pb.GetJobsResponse{Jobs: jobInfo}, nil
		}
		for _, v := range runningJobInfoList {
			var singerJobJobNodesAlloc int32
			var singerJobJobReason string
			var singerJobCpusAlloc int32
			var singerJobElapsedSeconds int64
			var singerJobInfoNodeList string
			var timeSubmit int64
			var singerJobTimeSubmit *timestamppb.Timestamp
			if len(v) != 0 {
				singerJobInfo := strings.Split(v, " ")
				singerJobAccount := singerJobInfo[0]
				singerJobUserName := singerJobInfo[12]
				singerJobJobId, _ := strconv.Atoi(singerJobInfo[1])
				singerJobState := singerJobInfo[11]
				singerJobJobPartition := singerJobInfo[8]
				singerJobJobName := singerJobInfo[4]
				singerJobQos := singerJobInfo[9]
				singerJobWorkingDirectory := singerJobInfo[14]
				singerJobtimeLimitMinutes, _ := strconv.Atoi(singerJobInfo[5])
				submittimeSqlConfig := fmt.Sprintf("SELECT time_submit FROM %s_job_table WHERE id_job = ?", clusterName)
				db.QueryRow(submittimeSqlConfig, singerJobJobId).Scan(&timeSubmit)
				if timeSubmit == 0 {
					singerJobTimeSubmit = &timestamppb.Timestamp{Seconds: int64(time.Now().Unix())}
				} else {
					singerJobTimeSubmit = &timestamppb.Timestamp{Seconds: int64(time.Unix(timeSubmit, 0).Unix())}
				}
				if singerJobState == "PENDING" {
					singerJobJobNodesAlloc = 0
					// singerJobJobReason = singerJobInfo[17] //
					if _, ok := pendingUserMap[singerJobJobId]; ok {
						singerJobJobReason = pendingUserMap[singerJobJobId]
					}
					singerJobCpusAlloc = 0
					singerJobElapsedSeconds = 0
					singerJobInfoNodeList = "None assigned"
				} else {
					singerJobJobNodesAllocTemp, _ := strconv.Atoi(singerJobInfo[3])
					singerJobJobNodesAlloc = int32(singerJobJobNodesAllocTemp)
					singerJobJobReason = singerJobInfo[11]
					singerJobCpusAllocTemp, _ := strconv.Atoi(singerJobInfo[2])
					singerJobCpusAlloc = int32(singerJobCpusAllocTemp)
					singerJobElapsedSeconds = utils.GetRunningElapsedSeconds(singerJobInfo[7])
					singerJobInfoNodeList = singerJobInfo[15]
				}
				jobInfo = append(jobInfo, &pb.JobInfo{
					JobId:            uint32(singerJobJobId),
					Name:             singerJobJobName,
					Account:          singerJobAccount,
					User:             singerJobUserName,
					Partition:        singerJobJobPartition,
					Qos:              singerJobQos,
					State:            singerJobState,
					TimeLimitMinutes: int64(singerJobtimeLimitMinutes),
					WorkingDirectory: singerJobWorkingDirectory,
					Reason:           &singerJobJobReason,
					CpusAlloc:        &singerJobCpusAlloc,
					NodesAlloc:       &singerJobJobNodesAlloc,
					ElapsedSeconds:   &singerJobElapsedSeconds,
					NodeList:         &singerJobInfoNodeList,
					SubmitTime:       singerJobTimeSubmit,
				})
			}
		}

		if in.Sort != nil && len(jobInfo) != 0 {
			// 排序
			var sortKey string
			if in.Sort.GetField() == "" {
				sortKey = "JobId"
			} else {
				sortKey = in.Sort.GetField()
				// 字段转换
				words := strings.Split(sortKey, "_")
				for i := 0; i < len(words); i++ {
					words[i] = strings.Title(words[i])
				}
				sortKey = strings.Join(words, "")
			}
			sortOrder := in.Sort.GetOrder().String()
			sortJobinfo := utils.SortJobInfo(sortKey, sortOrder, jobInfo)
			return &pb.GetJobsResponse{Jobs: sortJobinfo}, nil
		}
		return &pb.GetJobsResponse{Jobs: jobInfo}, nil
	}
	// 查找SelectType插件的值
	slurmSelectTypeConfigCmd := fmt.Sprintf("scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'")
	output, _ := utils.RunCommand(slurmSelectTypeConfigCmd)

	// cputresId、memTresId、nodeTresId
	cpuTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'cpu'"
	memTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'mem'"
	nodeTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'node'"
	db.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	db.QueryRow(memTresSqlConfig).Scan(&memTresId)
	db.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	gpuSqlConfig := "SELECT id FROM tres_table WHERE type = 'gres' AND deleted = 0"
	rowList, err := db.Query(gpuSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rowList.Close()
	for rowList.Next() {
		err := rowList.Scan(&gpuId)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		gpuIdList = append(gpuIdList, gpuId)
	}
	err = rowList.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	if in.PageInfo != nil {
		page := in.PageInfo.Page
		pageSize := in.PageInfo.PageSize
		pageLimit = int(pageSize)
		if page == 1 {
			pageSize = 0
		} else {
			pageSize = pageSize * uint64(page-1)
		}
		if in.Filter != nil {
			if in.Filter.EndTime != nil {
				startTimeFilter = in.Filter.EndTime.StartTime.GetSeconds()
				endTimeFilter = in.Filter.EndTime.EndTime.GetSeconds()
			}
			if in.Filter.SubmitTime != nil {
				submitStartTime = in.Filter.SubmitTime.StartTime.GetSeconds()
				submitEndTime = in.Filter.SubmitTime.EndTime.GetSeconds()
			}
			// 四种情况
			if len(in.Filter.Users) != 0 && len(in.Filter.States) != 0 {
				for _, user := range in.Filter.Users {
					uid, _, _ := utils.GetUserUidGid(user)
					uidList = append(uidList, uid)
				}
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account, id_user, cpus_req, job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, work_dir, tres_alloc, tres_req FROM %s_job_table WHERE id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, uidListString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, uidListString, stateIdListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, accountsString, uidListString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, uidListString, stateIdListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else if len(in.Filter.Users) != 0 && len(in.Filter.States) == 0 {
				for _, user := range in.Filter.Users {
					uid, _, _ := utils.GetUserUidGid(user)
					uidList = append(uidList, uid)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, uidListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, uidListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, accountsString, uidListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, uidListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else if len(in.Filter.Users) == 0 && len(in.Filter.States) != 0 {
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, stateIdListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, accountsString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE account IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, stateIdListString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else {
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0) ORDER BY id_job LIMIT ? OFFSET ?", clusterName, accountsString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize}
					jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE account IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString)
					totalParams = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			}
		} else {
			jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table LIMIT ? OFFSET ?", clusterName)
			params = []interface{}{pageLimit, pageSize}
			jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table", clusterName)
		}
	} else {
		// 不分页的情况
		if in.Filter != nil {
			if in.Filter.EndTime != nil {
				startTimeFilter = in.Filter.EndTime.StartTime.GetSeconds()
				endTimeFilter = in.Filter.EndTime.EndTime.GetSeconds()
			}
			if in.Filter.SubmitTime != nil {
				submitStartTime = in.Filter.SubmitTime.StartTime.GetSeconds()
				submitEndTime = in.Filter.SubmitTime.EndTime.GetSeconds()
			}
			// 四种情况
			if len(in.Filter.Users) != 0 && len(in.Filter.States) != 0 {
				for _, user := range in.Filter.Users {
					uid, _, _ := utils.GetUserUidGid(user)
					uidList = append(uidList, uid)
				}
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, uidListString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, uidListString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else if len(in.Filter.Users) != 0 && len(in.Filter.States) == 0 {
				for _, user := range in.Filter.Users {
					uid, _, _ := utils.GetUserUidGid(user)
					uidList = append(uidList, uid)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, uidListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND id_user IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, uidListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else if len(in.Filter.Users) == 0 && len(in.Filter.States) != 0 {
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND state IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString, stateIdListString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			} else {
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table WHERE account IN (%s) AND (time_end >= ? OR ? = 0) AND (time_end <= ? OR ? = 0) AND (time_submit >= ? OR ? = 0) AND (time_submit <= ? OR ? = 0)", clusterName, accountsString)
					params = []interface{}{startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime}
				}
			}
		} else {
			jobSqlConfig = fmt.Sprintf("SELECT account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,`partition`,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req FROM %s_job_table", clusterName)
		}
	}
	rows, err := db.Query(jobSqlConfig, params...)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	defer rows.Close()

	pendingCmd := "squeue -t pending --noheader --format='%i %R' | tr '\n' ','"
	pendingResult, _ := utils.RunCommand(pendingCmd)
	if len(pendingResult) != 0 {
		pendingMap = utils.GetMapInfo(pendingResult)
	}
	for rows.Next() {
		err := rows.Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &memReq, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
		if err != nil {
			continue
		}
		var elapsedSeconds int64
		var reason string
		var stdoutPath string
		var stderrPath string
		var gpusAlloc int32
		var cpusAlloc int32
		var memAllocMb int64
		var nodeReq int32
		var nodesAllocTemp int32
		var nodeListTemp string
		var startTimeTimestamp *timestamppb.Timestamp
		var endTimeTimestamp *timestamppb.Timestamp
		stateString = utils.ChangeState(state)
		submitTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(submitTime, 0).Unix())}
		if startTime != 0 {
			startTimeTimestamp = &timestamppb.Timestamp{Seconds: int64(time.Unix(startTime, 0).Unix())}
		}
		if endTime != 0 {
			endTimeTimestamp = &timestamppb.Timestamp{Seconds: int64(time.Unix(endTime, 0).Unix())}
		}

		userName, _ := utils.GetUserNameByUid(idUser)

		qosSqlconfig := "SELECT name FROM qos_table WHERE id = ? AND deleted = 0"
		db.QueryRow(qosSqlconfig, idQos).Scan(&qosName)

		nodesAllocTemp = nodesAlloc
		nodeListTemp = nodeList
		if state == 0 || state == 2 {
			if _, ok := pendingMap[jobId]; ok {
				reason = pendingMap[jobId]
			} else {
				getReasonCmdTmp := fmt.Sprintf("squeue -j %d --noheader ", jobId)
				getReasonCmd := getReasonCmdTmp + " --format='%R'"
				reason, _ = utils.RunCommand(getReasonCmd)
			} // 这里还要补充下逻辑

			if state == 0 {
				cpusAlloc = 0
				memAllocMb = 0
				nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
				elapsedSeconds = 0
				gpusAlloc = 0
			} else {
				cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
				memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
				nodeReq = nodesAlloc
				elapsedSeconds = time.Now().Unix() - startTime
				if output == "cons_tres" || output == "cons_res" {
					if len(gpuIdList) == 0 {
						gpusAlloc = 0
					} else {
						gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
					}
				} else {
					gpusAlloc = 0
				}
			}
		} else if state == 1 {
			reason = "Running"
			cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
			memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
			nodeReq = nodesAlloc
			elapsedSeconds = time.Now().Unix() - startTime
			if output == "cons_tres" || output == "cons_res" {
				if len(gpuIdList) == 0 {
					gpusAlloc = 0
				} else {
					gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
				}
			} else {
				gpusAlloc = 0
			}
		} else {
			reason = "end of job"
			cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
			memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
			nodeReq = nodesAlloc
			if startTime != 0 && endTime != 0 {
				elapsedSeconds = endTime - startTime
			}
			if output == "cons_tres" || output == "cons_res" {
				if len(gpuIdList) == 0 {
					gpusAlloc = 0
				} else {
					gpusAlloc = utils.GetGpuAllocsFromGpuIdList(tresAlloc, gpuIdList)
				}
			} else {
				gpusAlloc = 0
			}
		}
		// 低版本slurm mem_req 默认值转换为0
		if memReq == 9223372036854777728 || memReq == 9223372036854779808 || memReq > 9000000000000000000 {
			memReq = 0
		}
		if len(fields) == 0 {
			jobInfo = append(jobInfo, &pb.JobInfo{
				JobId:            uint32(jobId),
				Name:             jobName,
				Account:          account,
				User:             userName,
				Partition:        partition,
				Qos:              qosName,
				State:            stateString,
				CpusReq:          cpusReq,
				MemReqMb:         int64(memReq),
				TimeLimitMinutes: timeLimitMinutes,
				SubmitTime:       submitTimeTimestamp,
				WorkingDirectory: workingDirectory,
				NodeList:         &nodeListTemp,
				StartTime:        startTimeTimestamp,
				EndTime:          endTimeTimestamp,
				StdoutPath:       &stdoutPath,
				StderrPath:       &stderrPath,
				NodesReq:         nodeReq,
				ElapsedSeconds:   &elapsedSeconds,
				Reason:           &reason,
				CpusAlloc:        &cpusAlloc,
				MemAllocMb:       &memAllocMb,
				GpusAlloc:        &gpusAlloc,
				NodesAlloc:       &nodesAllocTemp,
			})
		} else {
			subJobInfo := &pb.JobInfo{}
			for _, field := range fields {
				switch field {
				case "job_id":
					subJobInfo.JobId = uint32(jobId)
				case "name":
					subJobInfo.Name = jobName
				case "account":
					subJobInfo.Account = account
				case "user":
					subJobInfo.User = userName
				case "partition":
					subJobInfo.Partition = partition
				case "qos":
					subJobInfo.Qos = qosName
				case "state":
					subJobInfo.State = stateString
				case "cpus_req":
					subJobInfo.CpusReq = cpusReq
				case "mem_req_mb":
					subJobInfo.MemReqMb = int64(memReq)
				case "nodes_req":
					subJobInfo.NodesReq = nodeReq
				case "time_limit_minutes":
					subJobInfo.TimeLimitMinutes = timeLimitMinutes
				case "submit_time":
					subJobInfo.SubmitTime = submitTimeTimestamp
				case "working_directory":
					subJobInfo.WorkingDirectory = workingDirectory
				case "stdout_path":
					subJobInfo.StdoutPath = &stdoutPath
				case "stderr_path":
					subJobInfo.StderrPath = &stderrPath
				case "start_time":
					subJobInfo.StartTime = startTimeTimestamp
				case "elapsed_seconds":
					subJobInfo.ElapsedSeconds = &elapsedSeconds
				case "reason":
					subJobInfo.Reason = &reason
				case "node_list":
					subJobInfo.NodeList = &nodeListTemp
				case "gpus_alloc":
					subJobInfo.GpusAlloc = &gpusAlloc
				case "cpus_alloc":
					subJobInfo.CpusAlloc = &cpusAlloc
				case "mem_alloc_mb":
					subJobInfo.MemAllocMb = &memAllocMb
				case "nodes_alloc":
					subJobInfo.NodesAlloc = &nodesAllocTemp
				case "end_time":
					subJobInfo.EndTime = endTimeTimestamp
				}
			}
			jobInfo = append(jobInfo, subJobInfo)
		}
	}
	err = rows.Err()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取总的页数逻辑
	if jobSqlTotalConfig != "" {
		db.QueryRow(jobSqlTotalConfig, totalParams...).Scan(&count)
		if count%pageLimit == 0 {
			totalCount = uint32(count) / uint32(pageLimit)
		} else {
			totalCount = uint32(count)/uint32(pageLimit) + 1
		}
		// 新加排序功能
		if in.Sort != nil && len(jobInfo) != 0 {
			// 排序
			var sortKey string
			if in.Sort.GetField() == "" {
				// 默认是按jobid排序
				sortKey = "JobId"
			} else {
				sortKey = in.Sort.GetField()
				// 字段转换
				words := strings.Split(sortKey, "_")
				for i := 0; i < len(words); i++ {
					words[i] = strings.Title(words[i])
				}
				sortKey = strings.Join(words, "")
			}
			sortOrder := in.Sort.GetOrder().String()
			sortJobinfo := utils.SortJobInfo(sortKey, sortOrder, jobInfo)
			return &pb.GetJobsResponse{Jobs: sortJobinfo}, nil
		}
		return &pb.GetJobsResponse{Jobs: jobInfo, TotalCount: &totalCount}, nil
	}
	// 新加排序功能
	if in.Sort != nil && len(jobInfo) != 0 {
		// 排序
		var sortKey string
		if in.Sort.GetField() == "" {
			// 默认是按jobid进行排序
			sortKey = "JobId"
		} else {
			sortKey = in.Sort.GetField()
			// 字段转换
			words := strings.Split(sortKey, "_")
			for i := 0; i < len(words); i++ {
				words[i] = strings.Title(words[i])
			}
			sortKey = strings.Join(words, "")
		}
		sortOrder := in.Sort.GetOrder().String()
		sortJobinfo := utils.SortJobInfo(sortKey, sortOrder, jobInfo)
		return &pb.GetJobsResponse{Jobs: sortJobinfo}, nil
	}
	return &pb.GetJobsResponse{Jobs: jobInfo}, nil
}

// 提交作业
func (s *serverJob) SubmitJob(ctx context.Context, in *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var (
		scriptString = "#!/bin/bash\n"
		name         string
		homedir      string
	)
	// 记录日志
	logger.Infof("Received request SubmitJob: %v", in)
	// 判断账户名、用户名是否包含大写字母
	resultAcct := utils.ContainsUppercase(in.Account)
	resultUser := utils.ContainsUppercase(in.UserId)
	if resultAcct || resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_UPPER_LETTER",
		}
		st := status.New(codes.Internal, "The account or username contains uppercase letters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE deleted = 0 AND name = ?"
	err := db.QueryRow(userSqlConfig, in.UserId).Scan(&name)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The user does not exists.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	modulepath := configValue.Modulepath.Path // 获取module配置文件路径

	// utils.ExecuteShellCommand("export PATH='/lustre/software/module/5.2.0/bin':$PATH")
	// 拼接提交作业的batch脚本
	scriptString += "#SBATCH " + "-A " + in.Account + "\n"
	scriptString += "#SBATCH " + "--partition=" + in.Partition + "\n"
	if in.Qos != nil {
		scriptString += "#SBATCH " + "--qos=" + *in.Qos + "\n"
	}
	scriptString += "#SBATCH " + "-J " + in.JobName + "\n"
	scriptString += "#SBATCH " + "--nodes=" + strconv.Itoa(int(in.NodeCount)) + "\n"
	scriptString += "#SBATCH " + "-c " + strconv.Itoa(int(in.CoreCount)) + "\n"
	if in.TimeLimitMinutes != nil {
		scriptString += "#SBATCH " + "--time=" + strconv.Itoa(int(*in.TimeLimitMinutes)) + "\n"
	}

	isAbsolute := filepath.IsAbs(in.WorkingDirectory)
	if !isAbsolute {
		homedirTemp, _ := utils.GetUserHomedir(in.UserId)
		homedir = homedirTemp + "/" + in.WorkingDirectory
	} else {
		homedir = in.WorkingDirectory
	}

	// scriptString += "#SBATCH " + "--chdir=" + in.WorkingDirectory + "\n"
	scriptString += "#SBATCH " + "--chdir=" + homedir + "\n"

	if in.Stdout != nil {
		scriptString += "#SBATCH " + "--output=" + *in.Stdout + "\n"
	}
	if in.Stderr != nil {
		scriptString += "#SBATCH " + "--error=" + *in.Stderr + "\n"
	}
	// 强行删除
	// if in.MemoryMb != nil {
	// 	scriptString += "#SBATCH " + "--mem=" + strconv.Itoa(int(*in.MemoryMb)) + "MB" + "\n"
	// }
	if in.GpuCount != 0 {
		scriptString += "#SBATCH " + "--gres=gpu:" + strconv.Itoa(int(in.GpuCount)) + "\n"
	}

	if len(in.ExtraOptions) != 0 {
		for _, extraVale := range in.ExtraOptions {
			scriptString += "#SBATCH " + extraVale + "\n"
		}
	}

	if !isAbsolute { // 提交的是交互式作业
		modulepathString := fmt.Sprintf("source %s", modulepath) // 改成从配置文件中获取profile文件路径信息
		scriptString += "\n" + modulepathString + "\n"
	} else {
		scriptString += "\n"
	}
	// scriptString += "\n" + "source /lustre/software/module/5.2.0/init/profile.sh\n"

	scriptString += in.Script
	// 提交作业

	submitResponse, err := utils.LocalSubmitJob(scriptString, in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SBATCH_FAILED",
		}
		st := status.New(codes.Unknown, submitResponse)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	responseList := strings.Split(strings.TrimSpace(string(submitResponse)), " ")
	jobIdString := responseList[len(responseList)-1]
	jobId, _ := strconv.Atoi(jobIdString)
	return &pb.SubmitJobResponse{JobId: uint32(jobId), GeneratedScript: scriptString}, nil
}

func main() {
	var (
		err error
	)
	// 连接数据库
	dbConfig := utils.DatabaseConfig()
	db, err = sql.Open("mysql", dbConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 创建日志实例
	logger = logrus.New()
	// 设置日志输出格式为JSON
	logger.SetFormatter(&logrus.JSONFormatter{})
	// 设置日志级别为Info
	logger.SetLevel(logrus.InfoLevel)
	// 设置日志输出到控制台和文件中
	file, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	logger.SetOutput(io.MultiWriter(os.Stdout, file))

	// 启动服务
	port := configValue.Service.Port
	portString := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", portString)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024), // 最大接受size 1GB
		grpc.MaxSendMsgSize(1024*1024*1024), // 最大发送size 1GB
	) // 创建gRPC服务器
	pb.RegisterUserServiceServer(s, &serverUser{})
	pb.RegisterAccountServiceServer(s, &serverAccount{})
	pb.RegisterConfigServiceServer(s, &serverConfig{})
	pb.RegisterJobServiceServer(s, &serverJob{})

	if err = s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
