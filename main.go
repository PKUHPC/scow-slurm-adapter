package main

import (
	"context"
	"database/sql"

	"fmt"
	"math"
	"net"
	config "scow-slurm-adapter/config"
	"scow-slurm-adapter/utils"
	"strconv"
	"strings"
	"time"

	pb "scow-slurm-adapter/gen/go"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wxnacy/wgo/arrays"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var configValue *config.Config
var db *sql.DB

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
	configValue = config.ParseConfig(config.DefaultConfigPath)
	dbConfig := utils.DatabaseConfig()
	var err error
	db, err = sql.Open("mysql", dbConfig)
	if err != nil {
		panic(err)
	}
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

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exists.").Err()
	}

	_, err = utils.SearchUidNumberFromLdap(in.UserId)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exists.").Err()
	}

	// 查询系统中的base Qos
	qosSqlConfig := fmt.Sprintf("select name from qos_table")
	rows, err := db.Query(qosSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&qosName)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		qosList = append(qosList, qosName)
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	baseQos := strings.Join(qosList, ",") // 系统中获取的baseQos的值
	// 查询用户是否在系统中
	partitions, _ := utils.GetPatitionInfo()
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, "normal")
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.AddUserToAccountResponse{}, nil
	}
	assocSqlConfig := fmt.Sprintf("select distinct user from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(assocSqlConfig).Scan(&user)
	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, "normal")
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.AddUserToAccountResponse{}, nil
	}
	// 关联已经存在的情况
	return nil, status.New(codes.AlreadyExists, "The user already exists in account.").Err()
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

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s'", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exist.").Err()
	}
	// ldap中不存在，slurm中的user中肯定不存在
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exist.").Err()
	}

	assocSqlConfig := fmt.Sprintf("select distinct user from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(assocSqlConfig).Scan(&user)
	if err != nil {
		return nil, status.New(codes.NotFound, "User and account assocation is not exists!").Err()
	}

	// 关联关系存在的情况下
	assocAcctSqlConfig := fmt.Sprintf("select distinct acct from %s_assoc_table where user = '%s' and deleted = 0 and acct != '%s'", clusterName, in.UserId, in.AccountName)
	rows, err := db.Query(assocAcctSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&acct)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		acctList = append(acctList, acct)
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}

	uid, err := utils.SearchUidNumberFromLdap(in.UserId)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exists.").Err()
	}

	jobSqlConfig := fmt.Sprintf("select job_name from %s_job_table where id_user = %d and account  = '%s' and state in (0, 1, 2)", clusterName, uid, in.AccountName)
	jobRows, err := db.Query(jobSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "The job query failed.").Err()
	}
	defer jobRows.Close()
	for jobRows.Next() {
		err := jobRows.Scan(&jobName)
		if err != nil {
			return nil, status.New(codes.Internal, "The job query failed.").Err()
		}
		jobList = append(jobList, jobName)
	}
	err = jobRows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "The job query failed.").Err()
	}

	if len(acctList) == 0 {
		// 有作业直接出错返回
		if len(jobList) != 0 {
			return nil, status.New(codes.Internal, "This user have running jobs!").Err()
		}

		// 没作业下直接删除用户
		deletedUserCmd := fmt.Sprintf("sacctmgr -i delete user name=%s account=%s", in.UserId, in.AccountName)
		res := utils.ExecuteShellCommand(deletedUserCmd)
		if res == 0 {
			return &pb.RemoveUserFromAccountResponse{}, nil
		}
		return nil, status.New(codes.Internal, "Shell command execute falied!").Err()
	}
	// 更改默认账号
	if len(jobList) != 0 {
		return nil, status.New(codes.Internal, "This user is running some jobs!").Err()
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

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exist.").Err()
	}
	// ldap中不存在的话在slurm的user表中肯定也不存在
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exist.").Err()
	}
	assocSqlConfig := fmt.Sprintf("select distinct user from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(assocSqlConfig).Scan(&user)
	if err != nil {
		return nil, status.New(codes.NotFound, "User and account assocation is not exists!").Err()
	}
	// 关联存在的情况下直接封锁账户
	blockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name=%s account=%s set MaxSubmitJobs=0  MaxJobs=0 MaxWall=00:00:00  GrpJobs=0 GrpSubmit=0 GrpSubmitJobs=0 MaxSubmitJobs=0 GrpWall=00:00:00", in.UserId, in.AccountName)
	res := utils.ExecuteShellCommand(blockUserCmd)
	if res == 0 {
		return &pb.BlockUserInAccountResponse{}, nil
	}
	return nil, status.New(codes.Internal, "Shell command execute falied!").Err()
}

func (s *serverUser) UnblockUserInAccount(ctx context.Context, in *pb.UnblockUserInAccountRequest) (*pb.UnblockUserInAccountResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exist.").Err()
	}
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exist.").Err()
	}
	assocSqlConfig := fmt.Sprintf("select distinct user from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(assocSqlConfig).Scan(&user)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user is not associated with the account").Err()
	}
	// 最大提交作业数为NULL表示没被封锁
	maxSubmitJobsSqlConfig := fmt.Sprintf("select distinct max_submit_jobs from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(maxSubmitJobsSqlConfig).Scan(&maxSubmitJobs)
	if err != nil {
		return &pb.UnblockUserInAccountResponse{}, nil
	}
	// 用户从账户中解封的操作
	unblockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name='%s' account='%s' set MaxSubmitJobs=-1 MaxJobs=-1 MaxWall=-1  GrpJobs=-1 GrpSubmit=-1 GrpSubmitJobs=-1 MaxSubmitJobs=-1 GrpWall=-1", in.UserId, in.AccountName)
	res := utils.ExecuteShellCommand(unblockUserCmd)
	if res == 0 {
		return &pb.UnblockUserInAccountResponse{}, nil
	}
	return nil, status.New(codes.Internal, "Shell command execute falied!").Err()
}

func (s *serverUser) QueryUserInAccountBlockStatus(ctx context.Context, in *pb.QueryUserInAccountBlockStatusRequest) (*pb.QueryUserInAccountBlockStatusResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exist.").Err()
	}

	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exist.").Err()
	}
	assocSqlConfig := fmt.Sprintf("select distinct user from %s_assoc_table where user = '%s' and acct  = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(assocSqlConfig).Scan(&user)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user is not associated with the account").Err()
	}
	maxSubmitJobSqlConfig := fmt.Sprintf("select distinct max_submit_jobs from %s_assoc_table where user = '%s' and acct = '%s' and deleted = 0", clusterName, in.UserId, in.AccountName)
	err = db.QueryRow(maxSubmitJobSqlConfig).Scan(&maxSubmitJobs)
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

	clusterName := configValue.MySQLConfig.ClusterName

	// 判断用户是否存在
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err := db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exist.").Err()
	}
	// 查询和用户相关的账户信息
	assocSqlConfig := fmt.Sprintf("select acct from %s_assoc_table where user = '%s' and deleted = 0", clusterName, in.UserId)
	rows, err := db.Query(assocSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&assocAcct)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		acctList = append(acctList, assocAcct)
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	return &pb.ListAccountsResponse{Accounts: acctList}, nil
}

func (s *serverAccount) CreateAccount(ctx context.Context, in *pb.CreateAccountRequest) (*pb.CreateAccountResponse, error) {
	var (
		acctName string
		qosName  string
		qosList  []string
	)

	_, err := utils.SearchUidNumberFromLdap(in.OwnerUserId)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exists.").Err()
	}

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err = db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		partitions, _ := utils.GetPatitionInfo() // 获取系统中计算分区信息
		// 获取系统中Qos
		qosSqlConfig := fmt.Sprintf("select name from qos_table")
		rows, err := db.Query(qosSqlConfig)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&qosName)
			if err != nil {
				return nil, status.New(codes.Internal, "Database query failed.").Err()
			}
			qosList = append(qosList, qosName)
		}

		err = rows.Err()
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		baseQos := strings.Join(qosList, ",")
		createAccountCmd := fmt.Sprintf("sacctmgr -i create account name=%s", in.AccountName)
		utils.ExecuteShellCommand(createAccountCmd)
		for _, p := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name=%s partition=%s account=%s", in.OwnerUserId, p, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos=%s DefaultQOS=%s", in.OwnerUserId, baseQos, "normal")
			utils.ExecuteShellCommand(createUserCmd)
			utils.ExecuteShellCommand(modifyUserCmd)
		}
		return &pb.CreateAccountResponse{}, nil
	}
	return nil, status.New(codes.AlreadyExists, "The account is already exists.").Err()
}

func (s *serverAccount) BlockAccount(ctx context.Context, in *pb.BlockAccountRequest) (*pb.BlockAccountResponse, error) {
	var (
		acctName      string
		assocAcctName string
		acctList      []string
	)

	clusterName := configValue.MySQLConfig.ClusterName

	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "Account does not exist.").Err()
	}
	partitions, _ := utils.GetPatitionInfo()
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)
	if output == "ALL" {
		acctSqlConfig := fmt.Sprintf("select DISTINCT acct from %s_assoc_table where deleted=0 and acct != '%s'", clusterName, in.AccountName)
		rows, err := db.Query(acctSqlConfig)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&assocAcctName)
			if err != nil {
				return nil, status.New(codes.Internal, "Database query failed.").Err()
			}
			acctList = append(acctList, assocAcctName)
		}
		err = rows.Err()
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
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
	index := arrays.ContainsString(AllowAcctList, in.AccountName)
	if index == -1 {
		return &pb.BlockAccountResponse{}, nil
	}
	// 账号存在
	updateAllowAcct := utils.DeleteSlice2(AllowAcctList, in.AccountName)
	for _, p := range partitions {
		updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition=%s AllowAccounts=%s", p, strings.Join(updateAllowAcct, ","))
		utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
	}
	// updateSlurmConfigFile := fmt.Sprintf("sed -i 's/\\(AllowAccounts=\\).*/\\1%s/'   /etc/slurm/slurm.conf", strings.Join(updateAllowAcct, ","))
	return &pb.BlockAccountResponse{}, nil
}

func (s *serverAccount) UnblockAccount(ctx context.Context, in *pb.UnblockAccountRequest) (*pb.UnblockAccountResponse, error) {
	var (
		acctName string
	)
	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The account does not exists.").Err()
	}
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
	clusterName := configValue.MySQLConfig.ClusterName

	// 多行数据的搜索
	acctSqlConfig := fmt.Sprintf("select name from acct_table where deleted = 0")
	rows, err := db.Query(acctSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&acctName)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		acctList = append(acctList, acctName)
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}

	// 查询allowAcct的值(ALL和具体的acct列表)
	partitions, _ := utils.GetPatitionInfo()
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, _ := utils.RunCommand(getAllowAcctCmd)

	for _, v := range acctList {
		var userInfo []*pb.ClusterAccountInfo_UserInAccount
		assocSqlConfig := fmt.Sprintf("select distinct user, max_submit_jobs from %s_assoc_table where deleted = 0 and acct = '%s' and user != '' ", clusterName, v)
		rows, err := db.Query(assocSqlConfig)
		if err != nil {
			return nil, status.New(codes.Internal, "The user query failed.").Err()
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
			return nil, status.New(codes.Internal, "The user query failed.").Err()
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
	acctSqlConfig := fmt.Sprintf("select name from acct_table where name = '%s' and deleted = 0", in.AccountName)
	err := db.QueryRow(acctSqlConfig).Scan(&acctName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The account does not exists.").Err()
	}
	partitions, _ := utils.GetPatitionInfo()
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
	var parts []*pb.Partition // 定义返回的类型
	partitions, _ := utils.GetPatitionInfo()
	for _, partition := range partitions {
		var (
			totalGpus uint32
			comment   string
			qos       []string
		)
		getPartitionInfoCmd := fmt.Sprintf("scontrol show partition=%s | grep -i mem=", partition)
		output, _ := utils.RunCommand(getPartitionInfoCmd)
		configArray := strings.Split(output, ",")
		totalCpusCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $3}'", configArray[0])
		totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}' | awk -F'M' '{print $1}'", configArray[1])
		totalNodesCmd := fmt.Sprintf("echo %s | awk  -F'=' '{print $2}'", configArray[2])

		totalCpus, _ := utils.RunCommand(totalCpusCmd)
		totalMems, _ := utils.RunCommand(totalMemsCmd)
		totalNodes, _ := utils.RunCommand(totalNodesCmd)

		// 将字符串转换为int
		totalCpuInt, _ := strconv.Atoi(totalCpus)
		totalMemInt, _ := strconv.Atoi(totalMems)
		totalNodeNumInt, _ := strconv.Atoi(totalNodes)

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
				totalGpus = uint32(perNodeGpuNum)
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
		if qosArray[len(qosArray)-1] != "N/A" {
			qos = append(qos, qosArray[len(qosArray)-1])
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

// job service
func (s *serverJob) CancelJob(ctx context.Context, in *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	// 取消作业在登录节点上执行
	var (
		userName                string
		idJob                   int
		loginName               string
		loginNodeStatusResponse bool = false
	)

	clusterName := configValue.MySQLConfig.ClusterName
	loginNodes := configValue.LoginNodes

	// 判断用户是否存在
	userSqlConfig := fmt.Sprintf("select name from user_table where name = '%s' and deleted = 0", in.UserId)
	err := db.QueryRow(userSqlConfig).Scan(&userName)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exists.").Err()
	}
	// 用户存在的情况去查作业的情况
	jobSqlConfig := fmt.Sprintf("select id_job from %s_job_table where id_job = %d and state in (0, 1, 2)", clusterName, in.JobId)
	err = db.QueryRow(jobSqlConfig).Scan(&idJob)
	if err != nil {
		// 不存在或者作业已经完成
		return nil, status.New(codes.NotFound, "The job not found.").Err()
	}
	// 检测登录节点的存活状态
	for _, v := range loginNodes {
		loginNodeStatusResponse = utils.Ping(v)
		if loginNodeStatusResponse {
			loginName = v
			break
		}
	}
	if loginNodeStatusResponse == false {
		return nil, status.New(codes.NotFound, "The login nodes all dead.").Err()
	}
	host := fmt.Sprintf("%s:%d", loginName, 22)
	scancelJobCmd := fmt.Sprintf("scancel %d", in.JobId)
	scancelJobRes, err := utils.SshExectueShellCmd(host, in.UserId, scancelJobCmd)
	if err != nil {
		return nil, status.New(codes.NotFound, strings.Join(scancelJobRes, " ")).Err()
	}
	return &pb.CancelJobResponse{}, nil
}

func (s *serverJob) QueryJobTimeLimit(ctx context.Context, in *pb.QueryJobTimeLimitRequest) (*pb.QueryJobTimeLimitResponse, error) {
	var timeLimit uint64
	clusterName := configValue.MySQLConfig.ClusterName

	// 通过jobId来查找作业信息
	jobSqlConfig := fmt.Sprintf("select timelimit from %s_job_table where id_job = %d", clusterName, in.JobId)
	err := db.QueryRow(jobSqlConfig).Scan(&timeLimit)
	if err != nil {
		return nil, status.New(codes.NotFound, "The job does not exists.").Err()
	}
	return &pb.QueryJobTimeLimitResponse{TimeLimitMinutes: timeLimit}, nil
}

func (s *serverJob) ChangeJobTimeLimit(ctx context.Context, in *pb.ChangeJobTimeLimitRequest) (*pb.ChangeJobTimeLimitResponse, error) {
	var idJob int

	clusterName := configValue.MySQLConfig.ClusterName

	// 判断作业在不在排队、运行、暂停的状态
	jobSqlConfig := fmt.Sprintf("select id_job from %s_job_table where id_job = %d and state in (0, 1, 2)", clusterName, in.JobId)
	err := db.QueryRow(jobSqlConfig).Scan(&idJob)
	if err != nil {
		return nil, status.New(codes.NotFound, "The job does not exists.").Err()
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

	clusterName := configValue.MySQLConfig.ClusterName

	jobSqlConfig := fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where id_job = %d", clusterName, in.JobId)
	err := db.QueryRow(jobSqlConfig).Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &memReq, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
	if err != nil {
		return nil, status.New(codes.NotFound, "The job does not exists.").Err()
	}

	// cputresId、memTresId、nodeTresId
	cpuTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'cpu'")
	memTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'mem'")
	nodeTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'node'")
	db.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	db.QueryRow(memTresSqlConfig).Scan(&memTresId)
	db.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	stateString = utils.ChangeState(state)
	submitTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(submitTime, 0).Unix())}
	startTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(startTime, 0).Unix())}
	endTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(endTime, 0).Unix())}

	// username 转换，需要从ldap中拿数据
	userName, _ := utils.SearchUserUidFromLdap(idUser)

	qosSqlconfig := fmt.Sprintf("select name from qos_table where id = %d", idQos)
	db.QueryRow(qosSqlconfig).Scan(&qosName)

	// 查找SelectType插件的值
	slurmConfigCmd := fmt.Sprintf("scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'")
	output, _ := utils.RunCommand(slurmConfigCmd)

	gpuSqlConfig := fmt.Sprintf("select id from tres_table where type = 'gredds' and deleted = 0")
	rows, err := db.Query(gpuSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&gpuId)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		gpuIdList = append(gpuIdList, gpuId)
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}

	if state == 0 || state == 2 {
		getReasonCmd := fmt.Sprintf("scontrol show job=%d |grep 'Reason=' | awk '{print $2}'| awk -F'=' '{print $2}'", jobId)
		output, _ := utils.RunCommand(getReasonCmd)
		reason = output
		// get stdout stderr path
		getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
		getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
		StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
		StderrPath, _ := utils.RunCommand(getStderrPathCmd)
		stderrPath = StderrPath
		stdoutPath = StdoutPath

		if state == 0 {
			cpusAlloc = 0
			memAllocMb = 0
			getNodeReqCmd := fmt.Sprintf("squeue  -h | grep ' %d '  | awk '{print $7}'", jobId)
			nodeReqOutput, _ := utils.RunCommand(getNodeReqCmd)
			nodeNum, _ := strconv.Atoi(nodeReqOutput)
			nodeReq = int32(nodeNum)
			elapsedSeconds = 0
			gpusAlloc = 0
		} else {
			cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
			memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))

			getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId)
			elapsedSeconds = utils.FromCmdGetElapsedSeconds(getElapsedSecondsCmd)
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
		nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
		getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId)
		elapsedSeconds = utils.FromCmdGetElapsedSeconds(getElapsedSecondsCmd)

		// get stdout stderr path
		getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
		getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
		StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
		StderrPath, _ := utils.RunCommand(getStderrPathCmd)
		stderrPath = StderrPath
		stdoutPath = StdoutPath

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
		reason = "end of job"
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
		memReq            int64
		nodeReq           int32
		timeLimitMinutes  int64
		idUser            int
		submitTime        int64
		stdoutPath        string
		stderrPath        string
		startTime         int64
		timeSuspended     int64
		gresUsed          string
		elapsedSeconds    int64
		reason            string
		nodeList          string
		gpusAlloc         int32
		cpusAlloc         int32
		memAllocMb        int64
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
	)
	var fields []string = in.Fields

	clusterName := configValue.MySQLConfig.ClusterName

	// 查找SelectType插件的值
	slurmSelectTypeConfigCmd := fmt.Sprintf("scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'")
	output, _ := utils.RunCommand(slurmSelectTypeConfigCmd)

	// cputresId、memTresId、nodeTresId
	cpuTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'cpu'")
	memTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'mem'")
	nodeTresSqlConfig := fmt.Sprintf("select id from tres_table where type = 'node'")
	db.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	db.QueryRow(memTresSqlConfig).Scan(&memTresId)
	db.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	gpuSqlConfig := fmt.Sprintf("select id from tres_table where type = 'gres' and deleted = 0")
	rowList, err := db.Query(gpuSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
	}
	defer rowList.Close()
	for rowList.Next() {
		err := rowList.Scan(&gpuId)
		if err != nil {
			return nil, status.New(codes.Internal, "Database query failed.").Err()
		}
		gpuIdList = append(gpuIdList, gpuId)
	}
	err = rowList.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "Database query failed.").Err()
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
					uid, _ := utils.SearchUidNumberFromLdap(user)
					uidList = append(uidList, uid)
				}
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where id_user in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where id_user in (%s) and state  in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s) and id_user in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, accountsString, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where account in (%s) and id_user in (%s) and state  in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else if len(in.Filter.Users) != 0 && len(in.Filter.States) == 0 {
				for _, user := range in.Filter.Users {
					uid, _ := utils.SearchUidNumberFromLdap(user)
					uidList = append(uidList, uid)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s) and id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, accountsString, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where account in (%s) and id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else if len(in.Filter.Users) == 0 && len(in.Filter.States) != 0 {
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, accountsString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where account in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else {
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0) order by id_job limit %d offset %d", clusterName, accountsString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime, pageLimit, pageSize)
					jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table where account in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			}
		} else {
			jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table limit %d offset %d", clusterName, pageLimit, pageSize)
			jobSqlTotalConfig = fmt.Sprintf("select count(*) from %s_job_table", clusterName)
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
					uid, _ := utils.SearchUidNumberFromLdap(user)
					uidList = append(uidList, uid)
				}
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where id_user in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s)  and id_user in (%s) and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, uidListString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else if len(in.Filter.Users) != 0 && len(in.Filter.States) == 0 {
				for _, user := range in.Filter.Users {
					uid, _ := utils.SearchUidNumberFromLdap(user)
					uidList = append(uidList, uid)
				}
				uidListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s)  and id_user in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, uidListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else if len(in.Filter.Users) == 0 && len(in.Filter.States) != 0 {
				for _, state := range in.Filter.States {
					stateId := utils.GetStateId(state)
					stateIdList = append(stateIdList, stateId)
				}
				stateIdListString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s)  and state in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, stateIdListString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			} else {
				if len(in.Filter.Accounts) == 0 {
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				} else {
					accounts = in.Filter.Accounts
					accountsString := "'" + strings.Join(accounts, "','") + "'"
					jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table where account in (%s) and (time_end > %d or %d = 0) and (time_end < %d or %d = 0) and (time_submit > %d or %d = 0) and (time_submit < %d or %d = 0)", clusterName, accountsString, startTimeFilter, startTimeFilter, endTimeFilter, endTimeFilter, submitStartTime, submitStartTime, submitEndTime, submitEndTime)
				}
			}
		} else {
			jobSqlConfig = fmt.Sprintf("select account,id_user,cpus_req,job_name,id_job,id_qos,mem_req,nodelist,nodes_alloc,partition,state,timelimit,time_submit,time_start,time_end,time_suspended,gres_used,work_dir,tres_alloc,tres_req from %s_job_table", clusterName)
		}
	}
	rows, err := db.Query(jobSqlConfig)
	if err != nil {
		return nil, status.New(codes.Internal, "The job query failed.").Err()
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &memReq, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
		if err != nil {
			return nil, status.New(codes.Internal, "The job query failed.").Err()
		}
		qosSqlConfig := fmt.Sprintf("select name in qos_table where id = %d", idQos)
		db.QueryRow(qosSqlConfig).Scan(&qosName)
		stateString = utils.ChangeState(state)
		submitTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(submitTime, 0).Unix())}
		startTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(startTime, 0).Unix())}
		endTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(endTime, 0).Unix())}

		// username 转换，需要从ldap中拿数据
		userName, _ := utils.SearchUserUidFromLdap(idUser)

		qosSqlconfig := fmt.Sprintf("select name from qos_table where id = %d", idQos)
		db.QueryRow(qosSqlconfig).Scan(&qosName)

		if state == 0 || state == 2 {
			getReasonCmd := fmt.Sprintf("scontrol show job=%d |grep 'Reason=' | awk '{print $2}'| awk -F'=' '{print $2}'", jobId)
			output, _ := utils.RunCommand(getReasonCmd)
			reason = output
			// get stdout stderr path
			getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
			getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
			StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
			StderrPath, _ := utils.RunCommand(getStderrPathCmd)
			stderrPath = StderrPath
			stdoutPath = StdoutPath
			if state == 0 {
				cpusAlloc = 0
				memAllocMb = 0
				getNodeReqCmd := fmt.Sprintf("squeue  -h | grep ' %d '  | awk '{print $7}'", jobId)
				nodeReqOutput, _ := utils.RunCommand(getNodeReqCmd)
				jobId, _ := strconv.Atoi(nodeReqOutput)
				nodeReq = int32(jobId)
				elapsedSeconds = 0
				gpusAlloc = 0
			} else {
				cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
				memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
				nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
				getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId)
				elapsedSeconds = utils.FromCmdGetElapsedSeconds(getElapsedSecondsCmd)
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
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
			getElapsedSecondsCmd := fmt.Sprintf("scontrol show job=%d | grep 'RunTime' | awk '{print $1}' | awk -F'=' '{print $2}'", jobId)
			elapsedSeconds = utils.FromCmdGetElapsedSeconds(getElapsedSecondsCmd)
			// get stdout stderr path
			getStdoutPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdOut | awk -F'=' '{print $2}'", jobId)
			getStderrPathCmd := fmt.Sprintf("scontrol show job=%d | grep StdErr | awk -F'=' '{print $2}'", jobId)
			StdoutPath, _ := utils.RunCommand(getStdoutPathCmd)
			StderrPath, _ := utils.RunCommand(getStderrPathCmd)
			stderrPath = StderrPath
			stdoutPath = StdoutPath
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
			jobInfo = append(jobInfo, &pb.JobInfo{
				JobId:            uint32(jobId),
				Name:             jobName,
				Account:          account,
				User:             userName,
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
				StdoutPath:       &stdoutPath,
				StderrPath:       &stderrPath,
				NodesReq:         nodeReq,
				ElapsedSeconds:   &elapsedSeconds,
				Reason:           &reason,
				CpusAlloc:        &cpusAlloc,
				MemAllocMb:       &memAllocMb,
				GpusAlloc:        &gpusAlloc,
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
					subJobInfo.MemReqMb = memReq
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
					subJobInfo.NodeList = &nodeList
				case "gpus_alloc":
					subJobInfo.GpusAlloc = &gpusAlloc
				case "cpus_alloc":
					subJobInfo.CpusAlloc = &cpusAlloc
				case "mem_alloc_mb":
					subJobInfo.MemAllocMb = &memAllocMb
				case "nodes_alloc":
					subJobInfo.NodesAlloc = &nodesAlloc
				case "end_time":
					subJobInfo.EndTime = endTimeTimestamp
				}
			}
			jobInfo = append(jobInfo, subJobInfo)
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, status.New(codes.Internal, "The job query failed.").Err()
	}
	// 获取总的页数逻辑
	if jobSqlTotalConfig != "" {
		db.QueryRow(jobSqlTotalConfig).Scan(&count)
		if count%pageLimit == 0 {
			totalCount = uint32(count) / uint32(pageLimit)
		} else {
			totalCount = uint32(count)/uint32(pageLimit) + 1
		}
		return &pb.GetJobsResponse{Jobs: jobInfo, TotalCount: &totalCount}, nil
	}
	return &pb.GetJobsResponse{Jobs: jobInfo}, nil
}

// 提交作业
func (s *serverJob) SubmitJob(ctx context.Context, in *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var scriptString = "#!/bin/bash\n"
	var name string
	var loginName string
	var loginNodeStatusResponse bool = false

	loginNodes := configValue.LoginNodes

	// 检测登录节点的存活状态

	for _, v := range loginNodes {
		// loginNodeString := fmt.Sprintf("%v", v)
		loginNodeStatusResponse = utils.Ping(v)
		if loginNodeStatusResponse {
			loginName = v
			break
		}
	}
	if loginNodeStatusResponse == false {
		return nil, status.New(codes.NotFound, "The login nodes all dead.").Err()
	}

	dbConfig := utils.DatabaseConfig()
	db, err := sql.Open("mysql", dbConfig)
	userSqlConfig := fmt.Sprintf("select name from user_table where deleted = 0 and name = '%s'", in.UserId)
	err = db.QueryRow(userSqlConfig).Scan(&name)
	if err != nil {
		return nil, status.New(codes.NotFound, "The user does not exists.").Err()
	}

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
	scriptString += "#SBATCH " + "--chdir=" + in.WorkingDirectory + "\n"
	if in.Stdout != nil {
		scriptString += "#SBATCH " + "--output=" + *in.Stdout + "\n"
	}
	if in.Stderr != nil {
		scriptString += "#SBATCH " + "--error=" + *in.Stderr + "\n"
	}
	if in.MemoryMb != nil {
		scriptString += "#SBATCH " + "--mem=" + strconv.Itoa(int(*in.MemoryMb)) + "\n"
	}
	if in.GpuCount != 0 {
		scriptString += "#SBATCH " + "--gres=gpu:" + strconv.Itoa(int(in.GpuCount)) + "\n"
	}

	if len(in.ExtraOptions) != 0 {
		for _, extraVale := range in.ExtraOptions {
			scriptString += "#SBATCH " + extraVale + "\n"
		}
	}

	scriptString += "\n"
	scriptString += in.Script
	// ssh执行提交任务
	host := fmt.Sprintf("%s:%d", loginName, 22)
	submitJobRes, err := utils.SshSubmitJobCommand(host, in.UserId, scriptString, in.WorkingDirectory)
	if err != nil {
		return nil, status.New(codes.Unknown, strings.Join(submitJobRes, " ")).Err()
	}
	jobIdString := submitJobRes[len(submitJobRes)-1]
	jobId, _ := strconv.Atoi(jobIdString)
	return &pb.SubmitJobResponse{JobId: uint32(jobId), GeneratedScript: scriptString}, nil
}

func main() {
	// 监听本地8972端口
	lis, err := net.Listen("tcp", ":8972")
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer() // 创建gRPC服务器
	pb.RegisterUserServiceServer(s, &serverUser{})
	pb.RegisterAccountServiceServer(s, &serverAccount{})
	pb.RegisterConfigServiceServer(s, &serverConfig{})
	pb.RegisterJobServiceServer(s, &serverJob{})
	// 启动服务
	err = s.Serve(lis)
	if err != nil {
		fmt.Printf("failed to serve: %v", err)
		return
	}
}
