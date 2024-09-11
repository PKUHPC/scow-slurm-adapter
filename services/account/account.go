package account

import (
	"context"
	"fmt"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strings"
	"sync"

	// "github.com/wxnacy/wgo/arrays"
	"github.com/wxnacy/wgo/arrays"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerAccount struct {
	pb.UnimplementedAccountServiceServer
	muBlock   sync.Mutex // Add a Mutex field for locking
	muUnBlock sync.Mutex // Add a Mutex field for locking
}

func (s *ServerAccount) ListAccounts(ctx context.Context, in *pb.ListAccountsRequest) (*pb.ListAccountsResponse, error) {
	var (
		userName  string
		assocAcct string
		acctList  []string
	)
	caller.Logger.Infof("Received request ListAccounts: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("ListAccounts failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取集群名
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName

	// 判断用户在slurm中是否存在
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("ListAccounts failed: %v", st.Err())
		return nil, st.Err()
	}
	// 查询用户相关联的所有账户信息
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE user = ? AND deleted = 0", clusterName)
	rows, err := caller.DB.Query(assocSqlConfig, in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("ListAccounts failed: %v", st.Err())
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
			caller.Logger.Errorf("ListAccounts failed: %v", st.Err())
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
		caller.Logger.Errorf("ListAccounts failed: %v", st.Err())
		return nil, st.Err()
	}
	caller.Logger.Tracef("ListAccounts Response: %v", &pb.ListAccountsResponse{Accounts: acctList})
	return &pb.ListAccountsResponse{Accounts: acctList}, nil
}

func (s *ServerAccount) CreateAccount(ctx context.Context, in *pb.CreateAccountRequest) (*pb.CreateAccountResponse, error) {
	var (
		acctName string
		qosName  string
		qosList  []string
	)
	caller.Logger.Infof("Received request CreateAccount: %v", in)
	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	resultUser := utils.CheckAccountOrUserStrings(in.OwnerUserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取系统中默认的Qos信息
	defaultQos := caller.ConfigValue.Slurm.DefaultQOS
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		partitions, err := utils.GetPartitionInfo() // 获取系统中计算分区信息
		if err != nil || len(partitions) == 0 {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
			return nil, st.Err()
		}
		// 获取系统中Qos
		qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
		rows, err := caller.DB.Query(qosSqlConfig)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
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
				caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
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
			caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
			return nil, st.Err()
		}
		baseQos := strings.Join(qosList, ",")
		createAccountCmd := fmt.Sprintf("sacctmgr -i create account name=%s", in.AccountName)
		retcode := utils.ExecuteShellCommand(createAccountCmd)
		if retcode != 0 {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
			return nil, st.Err()
		}
		for _, p := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name=%s partition=%s account=%s", in.OwnerUserId, p, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos=%s DefaultQOS=%s", in.OwnerUserId, baseQos, defaultQos)
			retcode01 := utils.ExecuteShellCommand(createUserCmd)
			if retcode01 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed.")
				st, _ = st.WithDetails(errInfo)
				caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
				return nil, st.Err()
			}
			retcode02 := utils.ExecuteShellCommand(modifyUserCmd)
			if retcode02 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed.")
				st, _ = st.WithDetails(errInfo)
				caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
				return nil, st.Err()
			}
		}
		caller.Logger.Infof("CreateAccount sucess! account is: %v, owerUserId is: %v", in.AccountName, in.OwnerUserId)
		return &pb.CreateAccountResponse{}, nil
	}
	errInfo := &errdetails.ErrorInfo{
		Reason: "ACCOUNT_ALREADY_EXISTS",
	}
	message := fmt.Sprintf("The %s is already exists.", in.AccountName)
	st := status.New(codes.AlreadyExists, message)
	st, _ = st.WithDetails(errInfo)
	caller.Logger.Errorf("CreateAccount failed: %v", st.Err())
	return nil, st.Err()
}

func (s *ServerAccount) BlockAccount(ctx context.Context, in *pb.BlockAccountRequest) (*pb.BlockAccountResponse, error) {
	var (
		acctName      string
		assocAcctName string
		acctList      []string
	)
	// 记录日志
	caller.Logger.Infof("Received request BlockAccount: %v", in)
	s.muBlock.Lock()
	defer s.muBlock.Unlock()
	// 检查账户名中是否包含大写字母
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	if !resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
		return nil, st.Err()
	}

	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取计算分区AllowAccounts的值
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	// getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", "compute")

	output, err := utils.RunCommand(getAllowAcctCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	if output == "ALL" {
		acctSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE deleted = 0 AND acct != ?", clusterName)
		rows, err := caller.DB.Query(acctSqlConfig, in.AccountName)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
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
				caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
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
			caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
			return nil, st.Err()
		}
		allowAcct := strings.Join(acctList, ",")
		for _, v := range partitions {
			updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition='%s' AllowAccounts='%s'", v, allowAcct)
			retcode := utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
			if retcode != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed.")
				st, _ = st.WithDetails(errInfo)
				caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
				return nil, st.Err()
			}
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
		code := utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
		if code != 0 {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("BlockAccount failed: %v", st.Err())
			return nil, st.Err()
		}
	}
	caller.Logger.Infof("BlockAccount sucess! account is: %v", in.AccountName)
	return &pb.BlockAccountResponse{}, nil
}

func (s *ServerAccount) UnblockAccount(ctx context.Context, in *pb.UnblockAccountRequest) (*pb.UnblockAccountResponse, error) {
	var (
		acctName string
	)
	// 记录日志
	caller.Logger.Infof("Received request UnblockAccount: %v", in)
	s.muUnBlock.Lock() // 加锁操作
	defer s.muUnBlock.Unlock()
	// 检查用户名中是否包含大写字母
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	if !resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("UnblockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("UnblockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("UnblockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, err := utils.RunCommand(getAllowAcctCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("UnblockAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	if output == "ALL" {
		caller.Logger.Infof("Accout %v is Unblocked!", in.AccountName)
		return &pb.UnblockAccountResponse{}, nil
	}
	AllowAcctList := strings.Split(output, ",")
	index := arrays.ContainsString(AllowAcctList, in.AccountName)
	if index == -1 {
		// 不在里面的话需要解封
		AllowAcctList = append(AllowAcctList, in.AccountName)
		for _, p := range partitions {
			updatePartitionAllowAcctCmd := fmt.Sprintf("scontrol update partition=%s AllowAccounts=%s", p, strings.Join(AllowAcctList, ","))
			retcode := utils.ExecuteShellCommand(updatePartitionAllowAcctCmd)
			if retcode != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed.")
				st, _ = st.WithDetails(errInfo)
				caller.Logger.Errorf("UnblockAccount failed: %v", st.Err())
				return nil, st.Err()
			}
		}
		caller.Logger.Infof("Accout %v Unblocked sucess!", in.AccountName)
		return &pb.UnblockAccountResponse{}, nil
	}
	return &pb.UnblockAccountResponse{}, nil
}

func (s *ServerAccount) GetAllAccountsWithUsers(ctx context.Context, in *pb.GetAllAccountsWithUsersRequest) (*pb.GetAllAccountsWithUsersResponse, error) {
	var (
		acctName      string
		userName      string
		maxSubmitJobs int
		acctList      []string
		acctInfo      []*pb.ClusterAccountInfo
	)
	// 记录日志
	caller.Logger.Infof("Received request GetAllAccountsWithUsers: %v", in)
	// 获取集群名
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName

	// 获取系统中所有账户信息
	acctSqlConfig := fmt.Sprintf("SELECT name FROM acct_table WHERE deleted = 0")
	rows, err := caller.DB.Query(acctSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
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
			caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
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
		caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
		return nil, st.Err()
	}

	// 查询allowAcct的值(ALL和具体的acct列表)
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
		return nil, st.Err()
	}
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, err := utils.RunCommand(getAllowAcctCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取和每个账户关联的用户的信息
	for _, v := range acctList {
		var userInfo []*pb.ClusterAccountInfo_UserInAccount
		assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user, max_submit_jobs FROM %s_assoc_table WHERE deleted = 0 AND acct = ? AND user != ''", clusterName)
		rows, err := caller.DB.Query(assocSqlConfig, v)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SQL_QUERY_FAILED",
			}
			st := status.New(codes.Internal, err.Error())
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
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
			caller.Logger.Errorf("GetAllAccountsWithUsers failed: %v", st.Err())
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
	caller.Logger.Tracef("GetAllAccountsWithUsers: %v", acctInfo)
	return &pb.GetAllAccountsWithUsersResponse{Accounts: acctInfo}, nil
}

func (s *ServerAccount) QueryAccountBlockStatus(ctx context.Context, in *pb.QueryAccountBlockStatusRequest) (*pb.QueryAccountBlockStatusResponse, error) {
	var (
		acctName string
	)
	// 记录日志
	caller.Logger.Infof("Received request QueryAccountBlockStatus: %v", in)
	// 检查用户名中是否包含大写字母
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	if !resultAcct {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("QueryAccountBlockStatus failed: %v", st.Err())
		return nil, st.Err()
	}
	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("QueryAccountBlockStatus failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取系统中计算分区信息
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("QueryAccountBlockStatus failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取系统中分区AllowAccounts信息
	getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", partitions[0])
	output, err := utils.RunCommand(getAllowAcctCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("QueryAccountBlockStatus failed: %v", st.Err())
		return nil, st.Err()
	}
	if output == "ALL" {
		return &pb.QueryAccountBlockStatusResponse{Blocked: false}, nil
	}
	acctList := strings.Split(output, ",")
	index := arrays.ContainsString(acctList, in.AccountName)
	if index == -1 {
		caller.Logger.Infof("Account %v is Blocked", in.AccountName)
		return &pb.QueryAccountBlockStatusResponse{Blocked: true}, nil
	}
	caller.Logger.Infof("Account %v is Unblocked", in.AccountName)
	return &pb.QueryAccountBlockStatusResponse{Blocked: false}, nil
}

// 删除账户
func (s *ServerAccount) DeleteAccount(ctx context.Context, in *pb.DeleteAccountRequest) (*pb.DeleteAccountResponse, error) {
	var (
		acctName string
	)
	// 检查账户名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("DeleteAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	// 作业的判断
	accountRunningJobInfoCmd := fmt.Sprintf("squeue --noheader -A %s", in.AccountName)
	runningJobInfo, err := utils.RunCommand(accountRunningJobInfoCmd)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "CMD_EXECUTE_FAILED",
		}
		st := status.New(codes.NotFound, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("DeleteAccount failed: %v", st.Err())
		return nil, st.Err()
	}
	if len(runningJobInfo) == 0 {
		// 可以删
		// 具体的删除操作
		deleteAccountCmd := fmt.Sprintf("sacctmgr -i delete account name=%s", in.AccountName)
		_, err = utils.RunCommand(deleteAccountCmd)
		if err != nil {
			// 删除失败
			errInfo := &errdetails.ErrorInfo{
				Reason: "CMD_EXECUTE_FAILED",
			}
			st := status.New(codes.NotFound, err.Error())
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("DeleteAccount failed: %v", st.Err())
			return nil, st.Err()
		}
		return &pb.DeleteAccountResponse{}, nil
	} else {
		// 不能删
		errInfo := &errdetails.ErrorInfo{
			Reason: "HAVE_RUNNING_JOBS",
		}
		st := status.New(codes.NotFound, "Exist running jobs.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("DeleteAccount failed: %v", st.Err())
		return nil, st.Err()
	}
}

// func (s *ServerAccount) QueryAccountBlockStatus(ctx context.Context, in *pb.QueryAccountBlockStatusRequest) (*pb.QueryAccountBlockStatusResponse, error) {
// 	var (
// 		acctName                 string
// 		partitions               []string
// 		accountStatusInPartition []*pb.AccountStatusInPartition
// 		flag                     int
// 	)
// 	caller.Logger.Infof("Received request QueryAccountBlockStatus: %v", in)
// 	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
// 	if !resultAcct {
// 		errInfo := &errdetails.ErrorInfo{
// 			Reason: "ACCOUNT_CONTAIN_ILLEGAL_CHARACTERS",
// 		}
// 		st := status.New(codes.Internal, "The account contains illegal characters.")
// 		st, _ = st.WithDetails(errInfo)
// 		return nil, st.Err()
// 	}
// 	// 检查账户名是否在slurm中
// 	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
// 	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
// 	if err != nil {
// 		errInfo := &errdetails.ErrorInfo{
// 			Reason: "ACCOUNT_NOT_FOUND",
// 		}
// 		message := fmt.Sprintf("%s does not exists.", in.AccountName)
// 		st := status.New(codes.NotFound, message)
// 		st, _ = st.WithDetails(errInfo)
// 		return nil, st.Err()
// 	}
// 	// 获取系统中计算分区信息
// 	if len(in.QueriedPartitions) == 0 {
// 		partitions, err = utils.GetPartitionInfo()
// 		if err != nil || len(partitions) == 0 {
// 			errInfo := &errdetails.ErrorInfo{
// 				Reason: "COMMAND_EXEC_FAILED",
// 			}
// 			st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
// 			st, _ = st.WithDetails(errInfo)
// 			return nil, st.Err()
// 		}
// 	} else {
// 		partitions = in.QueriedPartitions
// 	}
// 	for _, p := range partitions {
// 		getAllowAcctCmd := fmt.Sprintf("scontrol show partition %s | grep AllowAccounts | awk '{print $2}' | awk -F '=' '{print $2}'", p)
// 		output, err := utils.RunCommand(getAllowAcctCmd)
// 		if err != nil || utils.CheckSlurmStatus(output) {
// 			errInfo := &errdetails.ErrorInfo{
// 				Reason: "COMMAND_EXEC_FAILED",
// 			}
// 			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
// 			st, _ = st.WithDetails(errInfo)
// 			return nil, st.Err()
// 		}
// 		if output == "ALL" {
// 			accountStatusInPartition = append(accountStatusInPartition, &pb.AccountStatusInPartition{
// 				Blocked:   false,
// 				Partition: p,
// 			})
// 		} else {
// 			acctList := strings.Split(output, ",")
// 			index := arrays.ContainsString(acctList, in.AccountName)
// 			if index == -1 {
// 				flag += 1
// 				accountStatusInPartition = append(accountStatusInPartition, &pb.AccountStatusInPartition{
// 					Blocked:   true,
// 					Partition: p,
// 				})
// 			} else {
// 				accountStatusInPartition = append(accountStatusInPartition, &pb.AccountStatusInPartition{
// 					Blocked:   false,
// 					Partition: p,
// 				})
// 			}
// 		}
// 	}
// 	if flag == len(partitions) {
// 		return &pb.QueryAccountBlockStatusResponse{Blocked: true, AccountBlockedDetails: accountStatusInPartition}, nil
// 	} else {
// 		return &pb.QueryAccountBlockStatusResponse{Blocked: false, AccountBlockedDetails: accountStatusInPartition}, nil
// 	}
// }
