package user

import (
	"context"
	"fmt"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerUser struct {
	pb.UnimplementedUserServiceServer
}

func (s *ServerUser) AddUserToAccount(ctx context.Context, in *pb.AddUserToAccountRequest) (*pb.AddUserToAccountResponse, error) {
	var (
		acctName string
		userName string
		qosName  string
		user     string
		qosList  []string
	)
	caller.Logger.Infof("Received request AddUserToAccount: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	defaultQos := caller.ConfigValue.Slurm.DefaultQOS

	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName) // 重新判断一下
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)      // 重新判断一下
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账号是否存在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询系统中的base Qos
	qosSqlConfig := "SELECT name FROM qos_table WHERE deleted = 0"
	rows, err := caller.DB.Query(qosSqlConfig)
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
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)

	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, defaultQos)
			retcode01 := utils.ExecuteShellCommand(createUserCmd)
			if retcode01 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "EXEC_COMMAND_FAILED",
				}
				st := status.New(codes.AlreadyExists, "Command exec fail.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			retcode02 := utils.ExecuteShellCommand(modifyUserCmd)
			if retcode02 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "EXEC_COMMAND_FAILED",
				}
				st := status.New(codes.AlreadyExists, "Command exec fail.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
		}
		return &pb.AddUserToAccountResponse{}, nil
	}
	// 检查账户和用户之间是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)

	if err != nil {
		for _, v := range partitions {
			createUserCmd := fmt.Sprintf("sacctmgr -i create user name='%s' partition='%s' account='%s'", in.UserId, v, in.AccountName)
			modifyUserCmd := fmt.Sprintf("sacctmgr -i modify user %s set qos='%s' DefaultQOS='%s'", in.UserId, baseQos, defaultQos)
			retCode1 := utils.ExecuteShellCommand(createUserCmd)
			if retCode1 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "EXEC_COMMAND_FAILED",
				}
				st := status.New(codes.AlreadyExists, "Command exec fail. ")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			retCode2 := utils.ExecuteShellCommand(modifyUserCmd)
			if retCode2 != 0 {
				errInfo := &errdetails.ErrorInfo{
					Reason: "EXEC_COMMAND_FAILED",
				}
				st := status.New(codes.AlreadyExists, "Command exec fail.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
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

func (s *ServerUser) RemoveUserFromAccount(ctx context.Context, in *pb.RemoveUserFromAccountRequest) (*pb.RemoveUserFromAccountResponse, error) {
	var (
		acctName string
		userName string
		user     string
		acct     string
		jobName  string
		jobList  []string
		acctList []string
	)
	caller.Logger.Infof("Received request RemoveUserFromAccount: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账号名是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户名是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账户和用户之间是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s and %s assocation is not exists!", in.UserId, in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 查询除当前账户外的关联账户信息
	assocAcctSqlConfig := fmt.Sprintf("SELECT DISTINCT acct FROM %s_assoc_table WHERE user = ? AND deleted = 0 AND acct != ?", clusterName)
	rows, err := caller.DB.Query(assocAcctSqlConfig, in.UserId, in.AccountName)
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
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户是否有未结束的作业
	jobSqlConfig := fmt.Sprintf("SELECT job_name FROM %s_job_table WHERE id_user = ? AND account = ? AND state IN (0, 1, 2)", clusterName)
	jobRows, err := caller.DB.Query(jobSqlConfig, uid, in.AccountName)
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
			message := fmt.Sprintf("The %s have running jobs!", in.UserId)
			st := status.New(codes.Internal, message)
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}

		// 没作业下直接删除用户
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
		message := fmt.Sprintf("The %s have running jobs!", in.UserId)
		st := status.New(codes.Internal, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	updateDefaultAcctCmd := fmt.Sprintf("sacctmgr -i update user set DefaultAccount=%s where user=%s", acctList[0], in.UserId)
	retcode1 := utils.ExecuteShellCommand(updateDefaultAcctCmd)
	if retcode1 != 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXECUTE_FAILED",
		}
		st := status.New(codes.Internal, "Shell command execute falied!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	deleteUerFromAcctCmd := fmt.Sprintf("sacctmgr -i delete user name=%s account=%s", in.UserId, in.AccountName)
	retcode2 := utils.ExecuteShellCommand(deleteUerFromAcctCmd)
	if retcode2 != 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXECUTE_FAILED",
		}
		st := status.New(codes.Internal, "Shell command execute falied!")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	return &pb.RemoveUserFromAccountResponse{}, nil
}

func (s *ServerUser) BlockUserInAccount(ctx context.Context, in *pb.BlockUserInAccountRequest) (*pb.BlockUserInAccountResponse, error) {
	var (
		acctName string
		userName string
		user     string
	)
	caller.Logger.Infof("Received request BlockUserInAccount: %v", in)
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	// 检查账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 检查账户与用户是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)

	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s and %s assocation is not exists!", in.UserId, in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 关联存在的情况下直接封锁账户
	blockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name=%s account=%s set MaxSubmitJobs=0 MaxJobs=0 GrpJobs=0 GrpSubmit=0 GrpSubmitJobs=0 MaxSubmitJobs=0", in.UserId, in.AccountName)
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

func (s *ServerUser) UnblockUserInAccount(ctx context.Context, in *pb.UnblockUserInAccountRequest) (*pb.UnblockUserInAccountResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)
	caller.Logger.Infof("Received request UnblockUserInAccount: %v", in)
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
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
		mesaage := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, mesaage)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	//  检查用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账户与用户是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s and %s assocation is not exists!", in.UserId, in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 最大提交作业数为NULL表示没被封锁
	maxSubmitJobsSqlConfig := fmt.Sprintf("SELECT DISTINCT max_submit_jobs FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(maxSubmitJobsSqlConfig, in.UserId, in.AccountName).Scan(&maxSubmitJobs)
	if err != nil {
		return &pb.UnblockUserInAccountResponse{}, nil
	}
	// 用户从账户中解封的操作
	unblockUserCmd := fmt.Sprintf("sacctmgr -i -Q modify user where name='%s' account='%s' set MaxSubmitJobs=-1 MaxJobs=-1 GrpJobs=-1 GrpSubmit=-1 GrpSubmitJobs=-1 MaxSubmitJobs=-1", in.UserId, in.AccountName)
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

func (s *ServerUser) QueryUserInAccountBlockStatus(ctx context.Context, in *pb.QueryUserInAccountBlockStatusRequest) (*pb.QueryUserInAccountBlockStatusResponse, error) {
	var (
		acctName      string
		userName      string
		user          string
		maxSubmitJobs int
	)
	// 记录日志
	caller.Logger.Infof("Received request QueryUserInAccountBlockStatus: %v", in)
	// 检查账户名、用户名是否包含大写字母
	resultAcct := utils.CheckAccountOrUserStrings(in.AccountName)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	// 判断账户是否在slurm中
	acctSqlConfig := "SELECT name FROM acct_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(acctSqlConfig, in.AccountName).Scan(&acctName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 判断用户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err = caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 检查账户与用户在slurm中是否存在关联关系
	assocSqlConfig := fmt.Sprintf("SELECT DISTINCT user FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(assocSqlConfig, in.UserId, in.AccountName).Scan(&user)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_ACCOUNT_NOT_FOUND",
		}
		message := fmt.Sprintf("%s and %s assocation is not exists!", in.UserId, in.AccountName)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 查询max_submit_jobs的值,通过max_submit_jobs来判断用户是否被封锁
	maxSubmitJobSqlConfig := fmt.Sprintf("SELECT DISTINCT max_submit_jobs FROM %s_assoc_table WHERE user = ? AND acct = ? AND deleted = 0", clusterName)
	err = caller.DB.QueryRow(maxSubmitJobSqlConfig, in.UserId, in.AccountName).Scan(&maxSubmitJobs)
	if err != nil {
		return &pb.QueryUserInAccountBlockStatusResponse{Blocked: false}, nil
	}
	return &pb.QueryUserInAccountBlockStatusResponse{Blocked: true}, nil
}

func (s *ServerUser) DeleteUser(ctx context.Context, in *pb.DeleteUserRequest) (*pb.DeleteUserResponse, error) {
	// 检查用户是不是存在
	var (
		userName string
	)
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	// 作业的判断
	userRunningJobInfoCmd := fmt.Sprintf("squeue --noheader -A %s", in.UserId)
	runningJobInfo, err := utils.RunCommand(userRunningJobInfoCmd)

	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "CMD_EXECUTE_FAILED",
		}
		st := status.New(codes.NotFound, err.Error())
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}

	if len(runningJobInfo) == 0 {
		deleteUserCmd := fmt.Sprintf("sacctmgr -i delete user name=%s", in.UserId)
		_, err = utils.RunCommand(deleteUserCmd)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "CMD_EXECUTE_FAILED",
			}
			st := status.New(codes.NotFound, err.Error())
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		// 执行成功直接返回
		return &pb.DeleteUserResponse{}, nil
	} else {
		// 不能删
		errInfo := &errdetails.ErrorInfo{
			Reason: "HAVE_RUNNING_JOBS",
		}
		st := status.New(codes.NotFound, "Exist running jobs.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
}
