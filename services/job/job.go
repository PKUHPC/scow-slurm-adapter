package job

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ServerJob struct {
	pb.UnimplementedJobServiceServer
}

func (s *ServerJob) CancelJob(ctx context.Context, in *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	var (
		userName string
		// idJob    int
	)
	caller.Logger.Infof("Received request CancelJob: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("CancelJob failed: %v", st.Err())
		return nil, st.Err()
	}
	// 判断用户是否存在
	userSqlConfig := "SELECT name FROM user_table WHERE name = ? AND deleted = 0"
	err := caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&userName)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("CancelJob failed: %v", st.Err())
		return nil, st.Err()
	}
	// 从squeue来获取对应的作业信息
	getJobInfoCmd := fmt.Sprintf("squeue --noheader -j %d", in.JobId) // 直接从slurm的运行时中获取作业的信息
	_, err = utils.RunCommand(getJobInfoCmd)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("CancelJob failed: %v", st.Err())
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
		caller.Logger.Errorf("CancelJob failed: %v", st.Err())
		return nil, st.Err()
	}
	return &pb.CancelJobResponse{}, nil
}

func (s *ServerJob) QueryJobTimeLimit(ctx context.Context, in *pb.QueryJobTimeLimitRequest) (*pb.QueryJobTimeLimitResponse, error) {
	var (
		timeLimit uint64
	)
	caller.Logger.Infof("Received request QueryJobTimeLimit: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	// 通过jobId来查找作业信息
	jobSqlConfig := fmt.Sprintf("SELECT timelimit FROM %s_job_table WHERE id_job = ? AND state IN (0, 1, 2)", clusterName)
	err := caller.DB.QueryRow(jobSqlConfig, in.JobId).Scan(&timeLimit)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("QueryJobTimeLimit failed: %v", st.Err())
		return nil, st.Err()
	}
	return &pb.QueryJobTimeLimitResponse{TimeLimitMinutes: timeLimit}, nil
}

func (s *ServerJob) ChangeJobTimeLimit(ctx context.Context, in *pb.ChangeJobTimeLimitRequest) (*pb.ChangeJobTimeLimitResponse, error) {
	// 记录日志
	caller.Logger.Infof("Received request ChangeJobTimeLimit: %v", in)
	// 从slurm的运行时取作业的信息
	getJobInfoCmd := fmt.Sprintf("squeue --noheader -j %d", in.JobId) // 构造获取作业的命令行
	_, err := utils.RunCommand(getJobInfoCmd)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("ChangeJobTimeLimit failed: %v", st.Err())
		return nil, st.Err()
	}
	if in.DeltaMinutes >= 0 {
		updateTimeLimitCmd := fmt.Sprintf("scontrol update job=%d TimeLimit+=%d", in.JobId, in.DeltaMinutes)
		result, err := utils.RunCommand(updateTimeLimitCmd)
		if err != nil || utils.CheckSlurmStatus(result) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("ChangeJobTimeLimit failed: %v", st.Err())
			return nil, st.Err()
		}
	} else {
		minitues := int64(math.Abs(float64(in.DeltaMinutes)))
		updateTimeLimitCmd := fmt.Sprintf("scontrol update job=%d TimeLimit-=%d", in.JobId, minitues)
		result, err := utils.RunCommand(updateTimeLimitCmd)
		if err != nil || utils.CheckSlurmStatus(result) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("ChangeJobTimeLimit failed: %v", st.Err())
			return nil, st.Err()
		}
	}
	return &pb.ChangeJobTimeLimitResponse{}, nil
}

func (s *ServerJob) GetJobById(ctx context.Context, in *pb.GetJobByIdRequest) (*pb.GetJobByIdResponse, error) {
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
		fields           []string = in.Fields
	)
	caller.Logger.Infof("Received request GetJobById: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	// 根据jobid查询作业详细信息
	jobSqlConfig := fmt.Sprintf("SELECT account, id_user, cpus_req, job_name, id_job, id_qos, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, work_dir, tres_alloc, tres_req FROM %s_job_table WHERE id_job = ?", clusterName)
	err := caller.DB.QueryRow(jobSqlConfig, in.JobId).Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "JOB_NOT_FOUND",
		}
		st := status.New(codes.NotFound, "The job does not exist.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
		return nil, st.Err()
	}
	// 查询cputresId、memTresId、nodeTresId值
	cpuTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'cpu'"
	memTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'mem'"
	nodeTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'node'"
	caller.DB.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	caller.DB.QueryRow(memTresSqlConfig).Scan(&memTresId)
	caller.DB.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	stateString = utils.ChangeState(state)
	submitTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(submitTime, 0).Unix())}
	startTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(startTime, 0).Unix())}
	endTimeTimestamp := &timestamppb.Timestamp{Seconds: int64(time.Unix(endTime, 0).Unix())}

	// username 转换，需要从ldap中拿数据
	userName, _ := utils.GetUserNameByUid(idUser)

	// 查询qos的名字
	qosSqlConfig := "SELECT name FROM qos_table WHERE id = ?"
	caller.DB.QueryRow(qosSqlConfig, idQos).Scan(&qosName)

	// 查找SelectType插件的值
	slurmConfigCmd := "scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'"
	output, err := utils.RunCommand(slurmConfigCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
		return nil, st.Err()
	}

	// 查询gpu对应的id信息
	gpuSqlConfig := "SELECT id FROM tres_table WHERE type = 'gres' AND deleted = 0"
	rows, err := caller.DB.Query(gpuSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
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
			caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
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
		caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
		return nil, st.Err()
	}

	// 状态为排队和挂起的作业信息
	if state == 0 || state == 2 {
		getReasonCmd := fmt.Sprintf("scontrol show job=%d |grep 'Reason=' | awk '{print $2}'| awk -F'=' '{print $2}'", jobId)
		output, err := utils.RunCommand(getReasonCmd)
		if err != nil || utils.CheckSlurmStatus(output) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("Failed get job by id, error is: %v", st.Err())
			return nil, st.Err()
		}
		reason = output

		if state == 0 {
			cpusAlloc = 0
			memAllocMb = 0
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
			elapsedSeconds = 0
			gpusAlloc = 0
		} else {
			cpusAlloc = int32(utils.GetResInfoNumFromTresInfo(tresAlloc, cpuTresId))
			memAllocMb = int64(utils.GetResInfoNumFromTresInfo(tresAlloc, memTresId))
			nodeReq = int32(utils.GetResInfoNumFromTresInfo(tresReq, nodeTresId))
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

		elapsedSeconds = time.Now().Unix() - startTime
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
		caller.Logger.Infof("GetJobByIdResponse: %v", jobInfo)
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
		caller.Logger.Infof("GetJobByIdResponse: %v", jobInfo)
		return &pb.GetJobByIdResponse{Job: jobInfo}, nil
	}
}

func (s *ServerJob) GetJobs(ctx context.Context, in *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
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
		gpuIdList         []int
		uidList           []int
		stateIdList       []int
		jobInfo           []*pb.JobInfo
		params            []interface{}
		totalParams       []interface{}
		pendingMap        map[int]string
		pendingUserMap    map[int]string
		orderStr          string
		accountsString    string
		uidListString     string
		stateIdListString string
	)
	// 记录日志
	caller.Logger.Infof("Received request GetJobs: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	var fields []string = in.Fields

	var filterStates = in.Filter.States // 这个是筛选的
	var baseStates = []string{"RUNNING", "PENDING", "SUSPENDED"}
	var submitUser = in.Filter.Users
	var getJobInfoCmdLine string
	setBool := utils.IsSubSet(baseStates, filterStates)

	pendingUserCmdTemp := fmt.Sprintf("squeue -t pending -u %s", strings.Join(submitUser, ","))
	pendingUserCmd := pendingUserCmdTemp + " --noheader --format='%i=%R' | tr '\\n' ';'"
	pendingUserResult, err := utils.RunCommand(pendingUserCmd)
	if err != nil || utils.CheckSlurmStatus(pendingUserResult) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetJobs failed: %v", st.Err())
		return nil, st.Err()
	}
	if len(pendingUserResult) != 0 {
		pendingUserMap = utils.GetPendingMapInfo(pendingUserResult)
	}

	if setBool && len(filterStates) != 0 && len(submitUser) != 0 {
		if len(in.Filter.Accounts) == 0 {
			if in.Filter.JobId == nil && in.Filter.JobName == nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")))
			} else if in.Filter.JobId == nil && in.Filter.JobName != nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -n %s", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobName)
			} else if in.Filter.JobId != nil && in.Filter.JobName == nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -j %d", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobId)
			} else {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -n %s -j %d", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobName, *in.Filter.JobId)
			}
			// getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")))
		} else {
			if in.Filter.JobId == nil && in.Filter.JobName == nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")))
			} else if in.Filter.JobId == nil && in.Filter.JobName != nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -n %s", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobName)
			} else if in.Filter.JobId != nil && in.Filter.JobName == nil {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -j %d", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobId)
			} else {
				getJobInfoCmdLine = fmt.Sprintf("squeue -u %s --noheader -t %s -n %s -j %d", strings.Join(submitUser, ","), strings.ToLower(strings.Join(in.Filter.States, ",")), *in.Filter.JobName, *in.Filter.JobId)
			}
			// getJobInfoCmdLine = fmt.Sprintf("squeue -u %s -A %s --noheader -t %s", strings.Join(submitUser, ","), strings.Join(in.Filter.Accounts, ","), strings.ToLower(strings.Join(in.Filter.States, ",")))
		}
		getFullCmdLine := getJobInfoCmdLine + " " + "--format='%b %a %A %C %D %j %l %m %M %P %q %S %T %u %V %Z %N' | tr '\n' ';'"

		caller.Logger.Tracef("GetJobs get jobs command: %v", getFullCmdLine)
		runningjobInfo, err := utils.RunCommand(getFullCmdLine)
		if err != nil || utils.CheckSlurmStatus(runningjobInfo) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
			return nil, st.Err()
		}
		// runningJobInfoList := strings.Split(runningjobInfo, ",")
		runningJobInfoList := strings.Split(runningjobInfo, ";")
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
			var timeLimit int64
			var singerJobGpusAlloc int32
			var singerJobTimeSubmit *timestamppb.Timestamp
			var singerJobtimeLimitMinutes int64
			if len(strings.Split(v, " ")) == 17 {
				singerJobInfo := strings.Split(v, " ")
				singerJobAccount := singerJobInfo[1]
				singerJobUserName := singerJobInfo[13]
				singerJobJobId, _ := strconv.Atoi(singerJobInfo[2])
				singerJobState := singerJobInfo[12]
				singerJobJobPartition := singerJobInfo[9]
				singerJobJobName := singerJobInfo[5]
				singerJobQos := singerJobInfo[10]
				singerJobWorkingDirectory := singerJobInfo[15]
				if singerJobInfo[6] == "UNLIMITED" {
					singerJobtimeLimitMinutes = 0
				} else if singerJobInfo[6] == "INVALID" { //INVALID 要另起逻辑
					timeLimitSqlConfig := fmt.Sprintf("SELECT timelimit FROM %s_job_table WHERE id_job = ?", clusterName)
					caller.DB.QueryRow(timeLimitSqlConfig, singerJobJobId).Scan(&timeLimit)
					singerJobtimeLimitMinutes = timeLimit
				} else {
					singerJobtimeLimitMinutes = utils.GetTimeLimit(singerJobInfo[6])
				}
				submittimeSqlConfig := fmt.Sprintf("SELECT time_submit FROM %s_job_table WHERE id_job = ?", clusterName)
				caller.DB.QueryRow(submittimeSqlConfig, singerJobJobId).Scan(&timeSubmit)
				if timeSubmit == 0 {
					singerJobTimeSubmit = &timestamppb.Timestamp{Seconds: int64(time.Now().Unix())}
				} else {
					singerJobTimeSubmit = &timestamppb.Timestamp{Seconds: int64(time.Unix(timeSubmit, 0).Unix())}
				}
				if singerJobState == "PENDING" {
					singerJobJobNodesAlloc = 0
					// singerJobJobReason = singerJobInfo[17]
					if _, ok := pendingUserMap[singerJobJobId]; ok {
						// singerJobJobReason = pendingUserMap[singerJobJobId] // 这个reason的值需要更新配置
						reason := pendingUserMap[singerJobJobId] // 这个reason的值需要更新配置
						re := regexp.MustCompile(`Job's account not permitted to use this partition`)
						match := re.FindString(reason)
						if match != "" {
							singerJobJobReason = match
						} else {
							singerJobJobReason = reason
						}
					}
					singerJobCpusAlloc = 0
					singerJobElapsedSeconds = 0
					singerJobInfoNodeList = "None assigned"
				} else {
					singerJobJobNodesAllocTemp, _ := strconv.Atoi(singerJobInfo[4])
					singerJobJobNodesAlloc = int32(singerJobJobNodesAllocTemp)
					singerJobJobReason = singerJobInfo[12]
					singerJobCpusAllocTemp, _ := strconv.Atoi(singerJobInfo[3])
					singerJobCpusAlloc = int32(singerJobCpusAllocTemp)
					singerJobElapsedSeconds = utils.GetRunningElapsedSeconds(singerJobInfo[8])
					singerJobInfoNodeList = singerJobInfo[16]
				}

				// 新加代码，在正在运行中的作业添加gpu分配逻辑
				if singerJobInfo[0] == "N/A" {
					singerJobGpusAlloc = 0
				} else {
					perNodeGresAlloc := strings.Split(singerJobInfo[0], ":")
					// 求长度
					lastIndex := len(perNodeGresAlloc) - 1
					lastElement := perNodeGresAlloc[lastIndex]
					perNodeGpusNum, _ := strconv.Atoi(lastElement)
					// perNodeGpusNum, _ := strconv.Atoi(perNodeGresAlloc[2])
					singerJobGpusAlloc = int32(perNodeGpusNum) * singerJobJobNodesAlloc
				}

				jobInfo = append(jobInfo, &pb.JobInfo{
					JobId:            uint32(singerJobJobId),
					Name:             singerJobJobName,
					Account:          singerJobAccount,
					User:             singerJobUserName,
					Partition:        singerJobJobPartition,
					Qos:              singerJobQos,
					State:            singerJobState,
					TimeLimitMinutes: singerJobtimeLimitMinutes,
					WorkingDirectory: singerJobWorkingDirectory,
					Reason:           &singerJobJobReason,
					CpusAlloc:        &singerJobCpusAlloc,
					NodesAlloc:       &singerJobJobNodesAlloc,
					ElapsedSeconds:   &singerJobElapsedSeconds,
					NodeList:         &singerJobInfoNodeList,
					SubmitTime:       singerJobTimeSubmit,
					GpusAlloc:        &singerJobGpusAlloc,
				})
			} else {
				continue
			}
		}
		// 返回
		if len(jobInfo) == 0 {
			return &pb.GetJobsResponse{Jobs: jobInfo}, nil
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
		caller.Logger.Tracef("GetJobs GetJobsResponse is: %v", &pb.GetJobsResponse{Jobs: jobInfo})
		return &pb.GetJobsResponse{Jobs: jobInfo}, nil
	}

	// 查找SelectType插件的值
	slurmSelectTypeConfigCmd := "scontrol show config | grep 'SelectType ' | awk -F'=' '{print $2}' | awk -F'/' '{print $2}'"
	output, err := utils.RunCommand(slurmSelectTypeConfigCmd)
	if err != nil || utils.CheckSlurmStatus(output) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetJobs failed: %v", st.Err())
		return nil, st.Err()
	}

	// cputresId、memTresId、nodeTresId
	cpuTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'cpu'"
	memTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'mem'"
	nodeTresSqlConfig := "SELECT id FROM tres_table WHERE type = 'node'"
	caller.DB.QueryRow(cpuTresSqlConfig).Scan(&cpuTresId)
	caller.DB.QueryRow(memTresSqlConfig).Scan(&memTresId)
	caller.DB.QueryRow(nodeTresSqlConfig).Scan(&nodeTresId)

	gpuSqlConfig := "SELECT id FROM tres_table WHERE type = 'gres' AND deleted = 0"
	rowList, err := caller.DB.Query(gpuSqlConfig)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
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
			caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
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
		caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
		return nil, st.Err()
	}

	if in.Sort != nil {
		orderStr = fmt.Sprintf("ORDER BY job_db_inx %s", in.Sort.GetOrder().String())
	} else {
		orderStr = "ORDER BY job_db_inx ASC" // 默认就是升序排序
	}

	// 从这里开始改造
	if in.Filter != nil {
		// 有搜索条件的
		if in.Filter.EndTime != nil {
			startTimeFilter = in.Filter.EndTime.StartTime.GetSeconds()
			endTimeFilter = in.Filter.EndTime.EndTime.GetSeconds()
		}
		if in.Filter.SubmitTime != nil {
			submitStartTime = in.Filter.SubmitTime.StartTime.GetSeconds()
			submitEndTime = in.Filter.SubmitTime.EndTime.GetSeconds()
		}
		if in.Filter.Accounts != nil {
			accountsString = "'" + strings.Join(in.Filter.Accounts, "','") + "'"
		}
		if in.Filter.Users != nil { // 用户
			for _, user := range in.Filter.Users {
				uid, _, _ := utils.GetUserUidGid(user)
				uidList = append(uidList, uid)
			}
			uidListString = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(uidList)), ","), "[]")
		}
		if in.Filter.States != nil { // 状态
			for _, state := range in.Filter.States {
				stateId := utils.GetStateId(state)
				stateIdList = append(stateIdList, stateId)
			}
			stateIdListString = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(stateIdList)), ","), "[]")
		}

		baseSQL := fmt.Sprintf("SELECT account, id_user, cpus_req, job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, work_dir, tres_alloc, tres_req FROM %s_job_table WHERE ", clusterName)
		databaseEncode := caller.ConfigValue.MySQLConfig.DatabaseEncode
		caller.Logger.Tracef("Database encode is: %s", databaseEncode)
		// 正常情况下，数据库编码格式是latin1，环境部署时config.yaml中databaseEncode也会配成latin1。 但是为了防止config.yaml中databaseEncode配成utf8，查询时需要做这样的转换。
		if strings.Contains(databaseEncode, "utf8") {
			baseSQL = fmt.Sprintf("SELECT account, id_user, cpus_req, CONVERT(CAST(job_name AS BINARY) USING utf8) AS job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, CONVERT(CAST(work_dir AS BINARY) USING utf8) AS work_dir, tres_alloc, tres_req FROM %s_job_table WHERE ", clusterName)
		}

		conditions := []string{}

		if uidListString != "" {
			conditions = append(conditions, fmt.Sprintf("id_user IN (%s)", uidListString))
		}
		if stateIdListString != "" {
			conditions = append(conditions, fmt.Sprintf("state IN (%s)", stateIdListString))
		}
		if accountsString != "" {
			conditions = append(conditions, fmt.Sprintf("account IN (%s)", accountsString))
		}
		if startTimeFilter != 0 {
			conditions = append(conditions, "(time_end >= ? OR ? = 0)")
			params = append(params, startTimeFilter, startTimeFilter)
			totalParams = append(totalParams, startTimeFilter, startTimeFilter)
		}
		if endTimeFilter != 0 {
			conditions = append(conditions, "(time_end <= ? OR ? = 0)")
			params = append(params, endTimeFilter, endTimeFilter)
			totalParams = append(totalParams, endTimeFilter, endTimeFilter)
		}
		if submitStartTime != 0 {
			conditions = append(conditions, "(time_submit >= ? OR ? = 0)")
			params = append(params, submitStartTime, submitStartTime)
			totalParams = append(totalParams, submitStartTime, submitStartTime)
		}
		if submitEndTime != 0 {
			conditions = append(conditions, "(time_submit <= ? OR ? = 0)")
			params = append(params, submitEndTime, submitEndTime)
			totalParams = append(totalParams, submitEndTime, submitEndTime)
		}
		// 加了按作业名和作业id来搜索作业的逻辑
		if in.Filter.JobId != nil {
			conditions = append(conditions, "id_job = ?")
			params = append(params, *in.Filter.JobId)
			totalParams = append(totalParams, *in.Filter.JobId)
		}
		if in.Filter.JobName != nil {
			conditions = append(conditions, "CONVERT(CAST(job_name AS BINARY) USING utf8) = ?")
			params = append(params, *in.Filter.JobName)
			totalParams = append(totalParams, *in.Filter.JobName)
		}
		queryConditions := strings.Join(conditions, " AND ")
		if in.PageInfo != nil {
			// 分页的情况
			page := in.PageInfo.Page
			pageSize := in.PageInfo.PageSize
			pageLimit = int(pageSize)
			if page == 1 {
				pageSize = 0
			} else {
				pageSize = pageSize * uint64(page-1)
			}
			jobSqlConfig = fmt.Sprintf("%s %s %s LIMIT ? OFFSET ?", baseSQL, queryConditions, orderStr)
			params = append(params, pageLimit, pageSize)
			jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE %s", clusterName, queryConditions)
		} else {
			// 不分页的情况
			jobSqlConfig = fmt.Sprintf("%s %s %s", baseSQL, queryConditions, orderStr)
			jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table WHERE %s", clusterName, queryConditions)
		}
	} else {
		// 没有搜索条件
		baseSQL := fmt.Sprintf("SELECT account, id_user, cpus_req, job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, work_dir, tres_alloc, tres_req FROM %s_job_table ", clusterName)
		databaseEncode := caller.ConfigValue.MySQLConfig.DatabaseEncode
		caller.Logger.Tracef("Database encode is: %s", databaseEncode)
		// 正常情况下，数据库编码格式是latin1，环境部署时config.yaml中databaseEncode也会配成latin1。 但是为了防止config.yaml中databaseEncode配成utf8，查询时需要做这样的转换。
		if strings.Contains(databaseEncode, "utf8") {
			baseSQL = fmt.Sprintf("SELECT account, id_user, cpus_req, CONVERT(CAST(job_name AS BINARY) USING utf8) AS job_name, id_job, id_qos, mem_req, nodelist, nodes_alloc, `partition`, state, timelimit, time_submit, time_start, time_end, time_suspended, gres_used, CONVERT(CAST(work_dir AS BINARY) USING utf8) AS work_dir, tres_alloc, tres_req FROM %s_job_table WHERE ", clusterName)
		}

		if in.PageInfo != nil {
			// 分页的情况 没有搜索的情况
			page := in.PageInfo.Page
			pageSize := in.PageInfo.PageSize
			pageLimit = int(pageSize)
			if page == 1 {
				pageSize = 0
			} else {
				pageSize = pageSize * uint64(page-1)
			}
			jobSqlConfig = fmt.Sprintf("%s %s LIMIT ? OFFSET ?", baseSQL, orderStr)
			params = append(params, pageLimit, pageSize)
			jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table", clusterName) // 总的作业条数
		} else {
			// 不分页的情况 没有搜索的情况
			jobSqlConfig = fmt.Sprintf("%s %s", baseSQL, orderStr)
			jobSqlTotalConfig = fmt.Sprintf("SELECT count(*) FROM %s_job_table", clusterName) // 总的作业条数
		}
	}
	caller.Logger.Tracef("GetJobs sql: %v, params: %v", jobSqlConfig, params)
	rows, err := caller.DB.Query(jobSqlConfig, params...)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SQL_QUERY_FAILED",
		}
		st := status.New(codes.Internal, err.Error())
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
		return nil, st.Err()
	}
	defer rows.Close()

	pendingCmd := "squeue -t pending --noheader --format='%i %R' | tr '\n' ','"
	pendingResult, err := utils.RunCommand(pendingCmd)
	if err != nil || utils.CheckSlurmStatus(pendingResult) {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("GetJobs Failed: %v", st.Err())
		return nil, st.Err()
	}
	if len(pendingResult) != 0 {
		pendingMap = utils.GetMapInfo(pendingResult)
	}
	for rows.Next() {
		err := rows.Scan(&account, &idUser, &cpusReq, &jobName, &jobId, &idQos, &memReq, &nodeList, &nodesAlloc, &partition, &state, &timeLimitMinutes, &submitTime, &startTime, &endTime, &timeSuspended, &gresUsed, &workingDirectory, &tresAlloc, &tresReq)
		if err != nil {
			caller.Logger.Errorf("GetJobs Failed: %v ", err)
			continue
		}
		caller.Logger.Tracef("GetJobs account: %v, idUser: %v, cpusReq: %v, jobName: %v, jobId: %v, idQos: %v, memReq: %v, nodeList: %v, nodesAlloc: %v, partition: %v, state: %v, "+
			"timeLimitMinutes: %v, submitTime: %v, startTime: %v, endTime: %v, timeSuspended: %v, gresUsed: %v, workingDirectory: %v, tresAlloc: %v, tresReq: %v", account, idUser,
			cpusReq, jobName, jobId, idQos, memReq, nodeList, nodesAlloc, partition, state, timeLimitMinutes, submitTime, startTime, endTime, timeSuspended, gresUsed, workingDirectory, tresAlloc, tresReq)
		var (
			elapsedSeconds     int64
			reason             string
			stdoutPath         string
			stderrPath         string
			gpusAlloc          int32
			cpusAlloc          int32
			memAllocMb         int64
			nodeReq            int32
			nodesAllocTemp     int32
			nodeListTemp       string
			startTimeTimestamp *timestamppb.Timestamp
			endTimeTimestamp   *timestamppb.Timestamp
		)
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
		caller.DB.QueryRow(qosSqlconfig, idQos).Scan(&qosName)

		nodesAllocTemp = nodesAlloc
		nodeListTemp = nodeList
		if state == 0 || state == 2 {
			if _, ok := pendingMap[jobId]; ok {
				reason = pendingMap[jobId]
			} else {
				getReasonCmdTmp := fmt.Sprintf("squeue -j %d --noheader ", jobId)
				getReasonCmd := getReasonCmdTmp + " --format='%R'"
				reason, err = utils.RunCommand(getReasonCmd)

				re := regexp.MustCompile(`Job's account not permitted to use this partition`)
				match := re.FindString(reason)
				if match != "" {
					reason = match
				}

				if utils.CheckSlurmStatus(reason) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "SLURMCTLD_FAILED",
					}
					st := status.New(codes.Internal, "slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					caller.Logger.Errorf("GetJobs Failed: %v ", st.Err())
					return nil, st.Err()
				}

				if err != nil {
					logrus.Infof("Received err job info: %v", jobId)
					continue // 一般是数据库中有数据但是squeue中没有数据导致执行命令行失败
				}
			}

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
			// 新加逻辑
			getReasonCmdTmp := fmt.Sprintf("squeue -j %d --noheader ", jobId)
			getReasonCmd := getReasonCmdTmp + " --format='%R'"
			reason, err := utils.RunCommand(getReasonCmd)
			if utils.CheckSlurmStatus(reason) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "SLURMCTLD_FAILED",
				}
				st := status.New(codes.Internal, "slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				caller.Logger.Errorf("GetJobs failed: %v", st.Err())
				return nil, st.Err()
			}

			if err != nil {
				continue // 一般是数据库中有数据但是squeue中没有数据导致执行命令行失败
			}

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
		if memReq > 4000000000 {
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
		caller.Logger.Errorf("GetJobs failed: %v", st.Err())
		return nil, st.Err()
	}
	// 获取总的页数逻辑
	if jobSqlTotalConfig != "" {
		caller.DB.QueryRow(jobSqlTotalConfig, totalParams...).Scan(&count)
		totalCount = uint32(count)
		caller.Logger.Tracef("GetJobs GetJobsResponse is: %v", &pb.GetJobsResponse{Jobs: jobInfo, TotalCount: &totalCount})
		return &pb.GetJobsResponse{Jobs: jobInfo, TotalCount: &totalCount}, nil
	}
	caller.Logger.Tracef("GetJobs GetJobsResponse is: %v", &pb.GetJobsResponse{Jobs: jobInfo})
	return &pb.GetJobsResponse{Jobs: jobInfo}, nil
}

func (s *ServerJob) SubmitJob(ctx context.Context, in *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var (
		scriptString = "#!/bin/bash\n"
		name         string
		homedir      string
	)
	caller.Logger.Infof("Received request SubmitJob: %v", in)
	resultAcct := utils.CheckAccountOrUserStrings(in.Account)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultAcct || !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "ACCOUNT_USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The account or username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitJob failed: %v", st.Err())
		return nil, st.Err()
	}
	// 检查账户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE deleted = 0 AND name = ?"
	err := caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&name)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitJob failed: %v", st.Err())
		return nil, st.Err()
	}

	modulepath := caller.ConfigValue.Modulepath.Path
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

	modulepathString := fmt.Sprintf("source %s", modulepath) // 改成从配置文件中获取profile文件路径信息
	scriptString += "\n" + modulepathString + "\n"

	scriptString += in.Script
	// 提交作业

	submitResponse, err := utils.LocalSubmitJob(scriptString, in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SBATCH_FAILED",
		}
		st := status.New(codes.Unknown, submitResponse)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitJob failed: %v", st.Err())
		return nil, st.Err()
	}
	responseList := strings.Split(strings.TrimSpace(string(submitResponse)), " ")
	jobIdString := responseList[len(responseList)-1]
	jobId, _ := strconv.Atoi(jobIdString)
	caller.Logger.Infof("SubmitJobResponse: %v", &pb.SubmitJobResponse{JobId: uint32(jobId), GeneratedScript: scriptString})
	return &pb.SubmitJobResponse{JobId: uint32(jobId), GeneratedScript: scriptString}, nil
}

func (s *ServerJob) SubmitScriptAsJob(ctx context.Context, in *pb.SubmitScriptAsJobRequest) (*pb.SubmitScriptAsJobResponse, error) {
	var (
		name string
	)
	// 记录日志
	caller.Logger.Infof("Received request SubmitFileAsJob: %v", in)
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitScriptAsJob failed: %v", st.Err())
		return nil, st.Err()
	}
	// 检查账户是否在slurm中
	userSqlConfig := "SELECT name FROM user_table WHERE deleted = 0 AND name = ?"
	err := caller.DB.QueryRow(userSqlConfig, in.UserId).Scan(&name)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_NOT_FOUND",
		}
		message := fmt.Sprintf("%s does not exists.", in.UserId)
		st := status.New(codes.NotFound, message)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitScriptAsJob failed: %v", st.Err())
		return nil, st.Err()
	}

	// 具体的提交逻辑
	updateScript := "#!/bin/bash\n"
	trimmedScript := strings.TrimLeft(in.Script, "\n") // 去除最前面的空行
	// 通过换行符 "\n" 分割字符串
	checkBool1 := strings.Contains(trimmedScript, "--chdir")
	checkBool2 := strings.Contains(trimmedScript, " -D ")
	if !checkBool1 && !checkBool2 {
		if in.ScriptFileFullPath == nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "SCRIPT_FILE_FULL_PATH_NOT_SETTING",
			}
			st := status.New(codes.Unknown, "ScriptFileFullPath not setting")
			st, _ = st.WithDetails(errInfo)
			caller.Logger.Errorf("SubmitScriptAsJob failed: %v", st.Err())
			return nil, st.Err()
		}
		chdirString := fmt.Sprintf("#SBATCH --chdir=%s\n", *in.ScriptFileFullPath)
		updateScript = updateScript + chdirString
		for _, value := range strings.Split(trimmedScript, "\n")[1:] {
			updateScript = updateScript + value + "\n"
		}
		in.Script = updateScript
	}

	submitResponse, err := utils.LocalSubmitJob(in.Script, in.UserId)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "SBATCH_FAILED",
		}
		st := status.New(codes.Unknown, submitResponse)
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Errorf("SubmitScriptAsJob failed: %v", st.Err())
		return nil, st.Err()
	} else {
		// 这里还要获取jobid
		responseList := strings.Split(strings.TrimSpace(string(submitResponse)), " ")
		jobIdString := responseList[len(responseList)-1]
		jobId, _ := strconv.Atoi(jobIdString)
		caller.Logger.Infof("SubmitJobResponse: %v", &pb.SubmitScriptAsJobResponse{JobId: uint32(jobId)})
		return &pb.SubmitScriptAsJobResponse{JobId: uint32(jobId)}, nil
	}
}
