package config

import (
	"context"
	"fmt"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strconv"
	"strings"
	"sync"

	"github.com/wxnacy/wgo/arrays"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerConfig struct {
	pb.UnimplementedConfigServiceServer
}

func (s *ServerConfig) GetClusterConfig(ctx context.Context, in *pb.GetClusterConfigRequest) (*pb.GetClusterConfigResponse, error) {
	var (
		parts           []*pb.Partition
		qosName         string
		qosList         []string
		totalCpuInt     int
		totalMemInt     int
		totalNodeNumInt int
	)
	// 记录日志
	caller.Logger.Infof("Received request GetClusterConfig: %v", in)
	// 获取系统计算分区信息
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 查系统中的所有qos
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
	// fmt.Println(caller.ConfigValue.PartitionDesc, 112222)
	// for _, value := range caller.ConfigValue.PartitionDesc {
	// 	fmt.Println(value.Desc, value.Name)
	// }
	for _, partition := range partitions {
		var (
			totalGpus    uint32
			comment      string
			qos          []string
			totalMems    int
			totalCpus    string
			totalMemsTmp string
			totalNodes   string
		)

		getPartitionInfoCmd := fmt.Sprintf("scontrol show partition=%s | grep -i mem=", partition)
		output, err := utils.RunCommand(getPartitionInfoCmd)
		// 不同slurm版本的问题
		if err == nil && !utils.CheckSlurmStatus(output) {
			configArray := strings.Split(output, ",")

			if len(configArray) >= 4 {
				totalCpusCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $3}'", configArray[0])
				totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}'", configArray[1])
				totalNodesCmd := fmt.Sprintf("echo %s | awk  -F'=' '{print $2}'", configArray[2])

				totalCpus, err = utils.RunCommand(totalCpusCmd)
				if err != nil || utils.CheckSlurmStatus(totalCpus) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				totalMemsTmp, err = utils.RunCommand(totalMemsCmd)
				if err != nil || utils.CheckSlurmStatus(totalMemsTmp) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				totalNodes, err = utils.RunCommand(totalNodesCmd)
				if err != nil || utils.CheckSlurmStatus(totalNodes) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}

			} else if len(configArray) < 4 {
				totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}'", configArray[1])
				totalCpusCmd := "scontrol show part | grep TotalCPUs | awk '{print $2}' | awk -F'=' '{print $2}'"
				totalNodesCmd := "scontrol show part | grep TotalNodes | awk '{print $3}' | awk -F'=' '{print $2}'"

				totalCpus, err = utils.RunCommand(totalCpusCmd)
				if err != nil || utils.CheckSlurmStatus(totalCpus) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				totalMemsTmp, err = utils.RunCommand(totalMemsCmd)
				if err != nil || utils.CheckSlurmStatus(totalMemsTmp) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				totalNodes, err = utils.RunCommand(totalNodesCmd)
				if err != nil || utils.CheckSlurmStatus(totalNodes) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
			}

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

			// 将字符串转换为int
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			totalMemInt = totalMems
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)
		} else if err != nil && !utils.CheckSlurmStatus(output) {
			// 获取总cpu、总内存、总节点数
			getPartitionTotalCpusCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalCPUs | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
			totalCpus, err := utils.RunCommand(getPartitionTotalCpusCmd)
			if err != nil || utils.CheckSlurmStatus(totalCpus) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld dwon.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			getPartitionTotalNodesCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalNodes | awk '{print $3}' | awk -F'=' '{print $2}'", partition)
			totalNodes, err := utils.RunCommand(getPartitionTotalNodesCmd)
			if err != nil || utils.CheckSlurmStatus(totalNodes) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)

			// 取节点名，默认取第一个元素，在判断有没有[特殊符合
			getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
			nodeOutput, err := utils.RunCommand(getPartitionNodeNameCmd)
			if err != nil || utils.CheckSlurmStatus(nodeOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			nodeArray := strings.Split(nodeOutput, ",")
			res := strings.Contains(nodeArray[0], "[")
			if res {
				getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
				nodeNameOutput, err := utils.RunCommand(getNodeNameCmd)
				if err != nil || utils.CheckSlurmStatus(nodeNameOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
				getMemCmd := fmt.Sprintf("scontrol show node=%s | grep  RealMemory=| awk '{print $1}' | awk -F'=' '{print $2}'", nodeName)
				memOutput, err := utils.RunCommand(getMemCmd)
				if err != nil || utils.CheckSlurmStatus(memOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}

				nodeMem, _ := strconv.Atoi(memOutput)
				totalMemInt = nodeMem * totalNodeNumInt
			} else {
				getMemCmd := fmt.Sprintf("scontrol show node=%s | grep RealMemory=| awk '{print $1}' | awk -F'=' '{print $2}'", nodeArray[0])
				memOutput, err := utils.RunCommand(getMemCmd)
				if err != nil || utils.CheckSlurmStatus(memOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}

				nodeMem, _ := strconv.Atoi(memOutput)
				totalMemInt = nodeMem * totalNodeNumInt
			}
		} else {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}

		// 取节点名，默认取第一个元素，在判断有没有[特殊符合
		getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
		nodeOutput, err := utils.RunCommand(getPartitionNodeNameCmd)
		if err != nil || utils.CheckSlurmStatus(nodeOutput) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		nodeArray := strings.Split(nodeOutput, ",")

		res := strings.Contains(nodeArray[0], "[")
		if res {
			getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
			nodeNameOutput, err := utils.RunCommand(getNodeNameCmd)
			if err != nil || utils.CheckSlurmStatus(nodeNameOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
			gpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeName)
			gpusOutput, err := utils.RunCommand(gpusCmd)
			if err != nil || utils.CheckSlurmStatus(gpusOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			if gpusOutput == "Gres=(null)" {
				totalGpus = 0
			} else {
				// 字符串转整型
				perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
				totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
			}
		} else {
			getGpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeArray[0])
			gpusOutput, err := utils.RunCommand(getGpusCmd)
			if err != nil || utils.CheckSlurmStatus(gpusOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			if gpusOutput == "Gres=(null)" {
				totalGpus = 0
			} else {
				perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
				totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
			}
		}
		getPartitionQosCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' QoS=' | awk '{print $3}'", partition)
		qosOutput, err := utils.RunCommand(getPartitionQosCmd)
		if err != nil || utils.CheckSlurmStatus(qosOutput) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		qosArray := strings.Split(qosOutput, "=")

		// 获取AllowQos
		getPartitionAllowQosCmd := fmt.Sprintf("scontrol show partition=%s | grep AllowQos | awk '{print $3}'| awk -F'=' '{print $2}'", partition)
		// 返回的是字符串
		allowQosOutput, err := utils.RunCommand(getPartitionAllowQosCmd)
		if err != nil || utils.CheckSlurmStatus(allowQosOutput) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}

		if qosArray[len(qosArray)-1] != "N/A" {
			qos = append(qos, qosArray[len(qosArray)-1])
		} else {
			if allowQosOutput == "ALL" {
				qos = qosList
			} else {
				qos = strings.Split(allowQosOutput, ",")
			}
		}
		// 从配置文件中读取计算分区的描述字段
		for _, value := range caller.ConfigValue.PartitionDesc {
			if value.Name == partition {
				comment = value.Desc
				break
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
	return &pb.GetClusterConfigResponse{Partitions: parts, SchedulerName: "slurm"}, nil
}

func (s *ServerConfig) GetAvailablePartitions(ctx context.Context, in *pb.GetAvailablePartitionsRequest) (*pb.GetAvailablePartitionsResponse, error) {
	var (
		parts           []*pb.Partition
		userName        string
		user            string
		acctName        string
		qosName         string
		qosList         []string
		totalCpuInt     int
		totalMemInt     int
		totalNodeNumInt int
	)
	caller.Logger.Infof("Received request GetAvailablePartitions: %v", in)
	// 检查用户名中是否包含大写字母
	resultUser := utils.CheckAccountOrUserStrings(in.UserId)
	if !resultUser {
		errInfo := &errdetails.ErrorInfo{
			Reason: "USER_CONTAIN_ILLEGAL_CHARACTERS",
		}
		st := status.New(codes.Internal, "The username contains illegal characters.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	// 获取集群名
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName

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
		return nil, st.Err()
	}
	// 判断用户是否存在
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
	// 查系统中的所有qos
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
	// 关联关系存在的情况下去找用户
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	for _, partition := range partitions {
		var (
			totalMems int
			totalGpus uint32
			comment   string
			qos       []string
		)
		getPartitionAllowAccountsCmd := fmt.Sprintf("scontrol show part=%s | grep -i AllowAccounts | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
		accouts, err := utils.RunCommand(getPartitionAllowAccountsCmd) // 这个地方需要改一下，变成数组进行判断
		if err != nil || utils.CheckSlurmStatus(accouts) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		index := arrays.Contains(strings.Split(accouts, ","), in.AccountName)
		if accouts == "ALL" || index != -1 {
			getPartitionTotalCpusCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalCPUs | awk '{print $2}' | awk -F'=' '{print $2}'", partition)
			totalCpus, err := utils.RunCommand(getPartitionTotalCpusCmd)
			if err != nil || utils.CheckSlurmStatus(totalCpus) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			totalCpuInt, _ = strconv.Atoi(totalCpus)
			getPartitionTotalNodesCmd := fmt.Sprintf("scontrol show partition=%s | grep TotalNodes | awk '{print $3}' | awk -F'=' '{print $2}'", partition)
			totalNodes, err := utils.RunCommand(getPartitionTotalNodesCmd)
			if err != nil || utils.CheckSlurmStatus(totalNodes) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			totalNodeNumInt, _ = strconv.Atoi(totalNodes)

			getPartitionInfoCmd := fmt.Sprintf("scontrol show partition=%s | grep -i mem=", partition)
			output, _ := utils.RunCommand(getPartitionInfoCmd)
			if output != "" && utils.CheckSlurmStatus(output) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			if output != "" {
				configArray := strings.Split(output, ",")
				totalMemsCmd := fmt.Sprintf("echo %s | awk -F'=' '{print $2}'", configArray[1])
				totalMemsTmp, err := utils.RunCommand(totalMemsCmd)
				if err != nil || utils.CheckSlurmStatus(totalMemsTmp) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}

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
				nodeOutput, err := utils.RunCommand(getPartitionNodeNameCmd)
				if err != nil || utils.CheckSlurmStatus(nodeOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				nodeArray := strings.Split(nodeOutput, ",")
				res := strings.Contains(nodeArray[0], "[")
				if res {
					getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
					nodeNameOutput, err := utils.RunCommand(getNodeNameCmd)
					if err != nil || utils.CheckSlurmStatus(nodeNameOutput) {
						errInfo := &errdetails.ErrorInfo{
							Reason: "COMMAND_EXEC_FAILED",
						}
						st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
						st, _ = st.WithDetails(errInfo)
						return nil, st.Err()
					}
					nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
					getMemCmd := fmt.Sprintf("scontrol show node=%s | grep mem= | awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeName)
					memOutput, err := utils.RunCommand(getMemCmd)
					if err != nil || utils.CheckSlurmStatus(memOutput) {
						errInfo := &errdetails.ErrorInfo{
							Reason: "COMMAND_EXEC_FAILED",
						}
						st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
						st, _ = st.WithDetails(errInfo)
						return nil, st.Err()
					}
					nodeMem, _ := strconv.Atoi(memOutput)
					totalMemInt = nodeMem * totalNodeNumInt
				} else {
					getMemCmd := fmt.Sprintf("scontrol show node=%s | grep mem= | awk -F',' '{print $2}' | awk -F'=' '{print $2}'| awk -F'M' '{print $1}'", nodeArray[0])
					memOutput, err := utils.RunCommand(getMemCmd)
					if err != nil || utils.CheckSlurmStatus(memOutput) {
						errInfo := &errdetails.ErrorInfo{
							Reason: "COMMAND_EXEC_FAILED",
						}
						st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
						st, _ = st.WithDetails(errInfo)
						return nil, st.Err()
					}
					nodeMem, _ := strconv.Atoi(memOutput)
					totalMemInt = nodeMem * totalNodeNumInt
				}
			}
			// 取节点名，默认取第一个元素，在判断有没有[特殊符合
			getPartitionNodeNameCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' Nodes=' | awk -F'=' '{print $2}'", partition)
			nodeOutput, err := utils.RunCommand(getPartitionNodeNameCmd)
			if err != nil || utils.CheckSlurmStatus(nodeOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			nodeArray := strings.Split(nodeOutput, ",")

			res := strings.Contains(nodeArray[0], "[")
			if res {
				getNodeNameCmd := fmt.Sprintf("echo %s | awk -F'[' '{print $1,$2}' | awk -F'-' '{print $1}'", nodeArray[0])
				nodeNameOutput, err := utils.RunCommand(getNodeNameCmd)
				if err != nil || utils.CheckSlurmStatus(nodeNameOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				nodeName := strings.Join(strings.Split(nodeNameOutput, " "), "")
				gpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeName)
				gpusOutput, err := utils.RunCommand(gpusCmd)
				if err != nil || utils.CheckSlurmStatus(gpusOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				if gpusOutput == "Gres=(null)" {
					totalGpus = 0
				} else {
					// 字符串转整型
					perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
					totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
				}
			} else {
				getGpusCmd := fmt.Sprintf("scontrol show node=%s| grep ' Gres=' | awk -F':' '{print $NF}'", nodeArray[0])
				gpusOutput, err := utils.RunCommand(getGpusCmd)
				if err != nil || utils.CheckSlurmStatus(gpusOutput) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				if gpusOutput == "Gres=(null)" {
					totalGpus = 0
				} else {
					perNodeGpuNum, _ := strconv.Atoi(gpusOutput)
					totalGpus = uint32(perNodeGpuNum) * uint32(totalNodeNumInt)
				}
			}
			getPartitionQosCmd := fmt.Sprintf("scontrol show partition=%s | grep -i ' QoS=' | awk '{print $3}'", partition)
			qosOutput, err := utils.RunCommand(getPartitionQosCmd)
			if err != nil || utils.CheckSlurmStatus(qosOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			qosArray := strings.Split(qosOutput, "=")

			// 获取AllowQos
			getPartitionAllowQosCmd := fmt.Sprintf("scontrol show partition=%s | grep AllowQos | awk '{print $3}'| awk -F'=' '{print $2}'", partition)
			// 返回的是字符串
			allowQosOutput, err := utils.RunCommand(getPartitionAllowQosCmd)
			if err != nil || utils.CheckSlurmStatus(allowQosOutput) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}

			if qosArray[len(qosArray)-1] != "N/A" {
				qos = append(qos, qosArray[len(qosArray)-1])
			} else {
				if allowQosOutput == "ALL" {
					qos = qosList
				} else {
					qos = strings.Split(allowQosOutput, ",")
				}
			}
			// 加一个comment的描述
			for _, value := range caller.ConfigValue.PartitionDesc {
				if value.Name == partition {
					comment = value.Desc
					break
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

func extractNodeInfo(info string) (*pb.NodeInfo, error) {
	var (
		partitionList []string
		totalGpusInt  int
		allocGpusInt  int
		nodeState     pb.NodeInfo_NodeState
	)

	nodeName := utils.ExtractValue(info, "NodeName")
	partitions := utils.ExtractValue(info, "Partitions")
	partitionList = append(partitionList, strings.Split(partitions, ",")...)
	state := utils.ExtractValue(info, "State") // 这个地方要改
	switch state {
	case "IDLE", "IDLE+PLANNED":
		nodeState = pb.NodeInfo_IDLE
	case "DOWN", "DOWN+NOT_RESPONDING", "ALLOCATED+DRAIN", "IDLE+DRAIN", "IDLE+DRAIN+NOT_RESPONDING", "DOWN+DRAIN+INVALID_REG", "IDLE+NOT_RESPONDING":
		nodeState = pb.NodeInfo_NOT_AVAILABLE
	case "ALLOCATED", "MIXED":
		nodeState = pb.NodeInfo_RUNNING
	default: // 其他不知道的状态默认为不可用的状态
		nodeState = pb.NodeInfo_NOT_AVAILABLE
	}
	totalMem := utils.ExtractValue(info, "RealMemory")
	totalMemInt, _ := strconv.Atoi(totalMem)
	AllocMem := utils.ExtractValue(info, "AllocMem")
	AllocMemInt, _ := strconv.Atoi(AllocMem)
	totalCpuCores := utils.ExtractValue(info, "CPUTot")
	totalCpuCoresInt, _ := strconv.Atoi(totalCpuCores)
	allocCpuCores := utils.ExtractValue(info, "CPUAlloc")
	allocCpuCoresInt, _ := strconv.Atoi(allocCpuCores)
	totalGpus := utils.ExtractValue(info, "Gres")
	if totalGpus == "(null)" {
		totalGpusInt = 0
	} else {
		totalGpusStr := strings.Split(totalGpus, ":")[1]
		totalGpusInt, _ = strconv.Atoi(totalGpusStr)
	}
	allocGpus := utils.ExtractValue(info, "AllocTRES")
	if allocGpus == "" {
		allocGpusInt = 0
	} else {
		if strings.Contains(allocGpus, "gpu") {
			allocRes := strings.Split(allocGpus, ",")
			for _, res := range allocRes {
				if strings.Contains(res, "gpu") {
					gpuAllocResStr := strings.Split(res, "=")[1]
					allocGpusInt, _ = strconv.Atoi(gpuAllocResStr)
					break
				}
			}
		} else {
			allocGpusInt = 0
		}
	}

	return &pb.NodeInfo{
		NodeName:          nodeName,
		Partitions:        partitionList,
		State:             nodeState,
		CpuCoreCount:      uint32(totalCpuCoresInt),
		AllocCpuCoreCount: uint32(allocCpuCoresInt),
		IdleCpuCoreCount:  uint32(totalCpuCoresInt) - uint32(allocCpuCoresInt),
		TotalMemMb:        uint32(totalMemInt),
		AllocMemMb:        uint32(AllocMemInt),
		IdleMemMb:         uint32(totalMemInt) - uint32(AllocMemInt),
		GpuCount:          uint32(totalGpusInt),
		AllocGpuCount:     uint32(allocGpusInt),
		IdleGpuCount:      uint32(totalGpusInt) - uint32(allocGpusInt),
	}, nil
}

func getNodeInfo(node string, wg *sync.WaitGroup, nodeChan chan<- *pb.NodeInfo, errChan chan<- error) {
	defer wg.Done()

	getNodeInfoCmd := fmt.Sprintf("scontrol show nodes %s --oneliner", node)
	info, err := utils.RunCommand(getNodeInfoCmd)
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		errChan <- st.Err()
		return
	}

	nodeInfo, err := extractNodeInfo(info)
	if err != nil {
		errChan <- err
		return
	}

	nodeChan <- nodeInfo
}

func (s *ServerConfig) GetClusterNodesInfo(ctx context.Context, in *pb.GetClusterNodesInfoRequest) (*pb.GetClusterNodesInfoResponse, error) {
	var (
		wg            sync.WaitGroup
		nodesInfo     []*pb.NodeInfo
		nodesInfoList []string
	)
	caller.Logger.Infof("Received request GetClusterNodesInfo: %v", in)
	nodeChan := make(chan *pb.NodeInfo, len(in.NodeNames))
	errChan := make(chan error, len(in.NodeNames))

	if len(in.NodeNames) == 0 {
		// 获取集群中全部节点的信息
		getNodesInfoCmd := "scontrol show nodes --oneliner | grep Partitions | awk '{print $1}' | awk -F= '{print $2}' | tr '\n' ';'" // 获取全部计算节点主机名
		output, err := utils.RunCommand(getNodesInfoCmd)
		if err != nil {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}
		nodesInfoList = strings.Split(output, ";")
		nodesInfoList = nodesInfoList[:len(nodesInfoList)-1]
	} else {
		nodesInfoList = in.NodeNames
	}

	for _, node := range nodesInfoList {
		node1 := node
		wg.Add(1)
		go func() {
			getNodeInfo(node1, &wg, chan<- *pb.NodeInfo(nodeChan), chan<- error(errChan))
		}()
	}

	go func() {
		wg.Wait()
		close(nodeChan)
		close(errChan)
	}()

	for nodeInfo := range nodeChan {
		nodesInfo = append(nodesInfo, nodeInfo)
	}

	select {
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	default:
	}
	return &pb.GetClusterNodesInfoResponse{Nodes: nodesInfo}, nil
}

func (s *ServerConfig) GetClusterInfo(ctx context.Context, in *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	var (
		parts []*pb.PartitionInfo
	)
	// 记录日志
	caller.Logger.Infof("Received request GetClusterInfo: %v", in)
	clusterName := caller.ConfigValue.MySQLConfig.ClusterName
	partitions, err := utils.GetPartitionInfo()
	if err != nil || len(partitions) == 0 {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or don't set partitions.")
		st, _ = st.WithDetails(errInfo)
		return nil, st.Err()
	}
	for _, v := range partitions {
		var (
			runningGpus      int
			idleGpus         int
			noAvailableGpus  int
			totalGpus        int
			totalCores       int
			idleCores        int
			runningCores     int
			noAvailableCores int
			pdJobNum         int
			runningJobNum    int
			state            string
			totalNodes       int
			runningNodes     int
			idleNodes        int
			noAvailableNodes int
		)
		getPartitionStatusCmd := fmt.Sprintf("sinfo -p %s --noheader", v)
		fullCmd := getPartitionStatusCmd + " --format='%P %c %C %G %a %D %F'| tr '\n' ','"
		result, err := utils.RunCommand(fullCmd) // 状态
		if err != nil || utils.CheckSlurmStatus(result) {
			errInfo := &errdetails.ErrorInfo{
				Reason: "COMMAND_EXEC_FAILED",
			}
			st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
			st, _ = st.WithDetails(errInfo)
			return nil, st.Err()
		}

		partitionElements := strings.Split(result, ",")
		for _, partitionElement := range partitionElements {
			// 移除可能存在的前导空格
			partitionElement = strings.TrimSpace(partitionElement)
			if partitionElement == "" {
				continue
			}
			resultList := strings.Split(partitionElement, " ")
			if len(resultList) < 7 {
				continue
			}

			state = resultList[4]
			nodeInfo := strings.Split(resultList[6], "/")
			runningNodesTmp, _ := strconv.Atoi(nodeInfo[0])
			runningNodes = runningNodes + runningNodesTmp
			idleNodesTmp, _ := strconv.Atoi(nodeInfo[1])
			idleNodes = idleNodes + idleNodesTmp
			noAvailableNodesTmp, _ := strconv.Atoi(nodeInfo[2])
			noAvailableNodes = noAvailableNodes + noAvailableNodesTmp
			totalNodesTmp, _ := strconv.Atoi(nodeInfo[3])
			totalNodes = totalNodesTmp + totalNodes
			// cores
			coresInfo := strings.Split(resultList[2], "/")
			totalCores, _ = strconv.Atoi(coresInfo[3])
			runningCoresTmp, _ := strconv.Atoi(coresInfo[0])
			runningCores = runningCores + runningCoresTmp
			idleCoresTmp, _ := strconv.Atoi(coresInfo[1])
			idleCores = idleCores + idleCoresTmp
			noAvailableCoresTmp, _ := strconv.Atoi(coresInfo[2])
			noAvailableCores = noAvailableCores + noAvailableCoresTmp
			gpuInfo := resultList[3] // 这是gpu的信息
			if gpuInfo == "(null)" {
				runningGpus = 0
				idleGpus = 0
				noAvailableGpus = 0
				totalGpus = 0
			} else {
				singerNodeGpusInfo := strings.Split(gpuInfo, ":")
				singerNodeGpus := singerNodeGpusInfo[len(singerNodeGpusInfo)-1] // 获取最后一个元素
				singerNodeGpusInt, _ := strconv.Atoi(singerNodeGpus)
				noAvailableGpus = noAvailableGpus + noAvailableNodes*singerNodeGpusInt
				totalGpus = totalGpus + singerNodeGpusInt*totalNodes
			}
		}

		if totalGpus == 0 {
			// 获取作业信息
			pdJobNumSqlCmd := fmt.Sprintf("squeue -p %s --noheader -t pd| wc -l", v)
			pdresult, err := utils.RunCommand(pdJobNumSqlCmd)
			if err != nil || utils.CheckSlurmStatus(pdresult) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			pdJobNum, _ = strconv.Atoi(pdresult)
			runningJobNumSqlCmd := fmt.Sprintf("squeue -p %s --noheader -t r| wc -l", v)
			runningresult, err := utils.RunCommand(runningJobNumSqlCmd)
			if err != nil || utils.CheckSlurmStatus(runningresult) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			runningJobNum, _ = strconv.Atoi(runningresult)
			resultRatio := float64(runningNodes) / float64(totalNodes)
			percentage := int(resultRatio * 100) // 保留整数
			if state == "up" {
				parts = append(parts, &pb.PartitionInfo{
					PartitionName:         v,
					NodeCount:             uint32(totalNodes),
					RunningNodeCount:      uint32(runningNodes),
					IdleNodeCount:         uint32(idleNodes),
					NotAvailableNodeCount: uint32(noAvailableNodes),
					CpuCoreCount:          uint32(totalCores),
					RunningCpuCount:       uint32(runningCores),
					IdleCpuCount:          uint32(idleCores),
					NotAvailableCpuCount:  uint32(noAvailableCores),
					GpuCoreCount:          uint32(totalGpus),
					RunningGpuCount:       uint32(runningGpus),
					IdleGpuCount:          uint32(idleGpus),
					NotAvailableGpuCount:  uint32(noAvailableGpus),
					JobCount:              uint32(pdJobNum) + uint32(runningJobNum),
					RunningJobCount:       uint32(runningJobNum),
					PendingJobCount:       uint32(pdJobNum),
					UsageRatePercentage:   uint32(percentage),
					PartitionStatus:       pb.PartitionInfo_AVAILABLE,
				})
			} else {
				parts = append(parts, &pb.PartitionInfo{
					PartitionName:         v,
					NodeCount:             uint32(totalNodes),
					RunningNodeCount:      uint32(runningNodes),
					IdleNodeCount:         uint32(idleNodes),
					NotAvailableNodeCount: uint32(noAvailableNodes),
					CpuCoreCount:          uint32(totalCores),
					RunningCpuCount:       uint32(runningCores),
					IdleCpuCount:          uint32(idleCores),
					NotAvailableCpuCount:  uint32(noAvailableCores),
					GpuCoreCount:          uint32(totalGpus),
					RunningGpuCount:       uint32(runningGpus),
					IdleGpuCount:          uint32(idleGpus),
					NotAvailableGpuCount:  uint32(noAvailableGpus),
					JobCount:              uint32(pdJobNum) + uint32(runningJobNum),
					RunningJobCount:       uint32(runningJobNum),
					PendingJobCount:       uint32(pdJobNum),
					UsageRatePercentage:   uint32(percentage),
					PartitionStatus:       pb.PartitionInfo_NOT_AVAILABLE,
				})
			}
		} else {
			// 排队作业统计
			pdJobNumSqlCmd := fmt.Sprintf("squeue -p %s --noheader -t pd| wc -l", v)
			pdresult, err := utils.RunCommand(pdJobNumSqlCmd)
			if err != nil || utils.CheckSlurmStatus(pdresult) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			pdJobNum, _ = strconv.Atoi(pdresult)
			runningJobNumCmd := fmt.Sprintf("squeue -p %s --noheader -t r| wc -l", v)
			runningResult, err := utils.RunCommand(runningJobNumCmd)
			if err != nil || utils.CheckSlurmStatus(runningResult) {
				errInfo := &errdetails.ErrorInfo{
					Reason: "COMMAND_EXEC_FAILED",
				}
				st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
				st, _ = st.WithDetails(errInfo)
				return nil, st.Err()
			}
			runningJobNum, _ = strconv.Atoi(runningResult)
			if runningJobNum != 0 {
				// 获取正在使用的GPU卡数
				useGpuCardstr := fmt.Sprintf("squeue -p %s -t r ", v)
				useGpuCardCmd := useGpuCardstr + " " + " --format='%b' --noheader | awk -F':' '{print $3}' | awk '{sum+=$1} END {print sum}'"
				useGpuCardResult, err := utils.RunCommand(useGpuCardCmd)
				if err != nil || utils.CheckSlurmStatus(useGpuCardResult) {
					errInfo := &errdetails.ErrorInfo{
						Reason: "COMMAND_EXEC_FAILED",
					}
					st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
					st, _ = st.WithDetails(errInfo)
					return nil, st.Err()
				}
				runningGpus, _ = strconv.Atoi(useGpuCardResult)
				idleGpus = totalGpus - runningGpus - noAvailableGpus
			} else {
				runningGpus = 0
				idleGpus = totalGpus
			}
			resultRatio := float64(runningNodes) / float64(totalNodes)
			percentage := int(resultRatio * 100) // 保留整数
			if state == "up" {
				parts = append(parts, &pb.PartitionInfo{
					PartitionName:         v,
					NodeCount:             uint32(totalNodes),
					RunningNodeCount:      uint32(runningNodes),
					IdleNodeCount:         uint32(idleNodes),
					NotAvailableNodeCount: uint32(noAvailableNodes),
					CpuCoreCount:          uint32(totalCores),
					RunningCpuCount:       uint32(runningCores),
					IdleCpuCount:          uint32(idleCores),
					NotAvailableCpuCount:  uint32(noAvailableCores),
					GpuCoreCount:          uint32(totalGpus),
					RunningGpuCount:       uint32(runningGpus),
					IdleGpuCount:          uint32(idleGpus),
					NotAvailableGpuCount:  uint32(noAvailableGpus),
					JobCount:              uint32(pdJobNum) + uint32(runningJobNum),
					RunningJobCount:       uint32(runningJobNum),
					PendingJobCount:       uint32(pdJobNum),
					UsageRatePercentage:   uint32(percentage),
					PartitionStatus:       pb.PartitionInfo_AVAILABLE,
				})
			} else {
				parts = append(parts, &pb.PartitionInfo{
					PartitionName:         v,
					NodeCount:             uint32(totalNodes),
					RunningNodeCount:      uint32(runningNodes),
					IdleNodeCount:         uint32(idleNodes),
					NotAvailableNodeCount: uint32(noAvailableNodes),
					CpuCoreCount:          uint32(totalCores),
					RunningCpuCount:       uint32(runningCores),
					IdleCpuCount:          uint32(idleCores),
					NotAvailableCpuCount:  uint32(noAvailableCores),
					GpuCoreCount:          uint32(totalGpus),
					RunningGpuCount:       uint32(runningGpus),
					IdleGpuCount:          uint32(idleGpus),
					NotAvailableGpuCount:  uint32(noAvailableGpus),
					JobCount:              uint32(pdJobNum) + uint32(runningJobNum),
					RunningJobCount:       uint32(runningJobNum),
					PendingJobCount:       uint32(pdJobNum),
					UsageRatePercentage:   uint32(percentage),
					PartitionStatus:       pb.PartitionInfo_NOT_AVAILABLE,
				})
			}
		}
	}
	return &pb.GetClusterInfoResponse{ClusterName: clusterName, Partitions: parts}, nil
}
