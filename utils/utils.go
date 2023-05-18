package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"os"
	"os/exec"
	"path"
	config "scow-slurm-adapter/config"
	"strings"
	"syscall"

	ldap "github.com/go-ldap/ldap/v3"
	"golang.org/x/crypto/ssh"
)

// type RichError struct {
// 	reason    string
// 	Message string
// }

// func (re *RichError) Error() string {
// 	return re.Message
// }

// func (re *RichError) GRPCStatus() *status.Status {
// 	return status.New(codes.NotFound, re.Message).WithDetails(&ErrorCode{Code: re.Code})
// }

// type ErrorCode struct {
// 	Code string `json:"code"`
// }

func ExecuteShellCommand(command string) int {
	var res int
	cmd := exec.Command("bash", "-c", command)
	stdout, _ := cmd.StdoutPipe()
	defer stdout.Close()
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	if err := cmd.Wait(); err != nil {
		if ex, ok := err.(*exec.ExitError); ok {
			res = ex.Sys().(syscall.WaitStatus).ExitStatus()
		}
	}
	return res
}

func RunCommand(command string) (string, error) {
	cmd := exec.Command("bash", "-c", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func DatabaseConfig() string {
	config := config.ParseConfig(config.DefaultConfigPath)
	host := config.MySQLConfig.Host
	userName := config.MySQLConfig.User
	passWord := config.MySQLConfig.Password
	dbName := config.MySQLConfig.DBName
	port := config.MySQLConfig.Port

	dbConfig := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, passWord, host, port, dbName, "latin1")
	return dbConfig
}

func GetPatitionInfo() ([]string, error) {
	shellCmd := fmt.Sprintf("cat /etc/slurm/slurm.conf | grep -i PartitionName | grep -v '#' | awk '{print $1}' | awk -F'=' '{print $2}'| tr '\n' ','")
	output, err := RunCommand(shellCmd)
	if err != nil {
		return nil, err
	}
	resOutput := strings.Split(output, ",")
	resOutput = resOutput[:len(resOutput)-1]
	return resOutput, nil
}

func DeleteSlice2(data []string, word string) []string {
	tmp := make([]string, 0, len(data))
	for _, v := range data {
		if v != word {
			tmp = append(tmp, v)
		}
	}
	return tmp
}

func ChangeState(stateInit int) string {
	var stateString string
	switch stateInit {
	case 0:
		stateString = "PENDING"
	case 1:
		stateString = "RUNNING"
	case 2:
		stateString = "SUSPEND"
	case 3:
		stateString = "COMPLETED"
	case 4:
		stateString = "CANCELED"
	case 5:
		stateString = "FAILED"
	case 6:
		stateString = "TIMEOUT"
	case 7:
		stateString = "NODE_FAIL"
	default:
		stateString = "COMPLETED"
	}
	return stateString
}

func GetStateId(stateString string) int {
	var state int
	switch stateString {
	case "PENDING":
		state = 0
	case "RUNNING":
		state = 1
	case "SUSPEND":
		state = 2
	case "COMPLETED":
		state = 3
	case "CANCELED":
		state = 4
	case "FAILED":
		state = 5
	case "TIMEOUT":
		state = 6
	case "NODE_FAIL":
		state = 7
	default:
		state = 3
	}
	return state
}

func FromCmdGetElapsedSeconds(cmd string) int64 {
	ElapsedSecondsOutput, _ := RunCommand(cmd)
	// 先判断作业时长中是否包含-
	// 超过一天的作业
	if strings.Contains(ElapsedSecondsOutput, "-") {
		ElapsedSecondsList := strings.Split(ElapsedSecondsOutput, "-")
		day, _ := strconv.Atoi(ElapsedSecondsList[0])
		ElapsedSecondsListNew := strings.Split(ElapsedSecondsList[1], ":")
		hours, _ := strconv.Atoi(ElapsedSecondsListNew[0])
		minutes, _ := strconv.Atoi(ElapsedSecondsListNew[1])
		seconds, _ := strconv.Atoi(ElapsedSecondsListNew[2])
		return int64(seconds) + int64(minutes)*60 + int64(hours)*3600 + int64(day)*24*3600
	} else {
		// 没有超过一天的作业
		ElapsedSecondsList := strings.Split(ElapsedSecondsOutput, ":")
		hours, _ := strconv.Atoi(ElapsedSecondsList[0])
		minutes, _ := strconv.Atoi(ElapsedSecondsList[1])
		seconds, _ := strconv.Atoi(ElapsedSecondsList[2])
		elapsedSeconds := int64(seconds) + int64(minutes)*60 + int64(hours)*3600
		return elapsedSeconds
	}
}

func Ping(hostName string) bool {
	pingCmd := fmt.Sprintf("ping -c 1 %s  > /dev/null && echo true || echo false", hostName)
	output, err := exec.Command("/bin/sh", "-c", pingCmd).Output()
	if err != nil {
		return false
	}
	status := strings.TrimSpace(string(output))
	if status == "false" {
		return false
	} else {
		return true
	}
}

func GetGpuAllocsFromGpuId(matchCmd string, gpuId int, tresAlloc string) int32 {
	var gpusAlloc int32
	res := ExecuteShellCommand(matchCmd)
	if res == 0 {
		resAllocList := strings.Split(tresAlloc, ",")
		for _, v := range resAllocList {
			vList := strings.Split(v, "=")
			id := vList[0]
			number := vList[1]
			idInt, _ := strconv.Atoi(id)
			numberInt, _ := strconv.Atoi(number)
			if idInt == gpuId {
				gpusAlloc = int32(numberInt)
				return gpusAlloc
			}
		}
	}
	return 0
}

func GetGpuAllocsFromGpuIdList(tresAlloc string, gpuId []int) int32 {
	var gpusAlloc int32
	resAllocList := strings.Split(tresAlloc, ",")
	for _, idValue := range gpuId {
		for _, resAlloc := range resAllocList {
			resAllocKey := strings.Split(resAlloc, "=")
			id := resAllocKey[0]
			idInt, _ := strconv.Atoi(id)
			if idInt == idValue {
				number := resAllocKey[1]
				numberInt, _ := strconv.Atoi(number)
				gpusAlloc = int32(numberInt)
				return gpusAlloc
			}
		}
	}
	return gpusAlloc
}

func GetResInfoNumFromTresInfo(tresInfo string, resId int) int {
	var resInfoNum int
	resAllocList := strings.Split(tresInfo, ",")
	for _, resInfo := range resAllocList {
		resInfoKey := strings.Split(resInfo, "=")
		id := resInfoKey[0]
		idInt, _ := strconv.Atoi(id)
		if idInt == resId {
			tresNum := resInfoKey[1]
			tresNumInt, _ := strconv.Atoi(tresNum)
			resInfoNum = tresNumInt
			return resInfoNum
		}
	}
	return resInfoNum
}

func SshExectueShellCmd(hostName string, user string, cmd string) ([]string, error) {
	var errbuf bytes.Buffer
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadFile(path.Join(homePath, ".ssh", "id_rsa"))
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	client, err := ssh.Dial("tcp", hostName, &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		return nil, err
	}
	// 建立新会话
	session, err := client.NewSession()
	defer session.Close()
	if err != nil {
		return nil, err
	}

	// Set the output to a bytes.Buffer
	session.Stderr = &errbuf
	// 会话输入关联到系统标准输入设备
	result, err := session.Output(cmd)
	// stderr as a string by calling the Buffer.String() method
	stderr := errbuf.String()
	if err != nil {
		return strings.Split(stderr, " "), err
	}
	outputList := strings.Split(strings.TrimSpace(string(result)), " ")
	return outputList, nil
}

func SshSubmitJobCommand(hostName string, user string, script string, workingDirectory string) ([]string, error) {
	var errbuf bytes.Buffer
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadFile(path.Join(homePath, ".ssh", "id_rsa"))
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	client, err := ssh.Dial("tcp", hostName, &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		return nil, err
	}
	// 建立新会话
	session, err := client.NewSession()
	defer session.Close()
	if err != nil {
		return nil, err
	}

	// Set the output to a bytes.Buffer
	session.Stderr = &errbuf
	createDirAndSubmitJob := fmt.Sprintf("mkdir -p %s; sbatch", workingDirectory)
	// 会话输入关联到系统标准输入设备
	session.Stdin = strings.NewReader(script)
	result, err := session.Output(createDirAndSubmitJob)
	// stderr as a string by calling the Buffer.String() method
	stderr := errbuf.String()
	if err != nil {
		return strings.Split(stderr, " "), err
	}
	outputList := strings.Split(strings.TrimSpace(string(result)), " ")
	return outputList, nil
}

func SearchUidNumberFromLdap(user string) (int, error) {
	config := config.ParseConfig(config.DefaultConfigPath)
	ip := config.LDAPConfig.IP
	port := config.LDAPConfig.Port
	baseDN := config.LDAPConfig.BaseDN
	bindDN := config.LDAPConfig.BindDN
	password := config.LDAPConfig.Password

	ldapUrl := fmt.Sprintf("%s:%d", ip, port)
	l, err := ldap.Dial("tcp", ldapUrl)
	if err != nil {
		fmt.Printf("Failed to connect to LDAP server: %s", err.Error())
		return 0, err
	}
	defer l.Close()

	// 绑定到 LDAP 服务器，使用管理员账户进行查询
	err = l.Bind(fmt.Sprintf("%v", bindDN), fmt.Sprintf("%v", password))
	if err != nil {
		fmt.Printf("Failed to bind to LDAP server: %s", err.Error())
		return 0, err
	}

	// 查询用户的 UID
	searchRequest := ldap.NewSearchRequest(
		fmt.Sprintf("%v", baseDN),
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&(objectClass=posixAccount)(uid=%s))", user),
		[]string{"uidNumber"},
		nil,
	)
	searchResult, err := l.Search(searchRequest)
	if err != nil {
		fmt.Printf("Failed to search LDAP server: %s", err.Error())
		return 0, err
	}

	// 打印查询结果
	if len(searchResult.Entries) == 0 {
		return 0, errors.New("User not found.")
	} else {
		uid := searchResult.Entries[0].GetAttributeValue("uidNumber")
		myIntUid, _ := strconv.Atoi(uid)
		return myIntUid, nil
	}
}

func SearchUserUidFromLdap(uid int) (string, error) {
	config := config.ParseConfig(config.DefaultConfigPath)
	ip := config.LDAPConfig.IP
	port := config.LDAPConfig.Port
	baseDN := config.LDAPConfig.BaseDN
	bindDN := config.LDAPConfig.BindDN
	password := config.LDAPConfig.Password

	ldapUrl := fmt.Sprintf("%s:%d", ip, port)
	l, err := ldap.Dial("tcp", ldapUrl)
	if err != nil {
		fmt.Printf("Failed to connect to LDAP server: %s", err.Error())
		return "", err
	}
	defer l.Close()

	// 绑定到 LDAP 服务器，使用管理员账户进行查询
	err = l.Bind(fmt.Sprintf("%v", bindDN), fmt.Sprintf("%v", password))
	if err != nil {
		fmt.Printf("Failed to bind to LDAP server: %s", err.Error())
		return "", err
	}

	// 查询用户的 UID
	searchRequest := ldap.NewSearchRequest(
		fmt.Sprintf("%v", baseDN),
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&(objectClass=posixAccount)(uidNumber=%d))", uid),
		[]string{"uid"},
		nil,
	)
	searchResult, err := l.Search(searchRequest)
	if err != nil {
		fmt.Printf("Failed to search LDAP server: %s", err.Error())
		return "", err
	}

	// 打印查询结果
	if len(searchResult.Entries) == 0 {
		return "", errors.New("User not found.")
	} else {
		uid := searchResult.Entries[0].GetAttributeValue("uid")
		// myIntUid, _ := strconv.Atoi(uid)
		return uid, nil
	}
}
