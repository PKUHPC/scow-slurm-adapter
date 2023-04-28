package utils

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"

	"os"
	"os/exec"
	"path"
	config "scow-slurm-adapter-server/config"
	"strings"
	"syscall"

	"golang.org/x/crypto/ssh"
)

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
	allSettings := config.ParseConfig()
	mysql := allSettings["mysql"]
	host := mysql.(map[string]interface{})["host"]
	userName := mysql.(map[string]interface{})["user"]
	passWord := mysql.(map[string]interface{})["password"]
	dbName := mysql.(map[string]interface{})["dbname"]
	port := mysql.(map[string]interface{})["port"]

	dbConfig := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, passWord, host, port, dbName, "latin1")
	return dbConfig
}

func GetPatitionInfo() ([]string, error) {
	shellCmd := fmt.Sprintf("cat /etc/slurm/slurm.conf | grep -i PartitionName | awk '{print $1}' | awk -F'=' '{print $2}'| tr '\n' ','")
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
		stateString = "CANCELLED"
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
	case "CANCELLED":
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
	ElapsedSecondsList := strings.Split(ElapsedSecondsOutput, ":")
	hours, _ := strconv.Atoi(ElapsedSecondsList[0])
	minutes, _ := strconv.Atoi(ElapsedSecondsList[1])
	seconds, _ := strconv.Atoi(ElapsedSecondsList[2])
	elapsedSeconds := int64(seconds) + int64(minutes)*60 + int64(hours)*3600
	return elapsedSeconds
}

func Ping(hostname string) bool {
	Command := fmt.Sprintf("ping -c 1 %s  > /dev/null && echo true || echo false", hostname)
	output, err := exec.Command("/bin/sh", "-c", Command).Output()
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
