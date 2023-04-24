package tools

import (
	"fmt"
	"os/exec"
	config "scow-slurm-adapter-server/config"
	"strings"
	"syscall"
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

	dbConfig := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", userName, passWord, host, port, dbName)
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
	if stateInit == 0 {
		stateString = "PENDING"
	} else if stateInit == 1 {
		stateString = "RUNNING"
	} else if stateInit == 2 {
		stateString = "SUSPEND"
	} else if stateInit == 3 {
		stateString = "COMPLETED"
	} else if stateInit == 4 {
		stateString = "CANCELLED"
	} else if stateInit == 5 {
		stateString = "FAILED"
	} else if stateInit == 6 {
		stateString = "TIMEOUT"
	} else if stateInit == 7 {
		stateString = "NODE_FAIL"
	} else {
		stateString = "COMPLETED"
	}
	return stateString
}
