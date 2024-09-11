package caller

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"scow-slurm-adapter/utils"
)

var (
	DB          *sql.DB
	ConfigValue *utils.Config
	Logger      *logrus.Logger
)

type LogFormatter struct{}

func (m *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format("2006-01-02 15:04:05")
	var newLog string

	// HasCaller()为true才会有调用信息
	if entry.HasCaller() {
		fName := filepath.Base(entry.Caller.File)
		newLog = fmt.Sprintf("[%s] [%s] [%s:%d %s] %s\n",
			timestamp, entry.Level, fName, entry.Caller.Line, entry.Caller.Function, entry.Message)
	} else {
		newLog = fmt.Sprintf("[%s] [%s] %s\n", timestamp, entry.Level, entry.Message)
	}

	b.WriteString(newLog)
	return b.Bytes(), nil
}

func init() {
	currentPwd, _ := os.Getwd()
	ConfigValue = utils.ParseConfig(currentPwd + "/" + utils.DefaultConfigPath)
	initDB()
	initLogger()
}

func initDB() {
	var (
		err error
	)
	dbConfig := utils.DatabaseConfig()
	DB, err = sql.Open("mysql", dbConfig)
	if err != nil {
		log.Fatal(err)
	}
	// 测试数据库连接
	err = DB.Ping()
	if err != nil {
		log.Fatal(err)
	}
	// defer DB.Close()
}

func initLogger() {
	Logger = logrus.New()
	Logger.SetReportCaller(true)
	// 设置日志输出格式为JSON
	Logger.SetFormatter(&LogFormatter{})
	// 设置日志级别为Info
	switch ConfigValue.LogConfig.Level {
	case "info":
		Logger.SetLevel(logrus.InfoLevel)
	case "debug":
		Logger.SetLevel(logrus.DebugLevel)
	case "trace":
		Logger.SetLevel(logrus.TraceLevel)
	default:
		Logger.SetLevel(logrus.InfoLevel)
	}

	// 创建一个 lumberjack.Logger，用于日志轮转配置
	logFile := &lumberjack.Logger{
		Filename:   "server.log", // 日志文件路径
		MaxSize:    100,          // 日志文件的最大大小（以MB为单位）
		MaxBackups: 5,            // 保留的旧日志文件数量
		MaxAge:     50,           // 保留的旧日志文件的最大天数
		LocalTime:  true,         // 使用本地时间戳
		Compress:   true,         // 是否压缩旧日志文件
	}
	Logger.SetOutput(io.MultiWriter(os.Stdout, logFile))
	defer logFile.Close()
}
