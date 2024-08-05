package caller

import (
	"database/sql"
	"io"
	"log"
	"os"
	"scow-slurm-adapter/utils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	DB          *sql.DB
	ConfigValue *utils.Config
	Logger      *logrus.Logger
)

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
	// 设置日志输出格式为JSON
	Logger.SetFormatter(&logrus.JSONFormatter{})
	// 设置日志级别为Info
	Logger.SetLevel(logrus.InfoLevel)

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
