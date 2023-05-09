package congfig

import (
	"github.com/spf13/viper"
)

type Config struct {
	MysqlConfig `mapstructure:"mysql"`
}

type MysqlConfig struct {
	Host        string `mapstructure:"host"`
	Port        int    `mapstructure:"port"`
	User        string `mapstructure:"user"`
	DbName      string `mapstructure:"dbname"`
	PassWord    string `mapstructure:"password"`
	ClusterName string `mapstructure:"clustername"`
}

func ParseConfig() map[string]interface{} {
	// 读取配置文件内容
	config := viper.New()
	config.AddConfigPath("./config")
	config.SetConfigName("config")
	config.SetConfigType("yaml")

	if err := config.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(err)
		}
	}
	allSettings := config.AllSettings()
	return allSettings
}
