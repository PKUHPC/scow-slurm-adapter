package congfig

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v3"
)

type MySQLConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	User        string `yaml:"user"`
	DBName      string `yaml:"dbname"`
	Password    string `yaml:"password"`
	ClusterName string `yaml:"clustername"`
}

type Service struct {
	Port int `yaml:"port"`
}

type Slurm struct {
	DefaultQOS string `yaml:"defaultqos"`
}

type Config struct {
	MySQLConfig MySQLConfig `yaml:"mysql"`
	Service     Service     `yaml:"service"`
	Slurm       Slurm       `yaml:"slurm"`
}

var (
	DefaultConfigPath string = "config/config.yaml"
)

// 解析配置文件
func ParseConfig(configFilePath string) *Config {
	confFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatal(err)
	}
	config := &Config{}

	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}
