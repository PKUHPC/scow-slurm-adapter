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

type LDAPConfig struct {
	IP       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	BaseDN   string `yaml:"basedn"`
	BindDN   string `yaml:"binddn"`
	Password string `yaml:"password"`
}

type Config struct {
	MySQLConfig MySQLConfig `yaml:"mysql"`
	LDAPConfig  LDAPConfig  `yaml:"ldap"`
	LoginNodes  []string    `yaml:"loginNode"`
}

var (
	DefaultConfigPath string = "config/config.yaml"
)

// func init() {
// 	DefaultConfigPath = "config/config.yaml"
// }

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
