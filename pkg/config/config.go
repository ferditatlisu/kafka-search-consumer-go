package config

import (
	"github.com/spf13/viper"
	"os"
)

type Config interface {
	GetConfig() (*ApplicationConfig, error)
}

type ServerConfig struct {
	Port string
}

type ApplicationConfig struct {
	Server      ServerConfig
	SearchKafka SearchKafkaConfig
	EarthKafka  SearchKafkaConfig
	SecureKafka SecureKafkaConfig
	Redis       RedisConfig
}

type SearchKafkaConfig struct {
	Servers string
	Topic   string
	Group   string
}

type SecureKafkaConfig struct {
	Servers            string
	UserName           string
	Password           string
	RootCAPath         string
	IntermediateCAPath string
}

type RedisConfig struct {
	Host     string
	Password string
	Database int
}

type config struct{}

func (c *config) GetConfig() (*ApplicationConfig, error) {
	configuration := ApplicationConfig{}
	env := getGoEnv()

	viperInstance := getViperInstance()
	err := viperInstance.ReadInConfig()

	if err != nil {
		return nil, err
	}

	sub := viperInstance.Sub(env)
	err = sub.Unmarshal(&configuration)

	if err != nil {
		return nil, err
	}

	return &configuration, nil
}

func CreateConfigInstance() *config {
	return &config{}
}

func getViperInstance() *viper.Viper {
	viperInstance := viper.New()
	viperInstance.SetConfigFile("resources/config.yml")
	return viperInstance
}

func getGoEnv() string {
	env := os.Getenv("GO_ENV")
	if env != "" {
		return env
	}
	return "stage"
}
