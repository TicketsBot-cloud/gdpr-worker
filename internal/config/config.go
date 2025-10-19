package config

import (
	"github.com/caarlos0/env/v10"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	JsonLogs        bool          `env:"JSON_LOGS" envDefault:"false"`
	LogLevel        zapcore.Level `env:"LOG_LEVEL" envDefault:"info"`
	MaxConcurrency  int           `env:"MAX_CONCURRENCY" envDefault:"1"`
	MaxRetries      int           `env:"MAX_RETRIES" envDefault:"3"`

	Database struct {
		Host     string `env:"HOST"`
		Database string `env:"NAME"`
		Username string `env:"USER"`
		Password string `env:"PASSWORD"`
		Threads  int    `env:"THREADS"`
	} `envPrefix:"DATABASE_"`

	Redis struct {
		Address  string `env:"ADDR"`
		Password string `env:"PASSWD"`
		Threads  int    `env:"THREADS"`
	} `envPrefix:"REDIS_"`

	Archiver struct {
		Url    string `env:"URL"`
		AesKey string `env:"AES_KEY"`
	} `envPrefix:"ARCHIVER_"`

	Discord struct {
		ProxyUrl string `env:"PROXY_URL"`
		Token    string `env:"TOKEN"`
	} `envPrefix:"DISCORD_"`
}

var Conf Config

func Parse() {
	if err := env.Parse(&Conf); err != nil {
		panic(err)
	}
}

func init() {
	Parse()
}
