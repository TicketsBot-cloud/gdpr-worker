package config

import (
	"time"

	"github.com/caarlos0/env/v10"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	JsonLogs       bool          `env:"JSON_LOGS" envDefault:"false"`
	LogLevel       zapcore.Level `env:"LOG_LEVEL" envDefault:"info"`
	MaxConcurrency int           `env:"MAX_CONCURRENCY" envDefault:"1"`
	MaxRetries     int           `env:"MAX_RETRIES" envDefault:"3"`

	Database struct {
		Host     string `env:"HOST,required,notEmpty"`
		Database string `env:"NAME,required,notEmpty"`
		Username string `env:"USER,required,notEmpty"`
		Password string `env:"PASSWORD"`
		Threads  int    `env:"THREADS" envDefault:"5"`
	} `envPrefix:"DATABASE_"`

	Redis struct {
		Address  string `env:"ADDR,required,notEmpty"`
		Password string `env:"PASSWD"`
		Threads  int    `env:"THREADS" envDefault:"5"`
	} `envPrefix:"REDIS_"`

	Archiver struct {
		Url string `env:"URL,required,notEmpty"`
		AesKey string `env:"AES_KEY,required,notEmpty"`
	} `envPrefix:"ARCHIVER_"`

	Discord struct {
		ProxyUrl string `env:"PROXY_URL"`
		Token string `env:"TOKEN,required,notEmpty"`
	} `envPrefix:"DISCORD_"`

	CacheDatabase struct {
		Host     string `env:"HOST"`
		Database string `env:"NAME"`
		Username string `env:"USER"`
		Password string `env:"PASSWORD"`
		Threads  int    `env:"THREADS" envDefault:"5"`
	} `envPrefix:"CACHE_"`

	Export struct {
		MaxPartBytes int `env:"MAX_PART_BYTES" envDefault:"12582912"`
		MaxMessageBytes int `env:"MAX_MESSAGE_BYTES" envDefault:"25165824"`
		MaxAttachmentBytes int `env:"MAX_ATTACHMENT_BYTES" envDefault:"20971520"`
		DmDelay     time.Duration `env:"DM_DELAY" envDefault:"3s"`
		Timeout     time.Duration `env:"TIMEOUT" envDefault:"30m"`
		Concurrency int           `env:"CONCURRENCY" envDefault:"15"`
	} `envPrefix:"EXPORT_"`
}

var Conf Config

func Parse() {
	if err := env.Parse(&Conf); err != nil {
		panic(err)
	}
}
