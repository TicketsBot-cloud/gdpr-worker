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
	Owner          uint64        `env:"OWNER"`
	DashboardUrl   string        `env:"DASHBOARD_URL" envDefault:"https://dashboard.tickets.bot"`

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
		Url    string `env:"URL,required,notEmpty"`
		AesKey string `env:"AES_KEY,required,notEmpty"`
	} `envPrefix:"ARCHIVER_"`

	Discord struct {
		ProxyUrl string `env:"PROXY_URL"`
		Token    string `env:"TOKEN,required,notEmpty"`
	} `envPrefix:"DISCORD_"`

	CacheDatabase struct {
		Host     string `env:"HOST"`
		Database string `env:"NAME"`
		Username string `env:"USER"`
		Password string `env:"PASSWORD"`
		Threads  int    `env:"THREADS" envDefault:"5"`
	} `envPrefix:"CACHE_"`

	Export struct {
		Timeout     time.Duration `env:"TIMEOUT" envDefault:"30m"`
		Concurrency int           `env:"CONCURRENCY" envDefault:"15"`
	} `envPrefix:"EXPORT_"`

	ExportS3 struct {
		Endpoint  string `env:"ENDPOINT,required,notEmpty"`
		Region    string `env:"REGION"`
		Bucket    string `env:"BUCKET,required,notEmpty"`
		AccessKey string `env:"ACCESS_KEY,required,notEmpty"`
		SecretKey string `env:"SECRET_KEY,required,notEmpty"`
		Secure    bool   `env:"SECURE" envDefault:"true"`
	} `envPrefix:"S3_EXPORT_"`
}

var Conf Config

func Parse() {
	if err := env.Parse(&Conf); err != nil {
		panic(err)
	}
}
