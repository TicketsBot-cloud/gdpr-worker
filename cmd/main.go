package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/callback"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/heartbeat"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/processor"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	config.Parse()

	logger := initLogger(config.Conf.JsonLogs, config.Conf.LogLevel)
	logger.Info("Starting GDPR Worker")

	logger.Info("Initializing i18n")
	if err := i18n.Init("locale"); err != nil {
		logger.Fatal("Failed to initialize i18n", zap.Error(err))
		return
	}

	logger.Info("Connecting to Redis")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.Conf.Redis.Address,
		Password: config.Conf.Redis.Password,
		DB:       0,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
		return
	}
	logger.Info("Connected to Redis")

	logger.Info("Connecting to database")
	if err := database.Connect(
		logger.With(zap.String("service", "database")),
		config.Conf.Database.Host,
		config.Conf.Database.Database,
		config.Conf.Database.Username,
		config.Conf.Database.Password,
		config.Conf.Database.Threads,
	); err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
		return
	}

	logger.Info("Initializing archiver client")
	archiver.Initialize(
		logger.With(zap.String("service", "archiver")),
		config.Conf.Archiver.Url,
		config.Conf.Archiver.AesKey,
	)

	proc := processor.New(logger.With(zap.String("service", "processor")))

	callbackHandler := callback.New(
		logger.With(zap.String("service", "callback")),
		config.Conf.Discord.ProxyUrl,
	)

	logger.Info("Starting heartbeat")
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	defer heartbeatCancel()
	go heartbeat.Start(heartbeatCtx, redisClient, logger.With(zap.String("service", "heartbeat")))

	logger.Info("Starting GDPR queue listener",
		zap.Int("max_concurrency", config.Conf.MaxConcurrency),
	)
	ch := make(chan gdprrelay.QueuedRequest)
	go gdprrelay.Listen(redisClient, ch, logger.With(zap.String("service", "gdprrelay")))

	semaphore := make(chan struct{}, config.Conf.MaxConcurrency)

	go func() {
		for request := range ch {
			semaphore <- struct{}{}

			go func(req gdprrelay.QueuedRequest) {
				defer func() {
					<-semaphore
				}()

				processCtx := context.Background()

				logger.Info("Processing GDPR request",
					zap.Int("type", int(req.Request.Type)),
					zap.Uint64("user_id", req.Request.UserId),
				)

				result := proc.Process(processCtx, req.Request)

				if result.Error != nil {
					logger.Error("Failed to process GDPR request",
						zap.Int("type", int(req.Request.Type)),
						zap.Uint64("user_id", req.Request.UserId),
						zap.Error(result.Error),
					)

					if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, "Failed"); updateErr != nil {
						logger.Error("Failed to update GDPR log",
							zap.Error(updateErr),
							zap.Uint64("user_id", req.Request.UserId),
						)
					}

					if rejectErr := gdprrelay.Reject(processCtx, redisClient, req.Request, logger); rejectErr != nil {
						logger.Error("Failed to reject GDPR request",
							zap.Error(rejectErr),
							zap.Uint64("user_id", req.Request.UserId),
						)
					}
				} else {
					if ackErr := gdprrelay.Acknowledge(processCtx, redisClient, req.Request, logger); ackErr != nil {
						logger.Error("Failed to acknowledge GDPR request",
							zap.Error(ackErr),
							zap.Uint64("user_id", req.Request.UserId),
						)
					}
				}

				callbackData := callback.ResultData{
					TotalDeleted:    result.TotalDeleted,
					MessagesDeleted: result.MessagesDeleted,
					Error:           result.Error,
					RequestType:     req.Request.Type,
					GuildIds:        req.Request.GuildIds,
					TicketIds:       req.Request.TicketIds,
				}

				callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer callbackCancel()

				if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, "Completed"); updateErr != nil {
					logger.Error("Failed to update GDPR log",
						zap.Error(updateErr),
						zap.Uint64("user_id", req.Request.UserId),
					)
				}

				if err := callbackHandler.SendCompletion(callbackCtx, req.Request, callbackData); err != nil {
					logger.Error("Failed to send completion callback",
						zap.Error(err),
						zap.Uint64("user_id", req.Request.UserId),
					)
				}
			}(request)
		}
	}()

	logger.Info("GDPR Worker is now running.")

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownCh

	logger.Info("Received shutdown signal, cleaning up...")

	logger.Info("GDPR Worker shutdown complete")
	fmt.Println("GDPR Worker stopped")
}

func initLogger(jsonLogs bool, level zapcore.Level) *zap.Logger {
	var config zap.Config

	if jsonLogs {
		config = zap.NewProductionConfig()
	} else {
		config = zap.NewDevelopmentConfig()
	}

	config.Level = zap.NewAtomicLevelAt(level)

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger
}
