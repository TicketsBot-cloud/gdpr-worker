package main

import (
	"context"
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
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
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
		logger.With(),
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
		logger.With(),
		config.Conf.Archiver.Url,
		config.Conf.Archiver.AesKey,
	)

	proc := processor.New(logger.With())

	callbackHandler := callback.New(
		logger.With(),
		config.Conf.Discord.ProxyUrl,
	)

	logger.Info("Starting heartbeat")
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	defer heartbeatCancel()
	go heartbeat.Start(heartbeatCtx, redisClient, logger.With())

	logger.Info("Starting GDPR queue listener")
	ch := make(chan gdprrelay.QueuedRequest)
	go gdprrelay.Listen(redisClient, ch, logger.With())

	semaphore := make(chan struct{}, config.Conf.MaxConcurrency)

	go func() {
		for request := range ch {
			semaphore <- struct{}{}

			go func(req gdprrelay.QueuedRequest) {
				defer func() {
					<-semaphore
				}()

				processCtx := context.Background()

				scrambledId := utils.ScrambleUserId(req.Request.UserId)
				requestTypeName := utils.GetRequestTypeName(int(req.Request.Type))

				logger.Info("Processing GDPR request",
					zap.String("scrambled_user_id", scrambledId),
					zap.String("request_type", requestTypeName),
					zap.Uint64("request_id", uint64(req.RequestID)),
				)

				result := proc.Process(processCtx, req.Request)

				if result.Error != nil {
					logger.Error("Failed to process GDPR request",
						zap.String("scrambled_user_id", scrambledId),
						zap.String("request_type", requestTypeName),
						zap.Uint64("request_id", uint64(req.RequestID)),
						zap.Error(result.Error),
					)

					if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, "Failed"); updateErr != nil {
						logger.Error("Failed to update GDPR log",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(updateErr),
						)
					}

					if rejectErr := gdprrelay.Reject(processCtx, redisClient, req.Request, logger); rejectErr != nil {
						logger.Error("Failed to reject GDPR request",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(rejectErr),
						)
					}
				} else {
					if ackErr := gdprrelay.Acknowledge(processCtx, redisClient, req.Request, logger); ackErr != nil {
						logger.Error("Failed to acknowledge GDPR request",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(ackErr),
						)
					}
				}

				callbackData := callback.ResultData{
					TranscriptsDeleted:    result.TranscriptsDeleted,
					MessagesDeleted: result.MessagesDeleted,
					Error:           result.Error,
					RequestType:     req.Request.Type,
					GuildIds:        req.Request.GuildIds,
					TicketIds:       req.Request.TicketIds,
				}

				callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer callbackCancel()

				if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, "Completed"); updateErr != nil {
					logger.Error("Failed to update GDPR log status to Completed",
						zap.Uint64("request_id", uint64(req.RequestID)),
						zap.String("scrambled_user_id", scrambledId),
						zap.Error(updateErr),
					)
				}

				if err := callbackHandler.SendCompletion(callbackCtx, req.Request, callbackData); err != nil {
					logger.Error("Failed to send completion callback",
						zap.Uint64("request_id", uint64(req.RequestID)),
						zap.String("scrambled_user_id", scrambledId),
						zap.Error(err),
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
}

func initLogger(jsonLogs bool, level zapcore.Level) *zap.Logger {
	var config zap.Config

	if jsonLogs {
		config = zap.NewProductionConfig()
	} else {
		config = zap.NewDevelopmentConfig()
	}

	config.Level = zap.NewAtomicLevelAt(level)
	config.DisableCaller = true        // Disable file/line information
	config.DisableStacktrace = true     // Disable stack traces

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger
}
