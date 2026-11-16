package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdl/rest/request"
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/callback"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/cleanup"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	queue "github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/heartbeat"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/processor"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	_ "github.com/joho/godotenv/autoload"
)

var errRetriesExhausted = errors.New("processing was interrupted too many times, please try again later")

// The proxy expects the request addressed to its own host rather than the usual CONNECT protocol.
func proxyHook(token string, req *http.Request) {
	if !strings.HasPrefix(req.URL.Path, "/api/v9/applications/") {
		req.URL.Scheme = "http"
		req.URL.Host = config.Conf.Discord.ProxyUrl
	}
}

func startExport(ctx context.Context, exportId int64, logger *zap.Logger) bool {
	row, err := database.Client.GdprExports.Get(ctx, exportId)
	if err != nil {
		// A database error must not drop the request.
		logger.Error("Failed to load GDPR export", zap.Int64("export_id", exportId), zap.Error(err))
		return true
	}

	if row == nil || (row.Status != dbmodel.GdprExportStatusQueued && row.Status != dbmodel.GdprExportStatusProcessing) {
		logger.Warn("Dropping GDPR export that is no longer pending", zap.Int64("export_id", exportId))
		return false
	}

	if err := database.Client.GdprExports.SetStatus(ctx, exportId, dbmodel.GdprExportStatusProcessing); err != nil {
		logger.Error("Failed to mark GDPR export as processing", zap.Int64("export_id", exportId), zap.Error(err))
	}

	return true
}

func main() {
	config.Parse()

	logger := initLogger(config.Conf.JsonLogs, config.Conf.LogLevel)
	logger.Info("Starting GDPR Worker")

	logger.Info("Initializing i18n")
	if err := i18n.Init("locale"); err != nil {
		logger.Fatal("Failed to initialize i18n", zap.Error(err))
		return
	}

	if config.Conf.Discord.ProxyUrl != "" {
		logger.Info("Configuring REST proxy", zap.String("url", config.Conf.Discord.ProxyUrl))
		request.RegisterPreRequestHook(proxyHook)
	}

	logger.Info("Connecting to Redis")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.Conf.Redis.Address,
		Password: config.Conf.Redis.Password,
		PoolSize: config.Conf.Redis.Threads,
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

	logger.Info("Initialising archiver client")
	archiver.Initialize(
		logger.With(),
		config.Conf.Archiver.Url,
		config.Conf.Archiver.AesKey,
	)

	var cachePool *pgxpool.Pool
	if config.Conf.CacheDatabase.Host != "" {
		logger.Info("Connecting to cache database")

		var err error
		cachePool, err = database.NewPool(
			context.Background(),
			config.Conf.CacheDatabase.Host,
			config.Conf.CacheDatabase.Database,
			config.Conf.CacheDatabase.Username,
			config.Conf.CacheDatabase.Password,
			config.Conf.CacheDatabase.Threads,
		)
		if err != nil {
			logger.Fatal("Failed to connect to cache database", zap.Error(err))
			return
		}
		logger.Info("Connected to cache database")
	} else {
		logger.Warn("Cache database not configured, user export cache data will be unavailable")
	}

	logger.Info("Initialising export storage client")
	store, err := minio.New(config.Conf.ExportS3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Conf.ExportS3.AccessKey, config.Conf.ExportS3.SecretKey, ""),
		Secure: config.Conf.ExportS3.Secure,
		Region: config.Conf.ExportS3.Region,
	})
	if err != nil {
		logger.Fatal("Failed to initialise export storage client", zap.Error(err))
		return
	}

	proc := processor.New(logger.With(), cachePool, store)

	callbackHandler := callback.New(
		logger.With(),
		config.Conf.Discord.ProxyUrl,
	)

	logger.Info("Starting heartbeat")
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	defer heartbeatCancel()
	go heartbeat.Start(heartbeatCtx, redisClient, logger.With())

	logger.Info("Starting export cleanup")
	go cleanup.Start(context.Background(), store, config.Conf.ExportS3.Bucket, logger.With())

	logger.Info("Starting GDPR queue listener")
	ch := make(chan gdprrelay.QueuedRequest)
	semaphore := make(chan struct{}, config.Conf.MaxConcurrency)
	go queue.Listen(redisClient, ch, semaphore, logger.With())

	go func() {
		for request := range ch {
			go func(req gdprrelay.QueuedRequest) {
				defer func() {
					<-semaphore
				}()

				processCtx := context.Background()

				scrambledId := utils.ScrambleUserId(req.Request.UserId)
				requestTypeName := gdprrelay.LogNames[req.Request.Type]

				logger.Info("Processing GDPR request",
					zap.String("scrambled_user_id", scrambledId),
					zap.String("request_type", requestTypeName),
					zap.Uint64("request_id", uint64(req.RequestID)),
				)

				if req.Request.ExportId != 0 && !startExport(processCtx, req.Request.ExportId, logger) {
					if ackErr := queue.Acknowledge(processCtx, redisClient, req, logger); ackErr != nil {
						logger.Error("Failed to acknowledge GDPR request",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(ackErr),
						)
					}
					return
				}

				var result processor.ProcessResult
				if req.RetryCount >= config.Conf.MaxRetries {
					result.Error = errRetriesExhausted
				} else {
					result = proc.Process(processCtx, req.Request)
				}
				// A retry only sees what earlier attempts left, so their counts carry over.
				result.TranscriptsDeleted += req.TranscriptsDeleted
				result.MessagesDeleted += req.MessagesDeleted

				finishCtx, finishCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer finishCancel()

				if result.Error != nil {
					logger.Error("Failed to process GDPR request",
						zap.String("scrambled_user_id", scrambledId),
						zap.String("request_type", requestTypeName),
						zap.Uint64("request_id", uint64(req.RequestID)),
						zap.Error(result.Error),
					)

					deadLettered, rejectErr := queue.Reject(finishCtx, redisClient, req, result.TranscriptsDeleted, result.MessagesDeleted, logger)
					if rejectErr != nil {
						logger.Error("Failed to reject GDPR request",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(rejectErr),
						)
					}

					if !deadLettered {
						return
					}

					if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, dbmodel.GdprLogStatusFailed); updateErr != nil {
						logger.Error("Failed to update GDPR log",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(updateErr),
						)
					}

					if req.Request.ExportId != 0 {
						if updateErr := database.Client.GdprExports.SetStatus(finishCtx, req.Request.ExportId, dbmodel.GdprExportStatusFailed); updateErr != nil {
							logger.Error("Failed to mark GDPR export as failed",
								zap.Int64("export_id", req.Request.ExportId),
								zap.String("scrambled_user_id", scrambledId),
								zap.Error(updateErr),
							)
						}
					}
				} else {
					if ackErr := queue.Acknowledge(finishCtx, redisClient, req, logger); ackErr != nil {
						logger.Error("Failed to acknowledge GDPR request",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(ackErr),
						)
					}
				}

				callbackData := callback.ResultData{
					TranscriptsDeleted: result.TranscriptsDeleted,
					MessagesDeleted:    result.MessagesDeleted,
					Error:              result.Error,
					RequestType:        req.Request.Type,
					GuildIds:           req.Request.GuildIds,
					TicketIds:          req.Request.TicketIds,
					ExportedFiles:      result.ExportedFiles,
					ExportId:           req.Request.ExportId,
				}

				if result.Error == nil {
					if updateErr := database.Client.GdprLogs.UpdateLogStatus(req.RequestID, dbmodel.GdprLogStatusCompleted); updateErr != nil {
						logger.Error("Failed to update GDPR log status to Completed",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(updateErr),
						)
					}
				}

				if req.Request.Source == gdprrelay.SourceDashboard {
					notification := gdprrelay.Notification{
						UserId:      req.Request.UserId,
						RequestId:   req.RequestID,
						RequestType: req.Request.Type,
						Status:      callbackData.NotificationStatus(),
						ExportId:    req.Request.ExportId,
						// Each request type sets at most one of the two counts.
						Count: result.TranscriptsDeleted + result.MessagesDeleted,
					}
					if result.Error != nil {
						notification.Error = result.Error.Error()
					}

					notifyCtx, notifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer notifyCancel()

					if err := gdprrelay.Notify(notifyCtx, redisClient, notification); err != nil {
						logger.Error("Failed to send GDPR notification",
							zap.Uint64("request_id", uint64(req.RequestID)),
							zap.String("scrambled_user_id", scrambledId),
							zap.Error(err),
						)
					}
					return
				}

				callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer callbackCancel()

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
	config.DisableCaller = true
	config.DisableStacktrace = true

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger
}
