package gdprrelay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const (
	keyProcessing = "tickets:gdpr:processing" // Redis list for GDPR requests currently being processed
	keyFailed     = "tickets:gdpr:failed"     // Redis list for GDPR requests that exceeded max retries
)

func Listen(redisClient *redis.Client, ch chan gdprrelay.QueuedRequest, semaphore chan struct{}, logger *zap.Logger) {
	ctx := context.Background()

	if err := recoverStalledRequests(ctx, redisClient, logger); err != nil {
		logger.Error("Failed to recover stalled requests", zap.Error(err))
	}

	for {
		// Taking the slot before popping keeps waiting requests out of the processing list, so recovery only counts started attempts.
		semaphore <- struct{}{}

		rawData, err := redisClient.BRPopLPush(ctx, gdprrelay.KeyPending, keyProcessing, 0).Result()
		if err != nil {
			<-semaphore
			if err == redis.Nil {
				continue
			}
			logger.Error("Failed to read from GDPR queue",
				zap.Error(err),
				zap.String("key", gdprrelay.KeyPending),
			)
			time.Sleep(5 * time.Second)
			continue
		}

		var queued gdprrelay.QueuedRequest
		if err := json.Unmarshal([]byte(rawData), &queued); err != nil {
			<-semaphore
			logger.Error("Failed to unmarshal GDPR request",
				zap.Error(err),
				zap.String("raw_data", rawData),
			)
			redisClient.LRem(ctx, keyProcessing, 1, rawData)
			continue
		}

		queued.LastAttemptAt = time.Now()

		logger.Info("Dequeued GDPR request",
			zap.String("scrambled_user_id", utils.ScrambleUserId(queued.Request.UserId)),
			zap.String("request_type", gdprrelay.LogNames[queued.Request.Type]),
			zap.Int("request_id", queued.RequestID),
			zap.Int("retry_count", queued.RetryCount),
		)

		ch <- queued
	}
}

func Acknowledge(ctx context.Context, redisClient *redis.Client, request gdprrelay.QueuedRequest, logger *zap.Logger) error {
	processingItems, err := redisClient.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to read processing queue: %w", err)
	}

	for _, item := range processingItems {
		var queued gdprrelay.QueuedRequest
		if err := json.Unmarshal([]byte(item), &queued); err != nil {
			continue
		}

		if queued.RequestID == request.RequestID {
			_, err := redisClient.LRem(ctx, keyProcessing, 1, item).Result()
			if err != nil {
				return fmt.Errorf("failed to remove from processing queue: %w", err)
			}
			return nil
		}
	}

	logger.Warn("Request not found in processing queue for acknowledgment",
		zap.String("scrambled_user_id", utils.ScrambleUserId(request.Request.UserId)),
		zap.String("request_type", gdprrelay.LogNames[request.Request.Type]),
		zap.Int("request_id", request.RequestID),
	)
	return nil
}

func Reject(ctx context.Context, redisClient *redis.Client, request gdprrelay.QueuedRequest, transcriptsDeleted, messagesDeleted int, logger *zap.Logger) (deadLettered bool, err error) {
	processingItems, err := redisClient.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return false, fmt.Errorf("failed to read processing queue: %w", err)
	}

	for _, item := range processingItems {
		var queued gdprrelay.QueuedRequest
		if jsonErr := json.Unmarshal([]byte(item), &queued); jsonErr != nil {
			continue
		}

		if queued.RequestID == request.RequestID {
			if _, removeErr := redisClient.LRem(ctx, keyProcessing, 1, item).Result(); removeErr != nil {
				logger.Error("Failed to remove from processing queue",
					zap.String("scrambled_user_id", utils.ScrambleUserId(queued.Request.UserId)),
					zap.Int("request_id", queued.RequestID),
					zap.Error(removeErr),
				)
				return false, removeErr
			}

			queued.RetryCount++
			queued.TranscriptsDeleted, queued.MessagesDeleted = transcriptsDeleted, messagesDeleted

			if queued.RetryCount >= config.Conf.MaxRetries {
				logger.Warn("GDPR request exceeded max retries",
					zap.String("scrambled_user_id", utils.ScrambleUserId(queued.Request.UserId)),
					zap.Int("request_id", queued.RequestID),
					zap.Int("retry_count", queued.RetryCount),
					zap.Error(err),
				)

				marshalled, _ := json.Marshal(queued)
				return true, redisClient.LPush(ctx, keyFailed, string(marshalled)).Err()
			}

			logger.Info("Requeuing failed GDPR request",
				zap.String("scrambled_user_id", utils.ScrambleUserId(queued.Request.UserId)),
				zap.Int("request_id", queued.RequestID),
				zap.Int("retry_count", queued.RetryCount),
				zap.Error(err),
			)

			// The request has already left the processing list, so a failed requeue is its last attempt.
			marshalled, marshalErr := json.Marshal(queued)
			if marshalErr != nil {
				return true, fmt.Errorf("failed to marshal queued request: %w", marshalErr)
			}

			if pushErr := redisClient.LPush(ctx, gdprrelay.KeyPending, string(marshalled)).Err(); pushErr != nil {
				return true, pushErr
			}

			return false, nil
		}
	}

	logger.Warn("Request not found in processing queue for rejection",
		zap.String("scrambled_user_id", utils.ScrambleUserId(request.Request.UserId)),
		zap.String("request_type", gdprrelay.LogNames[request.Request.Type]),
		zap.Int("request_id", request.RequestID),
	)
	return true, nil
}

func recoverStalledRequests(ctx context.Context, redisClient *redis.Client, logger *zap.Logger) error {
	processingItems, err := redisClient.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to read processing queue: %w", err)
	}

	if len(processingItems) == 0 {
		return nil
	}

	logger.Info("Recovering stalled requests", zap.Int("count", len(processingItems)))

	recovered := 0
	for i := len(processingItems) - 1; i >= 0; i-- {
		item := processingItems[i]

		var queued gdprrelay.QueuedRequest
		if err := json.Unmarshal([]byte(item), &queued); err != nil {
			logger.Error("Failed to unmarshal stalled request",
				zap.Error(err),
				zap.String("raw_data", item),
			)
			redisClient.LRem(ctx, keyProcessing, 1, item)
			continue
		}

		// The attempt that stalled counts, so a request that keeps crashing the worker runs out of retries.
		queued.RetryCount++

		logger.Info("Recovering stalled request",
			zap.Int("request_id", queued.RequestID),
			zap.Int("retry_count", queued.RetryCount),
		)

		marshalled, err := json.Marshal(queued)
		if err != nil {
			logger.Error("Failed to marshal stalled request",
				zap.Error(err),
				zap.Int("request_id", queued.RequestID),
			)
			continue
		}

		if err := redisClient.LPush(ctx, gdprrelay.KeyPending, string(marshalled)).Err(); err != nil {
			logger.Error("Failed to requeue stalled request",
				zap.Error(err),
				zap.Int("request_id", queued.RequestID),
			)
			continue
		}

		redisClient.LRem(ctx, keyProcessing, 1, item)
		recovered++
	}

	logger.Info("Stalled request recovery complete", zap.Int("recovered", recovered))

	return nil
}
