package gdprrelay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type RequestType int

const (
	RequestTypeAllTranscripts RequestType = iota
	RequestTypeSpecificTranscripts
	RequestTypeAllMessages
	RequestTypeSpecificMessages
)

type GDPRRequest struct {
	Type               RequestType `json:"type"`
	UserId             uint64      `json:"user_id"`
	GuildIds           []uint64    `json:"guild_ids,omitempty"`
	TicketIds          []int       `json:"ticket_ids,omitempty"`
	Language           string      `json:"language,omitempty"`
	InteractionToken   string      `json:"interaction_token,omitempty"`
	InteractionGuildId uint64      `json:"interaction_guild_id,omitempty"`
	ApplicationId      uint64      `json:"application_id,omitempty"`
}

// QueuedRequest wraps a GDPR request with metadata for reliable queue processing
type QueuedRequest struct {
	Request       GDPRRequest `json:"request"`
	QueuedAt      time.Time   `json:"queued_at"`
	RetryCount    int         `json:"retry_count"`
	LastAttemptAt time.Time   `json:"last_attempt_at,omitempty"`
	RequestID     string      `json:"request_id"`
}

const (
	keyPending    = "tickets:gdpr:pending"
	keyProcessing = "tickets:gdpr:processing"
	keyFailed     = "tickets:gdpr:failed"
	maxRetries    = 3
)

func Listen(redisClient *redis.Client, ch chan GDPRRequest, logger *zap.Logger) {
	ctx := context.Background()

	if err := recoverStalledRequests(ctx, redisClient, logger); err != nil {
		logger.Error("Failed to recover stalled requests", zap.Error(err))
	}

	for {
		rawData, err := redisClient.BRPopLPush(ctx, keyPending, keyProcessing, 0).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			logger.Error("Failed to read from GDPR queue",
				zap.Error(err),
				zap.String("key", keyPending),
			)
			time.Sleep(5 * time.Second)
			continue
		}

		var queued QueuedRequest
		if err := json.Unmarshal([]byte(rawData), &queued); err != nil {
			logger.Error("Failed to unmarshal GDPR request",
				zap.Error(err),
				zap.String("raw_data", rawData),
			)
			redisClient.LRem(ctx, keyProcessing, 1, rawData)
			continue
		}

		queued.LastAttemptAt = time.Now()

		logger.Info("Dequeued GDPR request",
			zap.String("request_id", queued.RequestID),
			zap.Int("retry_count", queued.RetryCount),
			zap.Int("type", int(queued.Request.Type)),
			zap.Uint64("user_id", queued.Request.UserId),
		)

		ch <- queued.Request
	}
}

func Acknowledge(ctx context.Context, redisClient *redis.Client, request GDPRRequest, logger *zap.Logger) error {
	processingItems, err := redisClient.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to read processing queue: %w", err)
	}

	for _, item := range processingItems {
		var queued QueuedRequest
		if err := json.Unmarshal([]byte(item), &queued); err != nil {
			continue
		}

		if requestsMatch(queued.Request, request) {
			_, err := redisClient.LRem(ctx, keyProcessing, 1, item).Result()
			if err != nil {
				return fmt.Errorf("failed to remove from processing queue: %w", err)
			}
			return nil
		}
	}

	logger.Warn("Request not found in processing queue for acknowledgment",
		zap.Uint64("user_id", request.UserId),
		zap.Int("type", int(request.Type)),
	)
	return nil
}

func Reject(ctx context.Context, redisClient *redis.Client, request GDPRRequest, logger *zap.Logger) error {
	processingItems, err := redisClient.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to read processing queue: %w", err)
	}

	for _, item := range processingItems {
		var queued QueuedRequest
		if jsonErr := json.Unmarshal([]byte(item), &queued); jsonErr != nil {
			continue
		}

		if requestsMatch(queued.Request, request) {
			if _, removeErr := redisClient.LRem(ctx, keyProcessing, 1, item).Result(); removeErr != nil {
				logger.Error("Failed to remove from processing queue",
					zap.Error(removeErr),
					zap.String("request_id", queued.RequestID),
				)
				return removeErr
			}

			queued.RetryCount++

			if queued.RetryCount >= maxRetries {
				logger.Warn("GDPR request exceeded max retries",
					zap.String("request_id", queued.RequestID),
					zap.Int("retry_count", queued.RetryCount),
					zap.Error(err),
				)

				marshalled, _ := json.Marshal(queued)
				return redisClient.LPush(ctx, keyFailed, string(marshalled)).Err()
			}

			logger.Info("Requeuing failed GDPR request",
				zap.String("request_id", queued.RequestID),
				zap.Int("retry_count", queued.RetryCount),
				zap.Error(err),
			)

			marshalled, marshalErr := json.Marshal(queued)
			if marshalErr != nil {
				return fmt.Errorf("failed to marshal queued request: %w", marshalErr)
			}

			return redisClient.LPush(ctx, keyPending, string(marshalled)).Err()
		}
	}

	logger.Warn("Request not found in processing queue for rejection",
		zap.Uint64("user_id", request.UserId),
		zap.Int("type", int(request.Type)),
	)
	return nil
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

		var queued QueuedRequest
		if err := json.Unmarshal([]byte(item), &queued); err != nil {
			logger.Error("Failed to unmarshal stalled request",
				zap.Error(err),
				zap.String("raw_data", item),
			)
			redisClient.LRem(ctx, keyProcessing, 1, item)
			continue
		}

		logger.Info("Recovering stalled request",
			zap.String("request_id", queued.RequestID),
			zap.Int("retry_count", queued.RetryCount),
		)

		marshalled, err := json.Marshal(queued)
		if err != nil {
			logger.Error("Failed to marshal stalled request",
				zap.Error(err),
				zap.String("request_id", queued.RequestID),
			)
			continue
		}

		if err := redisClient.LPush(ctx, keyPending, string(marshalled)).Err(); err != nil {
			logger.Error("Failed to requeue stalled request",
				zap.Error(err),
				zap.String("request_id", queued.RequestID),
			)
			continue
		}

		redisClient.LRem(ctx, keyProcessing, 1, item)
		recovered++
	}

	logger.Info("Stalled request recovery complete", zap.Int("recovered", recovered))

	return nil
}

func requestsMatch(a, b GDPRRequest) bool {
	if a.Type != b.Type || a.UserId != b.UserId {
		return false
	}

	if len(a.GuildIds) != len(b.GuildIds) {
		return false
	}
	for i := range a.GuildIds {
		if a.GuildIds[i] != b.GuildIds[i] {
			return false
		}
	}

	if len(a.TicketIds) != len(b.TicketIds) {
		return false
	}
	for i := range a.TicketIds {
		if a.TicketIds[i] != b.TicketIds[i] {
			return false
		}
	}

	return true
}
