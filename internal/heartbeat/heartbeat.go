package heartbeat

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const (
	HeartbeatKey      = "tickets:gdpr:worker:heartbeat" // Redis key for storing the heartbeat timestamp
	HeartbeatInterval = 10 * time.Second                 // How often to send heartbeat updates
	HeartbeatTTL      = 30 * time.Second                 // How long before the heartbeat expires if not refreshed
)

func Start(ctx context.Context, redisClient *redis.Client, logger *zap.Logger) {
	logger.Info("Starting heartbeat")

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	sendHeartbeat(ctx, redisClient, logger)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Heartbeat stopped")
			if err := redisClient.Del(context.Background(), HeartbeatKey).Err(); err != nil {
				logger.Error("Failed to clear heartbeat on shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			sendHeartbeat(ctx, redisClient, logger)
		}
	}
}

func sendHeartbeat(ctx context.Context, redisClient *redis.Client, logger *zap.Logger) {
	timestamp := time.Now().Unix()
	err := redisClient.Set(ctx, HeartbeatKey, timestamp, HeartbeatTTL).Err()
	if err != nil {
		logger.Error("Failed to send heartbeat", zap.Error(err))
	} else {
		logger.Debug("Heartbeat sent", zap.Int64("timestamp", timestamp))
	}
}

// Check verifies if the GDPR worker is alive
func Check(ctx context.Context, redisClient *redis.Client) (bool, error) {
	val, err := redisClient.Get(ctx, HeartbeatKey).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != "", nil
}
