package gdprrelay

import (
	"context"
	"encoding/json"
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
	InteractionToken   string      `json:"interaction_token,omitempty"`
	InteractionGuildId uint64      `json:"interaction_guild_id,omitempty"`
	ApplicationId      uint64      `json:"application_id,omitempty"`
}

const key = "tickets:gdpr"

func Publish(redisClient *redis.Client, data GDPRRequest) error {
	marshalled, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return redisClient.RPush(context.Background(), key, string(marshalled)).Err()
}

func Listen(redisClient *redis.Client, ch chan GDPRRequest, logger *zap.Logger) {
	for {
		res, err := redisClient.BLPop(context.Background(), 0, key).Result()
		if err != nil {
			logger.Error("Failed to read from GDPR queue",
				zap.Error(err),
				zap.String("key", key),
			)
			// Wait before retrying to avoid tight loop on persistent errors
			time.Sleep(5 * time.Second)
			continue
		}

		if len(res) < 2 {
			logger.Error("Invalid BLPop result - expected 2 elements",
				zap.Int("length", len(res)),
			)
			continue
		}

		var data GDPRRequest
		if err := json.Unmarshal([]byte(res[1]), &data); err != nil {
			logger.Error("Failed to unmarshal GDPR request",
				zap.Error(err),
				zap.String("raw_data", res[1]),
			)
			continue
		}

		ch <- data
	}
}
