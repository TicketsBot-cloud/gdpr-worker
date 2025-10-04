package callback

import (
	"context"
	"fmt"
	"strings"

	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/gdl/objects/interaction/component"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"go.uber.org/zap"
)

type ResultData struct {
	TotalDeleted     int
	MessagesDeleted  int
	Error            error
	RequestType      gdprrelay.RequestType
	GuildIds         []uint64
	TicketIds        []int
}

type Callback struct {
	logger      *zap.Logger
	rateLimiter *ratelimit.Ratelimiter
}

func New(logger *zap.Logger, proxyUrl string) *Callback {
	store := ratelimit.NewMemoryStore()

	return &Callback{
		logger:      logger,
		rateLimiter: ratelimit.NewRateLimiter(store, 0),
	}
}

func (c *Callback) SendCompletion(ctx context.Context, request gdprrelay.GDPRRequest, result ResultData) error {
	if request.InteractionToken == "" {
		c.logger.Debug("No interaction token, skipping callback")
		return nil
	}

	components := c.buildResultComponents(result)

	if err := c.editOriginalMessage(ctx, request, components); err != nil {
		if c.isTokenExpired(err) {
			if dmErr := c.sendCompletionViaDM(ctx, request, result); dmErr != nil {
				c.logger.Error("Failed to send completion via DM",
					zap.Error(dmErr),
					zap.Uint64("user_id", request.UserId),
				)
				return dmErr
			}
			return nil
		}

		c.logger.Error("Failed to edit original message",
			zap.Error(err),
			zap.Uint64("user_id", request.UserId),
		)
		return err
	}

	if err := c.sendEphemeralFollowup(ctx, request, result); err != nil {
		if c.isTokenExpired(err) {
			return nil
		}

		c.logger.Error("Failed to send ephemeral follow-up",
			zap.Error(err),
			zap.Uint64("user_id", request.UserId),
		)
	}

	return nil
}

func (c *Callback) isTokenExpired(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "unknown webhook") ||
		strings.Contains(errStr, "unknown interaction") ||
		strings.Contains(errStr, "invalid webhook token") ||
		strings.Contains(errStr, "interaction has already been acknowledged") ||
		strings.Contains(errStr, "context deadline exceeded")
}

func (c *Callback) buildResultMessage(result ResultData) string {
	var content string

	switch result.RequestType {
	case gdprrelay.RequestTypeAllTranscripts:
		if len(result.GuildIds) == 1 {
			if result.TotalDeleted == 0 {
				content = fmt.Sprintf(
					"**Request Type:** Delete all transcripts from server\n"+
						"**Result:** No transcripts found",
				)
			} else {
				content = fmt.Sprintf(
					"**Request Type:** Delete all transcripts from server\n"+
						"**Transcripts Deleted:** %d",
					result.TotalDeleted,
				)
			}
		} else {
			if result.TotalDeleted == 0 {
				content = fmt.Sprintf(
					"**Request Type:** Delete all transcripts from servers\n"+
						"**Servers:** %d\n"+
						"**Result:** No transcripts found",
					len(result.GuildIds),
				)
			} else {
				content = fmt.Sprintf(
					"**Request Type:** Delete all transcripts from servers\n"+
						"**Servers:** %d\n"+
						"**Transcripts Deleted:** %d",
					len(result.GuildIds), result.TotalDeleted,
				)
			}
		}

	case gdprrelay.RequestTypeSpecificTranscripts:
		if result.TotalDeleted == 0 {
			content = fmt.Sprintf(
				"**Request Type:** Delete specific transcripts\n"+
					"**Result:** No transcripts found (they may have already been deleted)",
			)
		} else {
			content = fmt.Sprintf(
				"**Request Type:** Delete specific transcripts\n"+
					"**Transcripts Deleted:** %d",
				result.TotalDeleted,
			)
		}

	case gdprrelay.RequestTypeAllMessages:
		if result.MessagesDeleted == 0 {
			content = "**Request Type:** Delete all messages from your account\n" +
				"**Result:** No messages found in closed tickets"
		} else {
			content = fmt.Sprintf(
				"**Request Type:** Delete all messages from your account\n"+
					"**Messages Anonymized:** %d\n",
				result.MessagesDeleted,
			)
		}

	case gdprrelay.RequestTypeSpecificMessages:
		if result.MessagesDeleted == 0 {
			content = fmt.Sprintf(
				"**Request Type:** Delete messages in specific tickets\n"+
					"**Result:** No messages found (tickets may not exist or have no messages from you)",
			)
		} else {
			content = fmt.Sprintf(
				"**Request Type:** Delete messages in specific tickets\n"+
					"**Messages Anonymized:** %d\n",
				result.MessagesDeleted,
			)
		}
	}

	if result.Error != nil {
		content += fmt.Sprintf("\n\n**⚠️ Error:** %s", result.Error.Error())
	}

	return content
}

func (c *Callback) buildResultComponents(result ResultData) []component.Component {
	colour := utils.Green
	if result.Error != nil {
		colour = utils.Red
	} else if result.TotalDeleted == 0 && result.MessagesDeleted == 0 {
		colour = utils.Orange
	}

	innerComponents := []component.Component{
		component.BuildTextDisplay(component.TextDisplay{
			Content: c.buildResultMessage(result),
		}),
	}

	container := utils.BuildContainerWithComponents(colour, "GDPR Request Completed", innerComponents)
	return []component.Component{container}
}

func (c *Callback) editOriginalMessage(ctx context.Context, request gdprrelay.GDPRRequest, components []component.Component) error {
	data := rest.WebhookEditBody{
		Components: components,
		Flags:      uint(message.FlagComponentsV2),
	}

	_, err := rest.EditOriginalInteractionResponse(ctx, request.InteractionToken, c.rateLimiter, request.ApplicationId, data)
	return err
}

func (c *Callback) sendEphemeralFollowup(ctx context.Context, request gdprrelay.GDPRRequest, result ResultData) error {
	var content string

	if result.Error != nil {
		content = fmt.Sprintf("⚠️ Your GDPR request encountered an error: %s", result.Error.Error())
	} else if result.TotalDeleted == 0 && result.MessagesDeleted == 0 {
		content = "ℹ️ Your GDPR request has been processed. No data was found to delete."
	} else {
		content = "✅ Your GDPR request has been processed successfully."
	}

	data := rest.WebhookBody{
		Content: content,
		Flags:   uint(message.FlagEphemeral),
	}

	_, err := rest.CreateFollowupMessage(ctx, request.InteractionToken, c.rateLimiter, request.ApplicationId, data)
	return err
}

func (c *Callback) sendCompletionViaDM(ctx context.Context, request gdprrelay.GDPRRequest, result ResultData) error {
	if config.Conf.Discord.Token == "" {
		c.logger.Error("Discord token not configured, cannot send DM",
			zap.Uint64("user_id", request.UserId),
		)
		return fmt.Errorf("discord token not configured")
	}

	dmChannel, err := rest.CreateDM(ctx, config.Conf.Discord.Token, c.rateLimiter, request.UserId)
	if err != nil {
		c.logger.Error("Failed to create DM channel",
			zap.Error(err),
			zap.Uint64("user_id", request.UserId),
		)
		return fmt.Errorf("failed to create DM channel: %w", err)
	}

	components := c.buildResultComponents(result)

	data := rest.CreateMessageData{
		Components: components,
		Flags:      uint(message.FlagComponentsV2),
	}

	_, err = rest.CreateMessage(ctx, config.Conf.Discord.Token, c.rateLimiter, dmChannel.Id, data)
	if err != nil {
		c.logger.Error("Failed to send DM message",
			zap.Error(err),
			zap.Uint64("user_id", request.UserId),
			zap.Uint64("channel_id", dmChannel.Id),
		)
		return fmt.Errorf("failed to send DM message: %w", err)
	}

	return nil
}
