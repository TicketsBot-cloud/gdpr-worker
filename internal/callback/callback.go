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
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
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

	locale := i18n.GetLocale(request.Language)
	components := c.buildResultComponents(locale, result)

	if err := c.editOriginalMessage(ctx, request, components); err != nil {
		if c.isTokenExpired(err) {
			if dmErr := c.sendCompletionViaDM(ctx, request, locale, result); dmErr != nil {
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

	if err := c.sendEphemeralFollowup(ctx, request, locale, result); err != nil {
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

func (c *Callback) buildResultMessage(locale *i18n.Locale, result ResultData) string {
	var content string

	switch result.RequestType {
	case gdprrelay.RequestTypeAllTranscripts:
		if result.TotalDeleted == 0 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedNoData)
		} else if len(result.GuildIds) == 1 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscripts, result.TotalDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscriptsMulti, len(result.GuildIds), result.TotalDeleted)
		}

	case gdprrelay.RequestTypeSpecificTranscripts:
		if result.TotalDeleted == 0 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedNoData)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificTranscripts, result.TotalDeleted)
		}

	case gdprrelay.RequestTypeAllMessages:
		if result.MessagesDeleted == 0 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedNoData)
		} else if len(result.GuildIds) == 1 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessages, result.MessagesDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessagesMulti, len(result.GuildIds), result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeSpecificMessages:
		if result.MessagesDeleted == 0 {
			content = i18n.GetMessage(locale, i18n.GdprCompletedNoData)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, result.MessagesDeleted)
		}
	}

	if result.Error != nil {
		content = i18n.GetMessage(locale, i18n.GdprCompletedError, result.Error.Error())
	}

	return content
}

func (c *Callback) buildResultComponents(locale *i18n.Locale, result ResultData) []component.Component {
	colour := utils.Green
	if result.Error != nil {
		colour = utils.Red
	}

	innerComponents := []component.Component{
		component.BuildTextDisplay(component.TextDisplay{
			Content: c.buildResultMessage(locale, result),
		}),
	}

	title := i18n.GetMessage(locale, i18n.GdprCompletedTitle)
	container := utils.BuildContainerWithComponents(colour, title, innerComponents)
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

func (c *Callback) sendEphemeralFollowup(ctx context.Context, request gdprrelay.GDPRRequest, locale *i18n.Locale, result ResultData) error {
	var content string

	if result.Error != nil {
		content = i18n.GetMessage(locale, i18n.GdprFollowupError, result.Error.Error())
	} else if result.TotalDeleted == 0 && result.MessagesDeleted == 0 {
		content = i18n.GetMessage(locale, i18n.GdprFollowupNoData)
	} else {
		content = i18n.GetMessage(locale, i18n.GdprFollowupSuccess)
	}

	data := rest.WebhookBody{
		Content: content,
		Flags:   uint(message.FlagEphemeral),
	}

	_, err := rest.CreateFollowupMessage(ctx, request.InteractionToken, c.rateLimiter, request.ApplicationId, data)
	return err
}

func (c *Callback) sendCompletionViaDM(ctx context.Context, request gdprrelay.GDPRRequest, locale *i18n.Locale, result ResultData) error {
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

	components := c.buildResultComponents(locale, result)

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
