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
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"go.uber.org/zap"
)

// ResultData contains the result of a GDPR request to be sent back to the user
type ResultData struct {
	TranscriptsDeleted int                   // Number of transcript archives deleted
	MessagesDeleted    int                   // Number of ticket messages deleted
	Error              error                 // Error if the processing failed
	RequestType        gdprrelay.RequestType // Type of GDPR request that was processed
	GuildIds           []uint64              // Guild IDs affected by this request
	TicketIds          []int                 // Ticket IDs affected by this request
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

	scrambledUserId := utils.ScrambleUserId(request.UserId)
	locale := i18n.GetLocale(request.Language)
	components := c.buildResultComponents(locale, result, request.GuildNames)

	if err := c.editOriginalMessage(ctx, request, components); err != nil {
		if c.isTokenExpired(err) {
			if dmErr := c.sendCompletionViaDM(ctx, request, locale, result); dmErr != nil {
				c.logger.Error("Failed to send completion via DM",
					zap.Error(dmErr),
					zap.String("scrambled_user_id", scrambledUserId),
				)
				return dmErr
			}
			return nil
		}

		c.logger.Error("Failed to edit original message",
			zap.Error(err),
			zap.String("scrambled_user_id", scrambledUserId),
		)
		return err
	}

	if err := c.sendEphemeralFollowup(ctx, request, locale, result); err != nil {
		if c.isTokenExpired(err) {
			return nil
		}

		c.logger.Error("Failed to send ephemeral follow-up",
			zap.Error(err),
			zap.String("scrambled_user_id", scrambledUserId),
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

func (c *Callback) buildResultMessage(locale *i18n.Locale, result ResultData, guildNames map[uint64]string) string {
	var content string

	switch result.RequestType {
	case gdprrelay.RequestTypeAllTranscripts:
		if len(result.GuildIds) == 1 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscripts, guildDisplay, result.TranscriptsDeleted)
		} else {
			guildDisplays := make([]string, len(result.GuildIds))
			for i, guildId := range result.GuildIds {
				guildDisplays[i] = utils.FormatGuildDisplay(guildId, guildNames)
			}
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscriptsMulti, strings.Join(guildDisplays, "\n* "), result.TranscriptsDeleted)
		}

	case gdprrelay.RequestTypeSpecificTranscripts:
		if len(result.GuildIds) > 0 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificTranscripts, guildDisplay, result.TranscriptsDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificTranscripts, "Unknown", result.TranscriptsDeleted)
		}

	case gdprrelay.RequestTypeAllMessages:
		if len(result.GuildIds) == 1 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessages, guildDisplay, result.MessagesDeleted)
		} else {
			guildDisplays := make([]string, len(result.GuildIds))
			for i, guildId := range result.GuildIds {
				guildDisplays[i] = utils.FormatGuildDisplay(guildId, guildNames)
			}
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessagesMulti, strings.Join(guildDisplays, "\n* "), result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeSpecificMessages:
		if len(result.GuildIds) > 0 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, guildDisplay, result.MessagesDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, "Unknown", result.MessagesDeleted)
		}
	}

	if result.Error != nil {
		content = i18n.GetMessage(locale, i18n.GdprCompletedError, result.Error.Error())
	}

	return content
}

func (c *Callback) buildResultComponents(locale *i18n.Locale, result ResultData, guildNames map[uint64]string) []component.Component {
	colour := utils.Green
	if result.Error != nil {
		colour = utils.Red
	}

	innerComponents := []component.Component{
		component.BuildTextDisplay(component.TextDisplay{
			Content: c.buildResultMessage(locale, result, guildNames),
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
	} else if result.TranscriptsDeleted == 0 && result.MessagesDeleted == 0 {
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
	scrambledUserId := utils.ScrambleUserId(request.UserId)

	if config.Conf.Discord.Token == "" {
		c.logger.Error("Discord token not configured, cannot send DM",
			zap.String("scrambled_user_id", scrambledUserId),
		)
		return fmt.Errorf("discord token not configured")
	}

	dmChannel, err := rest.CreateDM(ctx, config.Conf.Discord.Token, c.rateLimiter, request.UserId)
	if err != nil {
		c.logger.Error("Failed to create DM channel",
			zap.Error(err),
			zap.String("scrambled_user_id", scrambledUserId),
		)
		return fmt.Errorf("failed to create DM channel: %w", err)
	}

	components := c.buildResultComponents(locale, result, request.GuildNames)

	data := rest.CreateMessageData{
		Components: components,
		Flags:      uint(message.FlagComponentsV2),
	}

	_, err = rest.CreateMessage(ctx, config.Conf.Discord.Token, c.rateLimiter, dmChannel.Id, data)
	if err != nil {
		c.logger.Error("Failed to send DM message",
			zap.Error(err),
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Uint64("channel_id", dmChannel.Id),
		)
		return fmt.Errorf("failed to send DM message: %w", err)
	}

	return nil
}
