package callback

import (
	"context"
	"fmt"
	"strings"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	commonutils "github.com/TicketsBot-cloud/common/utils"
	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/gdl/objects/interaction/component"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
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
	ExportedFiles      int
	ExportId           int64
}

func (r ResultData) isExport() bool {
	return r.RequestType == gdprrelay.RequestTypeExportGuild || r.RequestType == gdprrelay.RequestTypeExportUser
}

func (r ResultData) NotificationStatus() string {
	switch {
	case r.Error != nil:
		return gdprrelay.NotificationStatusFailed
	case r.isExport() && r.ExportedFiles == 0:
		return gdprrelay.NotificationStatusNoData
	default:
		return gdprrelay.NotificationStatusCompleted
	}
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
			if dmErr := c.sendCompletionViaDM(ctx, request, components); dmErr != nil {
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
			guildDisplay := commonutils.FormatGuildDisplay(result.GuildIds[0], guildNames[result.GuildIds[0]])
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscripts, guildDisplay, result.TranscriptsDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscriptsMulti, joinGuildDisplays(result.GuildIds, guildNames), result.TranscriptsDeleted)
		}

	case gdprrelay.RequestTypeSpecificTranscripts:
		if len(result.GuildIds) > 0 {
			guildDisplay := commonutils.FormatGuildDisplay(result.GuildIds[0], guildNames[result.GuildIds[0]])
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificTranscripts, guildDisplay, result.TranscriptsDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificTranscripts, "Unknown", result.TranscriptsDeleted)
		}

	case gdprrelay.RequestTypeAllMessages:
		if len(result.GuildIds) == 1 {
			guildDisplay := commonutils.FormatGuildDisplay(result.GuildIds[0], guildNames[result.GuildIds[0]])
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessages, guildDisplay, result.MessagesDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessagesMulti, joinGuildDisplays(result.GuildIds, guildNames), result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeSpecificMessages:
		if len(result.GuildIds) > 0 {
			guildDisplay := commonutils.FormatGuildDisplay(result.GuildIds[0], guildNames[result.GuildIds[0]])
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, guildDisplay, result.MessagesDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, "Unknown", result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeExportGuild:
		if len(result.GuildIds) == 1 {
			guildDisplay := commonutils.FormatGuildDisplay(result.GuildIds[0], guildNames[result.GuildIds[0]])
			content = i18n.GetMessage(locale, i18n.GdprCompletedExportGuild, guildDisplay)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedExportGuildMulti, joinGuildDisplays(result.GuildIds, guildNames))
		}

	case gdprrelay.RequestTypeExportUser:
		content = i18n.GetMessage(locale, i18n.GdprCompletedExportUser)
	}

	if result.Error != nil {
		content += i18n.GetMessage(locale, i18n.GdprCompletedError, result.Error.Error())
	} else if result.isExport() {
		if result.ExportedFiles == 0 {
			content += i18n.GetMessage(locale, i18n.GdprCompletedExportNoData)
		} else {
			content += i18n.GetMessage(locale, i18n.GdprCompletedExportReady)
		}
	}

	return content
}

func joinGuildDisplays(guildIds []uint64, guildNames map[uint64]string) string {
	displays := make([]string, len(guildIds))
	for i, guildId := range guildIds {
		displays[i] = commonutils.FormatGuildDisplay(guildId, guildNames[guildId])
	}

	return strings.Join(displays, "\n* ")
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

	if result.isExport() && result.Error == nil && result.ExportedFiles > 0 {
		url := fmt.Sprintf("%s/gdpr?export_id=%d", config.Conf.DashboardUrl, result.ExportId)
		innerComponents = append(innerComponents, component.BuildActionRow(component.BuildButton(component.Button{
			Label: i18n.GetMessage(locale, i18n.GdprCompletedDownloadButton),
			Style: component.ButtonStyleLink,
			Url:   &url,
		})))
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

	nothingFound := result.TranscriptsDeleted == 0 && result.MessagesDeleted == 0
	if result.isExport() {
		nothingFound = result.ExportedFiles == 0
	}

	if result.Error != nil {
		content = i18n.GetMessage(locale, i18n.GdprFollowupError, result.Error.Error())
	} else if nothingFound {
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

func (c *Callback) sendCompletionViaDM(ctx context.Context, request gdprrelay.GDPRRequest, components []component.Component) error {
	dmChannel, err := rest.CreateDM(ctx, config.Conf.Discord.Token, c.rateLimiter, request.UserId)
	if err != nil {
		return fmt.Errorf("failed to create DM channel: %w", err)
	}

	data := rest.CreateMessageData{
		Components: components,
		Flags:      uint(message.FlagComponentsV2),
	}

	_, err = rest.CreateMessage(ctx, config.Conf.Discord.Token, c.rateLimiter, dmChannel.Id, data)
	if err != nil {
		return fmt.Errorf("failed to send DM message: %w", err)
	}

	return nil
}
