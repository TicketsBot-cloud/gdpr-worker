package callback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/gdl/objects/interaction/component"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdl/rest/request"
	"github.com/TicketsBot-cloud/gdpr-worker/i18n"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"go.uber.org/zap"
)

const maxPartAttempts = 3

// Discord accepts at most ten attachments on a message.
const maxAttachmentsPerMessage = 10

// Fallback when the message budget is unset; Discord's request cap is 25 MiB.
const defaultMaxMessageBytes = 24 * 1024 * 1024

// ResultData contains the result of a GDPR request to be sent back to the user
type ResultData struct {
	TranscriptsDeleted int                   // Number of transcript archives deleted
	MessagesDeleted    int                   // Number of ticket messages deleted
	Error              error                 // Error if the processing failed
	RequestType        gdprrelay.RequestType // Type of GDPR request that was processed
	GuildIds           []uint64              // Guild IDs affected by this request
	TicketIds          []int                 // Ticket IDs affected by this request
	ExportParts        []export.Part         // Deliverable archives for export requests
	ExportedFiles      int                   // Number of files across those archives
}

func (r ResultData) isExport() bool {
	return r.RequestType == gdprrelay.RequestTypeExportGuild || r.RequestType == gdprrelay.RequestTypeExportUser
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

// The export is delivered before the interaction message is updated: that token expires after
// fifteen minutes, which is exactly the case a long export hits.
func (c *Callback) SendCompletion(ctx context.Context, req gdprrelay.GDPRRequest, result ResultData) error {
	scrambledUserId := utils.ScrambleUserId(req.UserId)
	locale := i18n.GetLocale(req.Language)

	var deliveryErr error
	delivered := 0

	if result.isExport() && result.Error == nil && len(result.ExportParts) > 0 {
		delivered, deliveryErr = c.sendExportViaDM(ctx, req, locale, result)
		if deliveryErr != nil {
			c.logger.Error("Failed to deliver export via DM",
				zap.Error(deliveryErr),
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Int("delivered_parts", delivered),
				zap.Int("total_parts", len(result.ExportParts)),
			)
		}
	}

	if req.InteractionToken == "" {
		c.logger.Debug("No interaction token, skipping callback")
		return deliveryErr
	}

	components := c.buildResultComponents(locale, result, req.GuildNames, deliveryErr, delivered)

	if err := c.editOriginalMessage(ctx, req, components); err != nil {
		if c.isTokenExpired(err) {
			if dmErr := c.sendCompletionViaDM(ctx, req, components); dmErr != nil {
				c.logger.Error("Failed to send completion via DM",
					zap.Error(dmErr),
					zap.String("scrambled_user_id", scrambledUserId),
				)
				return dmErr
			}
			return deliveryErr
		}

		c.logger.Error("Failed to edit original message",
			zap.Error(err),
			zap.String("scrambled_user_id", scrambledUserId),
		)
		return err
	}

	if err := c.sendEphemeralFollowup(ctx, req, locale, result); err != nil {
		if !c.isTokenExpired(err) {
			c.logger.Error("Failed to send ephemeral follow-up",
				zap.Error(err),
				zap.String("scrambled_user_id", scrambledUserId),
			)
		}
	}

	return deliveryErr
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
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllTranscriptsMulti, joinGuildDisplays(result.GuildIds, guildNames), result.TranscriptsDeleted)
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
			content = i18n.GetMessage(locale, i18n.GdprCompletedAllMessagesMulti, joinGuildDisplays(result.GuildIds, guildNames), result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeSpecificMessages:
		if len(result.GuildIds) > 0 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, guildDisplay, result.MessagesDeleted)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedSpecificMessages, "Unknown", result.MessagesDeleted)
		}

	case gdprrelay.RequestTypeExportGuild:
		if len(result.GuildIds) == 1 {
			guildDisplay := utils.FormatGuildDisplay(result.GuildIds[0], guildNames)
			content = i18n.GetMessage(locale, i18n.GdprCompletedExportGuild, guildDisplay, result.ExportedFiles)
		} else {
			content = i18n.GetMessage(locale, i18n.GdprCompletedExportGuildMulti, joinGuildDisplays(result.GuildIds, guildNames), result.ExportedFiles)
		}

	case gdprrelay.RequestTypeExportUser:
		content = i18n.GetMessage(locale, i18n.GdprCompletedExportUser, result.ExportedFiles)
	}

	if result.Error != nil {
		content = i18n.GetMessage(locale, i18n.GdprCompletedError, result.Error.Error())
	}

	return content
}

func joinGuildDisplays(guildIds []uint64, guildNames map[uint64]string) string {
	displays := make([]string, len(guildIds))
	for i, guildId := range guildIds {
		displays[i] = utils.FormatGuildDisplay(guildId, guildNames)
	}

	return strings.Join(displays, "\n* ")
}

func (c *Callback) buildResultComponents(locale *i18n.Locale, result ResultData, guildNames map[uint64]string, deliveryErr error, delivered int) []component.Component {
	colour := utils.Green
	content := c.buildResultMessage(locale, result, guildNames)

	if deliveryErr != nil {
		colour = utils.Red
		content = i18n.GetMessage(locale, i18n.GdprErrorExportDmFailed, delivered, len(result.ExportParts))
	} else if result.Error != nil {
		colour = utils.Red
	}

	innerComponents := []component.Component{
		component.BuildTextDisplay(component.TextDisplay{
			Content: content,
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

func (c *Callback) openDM(ctx context.Context, userId uint64) (uint64, error) {
	if config.Conf.Discord.Token == "" {
		return 0, fmt.Errorf("discord token not configured")
	}

	channel, err := rest.CreateDM(ctx, config.Conf.Discord.Token, c.rateLimiter, userId)
	if err != nil {
		return 0, fmt.Errorf("failed to create DM channel: %w", err)
	}

	return channel.Id, nil
}

func (c *Callback) sendCompletionViaDM(ctx context.Context, request gdprrelay.GDPRRequest, components []component.Component) error {
	channelId, err := c.openDM(ctx, request.UserId)
	if err != nil {
		return err
	}

	data := rest.CreateMessageData{
		Components: components,
		Flags:      uint(message.FlagComponentsV2),
	}

	if _, err := rest.CreateMessage(ctx, config.Conf.Discord.Token, c.rateLimiter, channelId, data); err != nil {
		return fmt.Errorf("failed to send DM message: %w", err)
	}

	return nil
}

// The byte budget binds before the attachment count does whenever a part approaches the message
// limit, so a message carries several attachments only when the parts are small.
func batchParts(parts []export.Part, maxBytes int) [][]export.Part {
	if maxBytes <= 0 {
		maxBytes = defaultMaxMessageBytes
	}

	var batches [][]export.Part
	var current []export.Part
	size := 0

	for _, part := range parts {
		tooMany := len(current) >= maxAttachmentsPerMessage
		tooBig := size+len(part.Data) > maxBytes

		if len(current) > 0 && (tooMany || tooBig) {
			batches = append(batches, current)
			current, size = nil, 0
		}

		current = append(current, part)
		size += len(part.Data)
	}

	if len(current) > 0 {
		batches = append(batches, current)
	}

	return batches
}

// Spaced out and retried per message so a rate limit delays delivery rather than dropping it.
// Returns how many archives reached the user.
func (c *Callback) sendExportViaDM(ctx context.Context, req gdprrelay.GDPRRequest, locale *i18n.Locale, result ResultData) (int, error) {
	scrambledUserId := utils.ScrambleUserId(req.UserId)

	channelId, err := c.openDM(ctx, req.UserId)
	if err != nil {
		return 0, err
	}

	batches := batchParts(result.ExportParts, config.Conf.Export.MaxMessageBytes)
	delivered := 0

	for i, batch := range batches {
		if i > 0 {
			if err := sleep(ctx, config.Conf.Export.DmDelay); err != nil {
				return delivered, err
			}
		}

		content := i18n.GetMessage(locale, i18n.GdprExportDmMessage)
		if len(batches) > 1 {
			content = i18n.GetMessage(locale, i18n.GdprExportDmMessagePart, i+1, len(batches))
		}

		if err := c.sendBatch(ctx, channelId, content, batch); err != nil {
			return delivered, fmt.Errorf("failed to send export message %d of %d: %w", i+1, len(batches), err)
		}

		delivered += len(batch)

		c.logger.Debug("Export message delivered",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Int("message", i+1),
			zap.Int("messages", len(batches)),
			zap.Int("attachments", len(batch)),
		)
	}

	c.logger.Info("Export delivered via DM",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.Int("parts", delivered),
		zap.Int("messages", len(batches)),
		zap.Int("files", result.ExportedFiles),
	)

	return delivered, nil
}

func (c *Callback) sendBatch(ctx context.Context, channelId uint64, content string, batch []export.Part) error {
	var lastErr error

	for attempt := 1; attempt <= maxPartAttempts; attempt++ {
		if attempt > 1 {
			backoff := time.Duration(attempt-1) * 2 * time.Second
			if retryAfter, ok := retryAfterOf(lastErr); ok {
				backoff = retryAfter
			}

			if err := sleep(ctx, backoff); err != nil {
				return err
			}
		}

		// Id must match the attachment's position: the encoder names the form field files[i].
		attachments := make([]request.Attachment, len(batch))
		for i, part := range batch {
			attachments[i] = request.Attachment{
				Id:       i,
				FileName: part.Name,
				File: request.File{
					ContentType: "application/zip",
					// Fresh reader per attempt, or a retry uploads nothing.
					Reader: bytes.NewReader(part.Data),
				},
			}
		}

		data := rest.CreateMessageData{
			Content:     content,
			Attachments: attachments,
		}

		_, err := rest.CreateMessage(ctx, config.Conf.Discord.Token, c.rateLimiter, channelId, data)
		if err == nil {
			return nil
		}

		lastErr = err

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return lastErr
}

func retryAfterOf(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}

	var restErr request.RestError
	if errors.As(err, &restErr) && restErr.StatusCode == 429 {
		return 5 * time.Second, true
	}

	if strings.Contains(strings.ToLower(err.Error()), "rate limit") {
		return 5 * time.Second, true
	}

	return 0, false
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
