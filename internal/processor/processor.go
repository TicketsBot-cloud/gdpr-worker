package processor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdl/rest/request"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

var (
	errTranscriptDeletionFailed = errors.New("not every transcript could be deleted, please try again later")
	errAnonymizationFailed      = errors.New("not every message could be anonymized, please try again later")
)

// Processor handles the execution of GDPR data deletion and export requests
type Processor struct {
	logger      *zap.Logger
	rateLimiter *ratelimit.Ratelimiter
	db          export.Querier
	cachePool   *pgxpool.Pool
	store       *minio.Client
}

func New(logger *zap.Logger, cachePool *pgxpool.Pool, store *minio.Client) *Processor {
	return &Processor{
		logger:      logger,
		rateLimiter: ratelimit.NewRateLimiter(ratelimit.NewMemoryStore(), 0),
		db:          database.Pool,
		cachePool:   cachePool,
		store:       store,
	}
}

// ProcessResult contains the outcome of processing a GDPR request
type ProcessResult struct {
	TranscriptsDeleted int   // Number of transcript archives deleted from archiver
	MessagesDeleted    int   // Number of ticket messages deleted from database
	Error              error // Error if the processing failed, nil on success
	ExportedFiles      int
}

func (p *Processor) Process(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	switch request.Type {
	case gdprrelay.RequestTypeAllTranscripts:
		return p.processAllTranscripts(ctx, request)
	case gdprrelay.RequestTypeSpecificTranscripts:
		return p.processSpecificTranscripts(ctx, request)
	case gdprrelay.RequestTypeAllMessages:
		return p.processAllMessages(ctx, request)
	case gdprrelay.RequestTypeSpecificMessages:
		return p.processSpecificMessages(ctx, request)
	case gdprrelay.RequestTypeExportGuild, gdprrelay.RequestTypeExportUser:
		return p.processExport(ctx, request)
	default:
		return ProcessResult{Error: fmt.Errorf("unknown GDPR request type: %d", request.Type)}
	}
}

func (p *Processor) verifyGuildOwnership(ctx context.Context, guildId, userId uint64) error {
	scrambledUserId := utils.ScrambleUserId(userId)

	guild, err := rest.GetGuild(ctx, config.Conf.Discord.Token, p.rateLimiter, guildId)
	if err != nil {
		p.logger.Error("Failed to fetch guild for ownership verification",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Uint64("guild_id", guildId),
			zap.Error(err),
		)

		var restErr request.RestError
		if errors.As(err, &restErr) && (restErr.StatusCode == http.StatusForbidden || restErr.StatusCode == http.StatusNotFound) {
			return fmt.Errorf("the bot is not in server %d, so ownership could not be verified - add it back and try again", guildId)
		}
		return fmt.Errorf("ownership of server %d could not be verified right now, please try again later", guildId)
	}

	if guild.OwnerId != userId {
		p.logger.Warn("Ownership verification failed",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Uint64("guild_id", guildId),
			zap.String("scrambled_actual_owner_id", utils.ScrambleUserId(guild.OwnerId)),
		)
		return fmt.Errorf("you are not the owner of this server (ID: %d)", guildId)
	}

	p.logger.Debug("Guild ownership verified",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.Uint64("guild_id", guildId),
	)

	return nil
}

func (p *Processor) verifyAllGuildsOwnership(ctx context.Context, guildIds []uint64, userId uint64) error {
	for _, guildId := range guildIds {
		if err := p.verifyGuildOwnership(ctx, guildId, userId); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) processAllTranscripts(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("invalid server ID provided")}
	}

	scrambledUserId := utils.ScrambleUserId(request.UserId)
	requestTypeName := gdprrelay.LogNames[request.Type]

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	transcriptsDeleted := 0
	failed := false

	for _, guildId := range request.GuildIds {
		deleted, err := p.deleteAllTranscripts(ctx, guildId)
		transcriptsDeleted += deleted
		if err != nil {
			failed = true
			p.logger.Error("Failed to delete transcripts",
				zap.String("scrambled_user_id", scrambledUserId),
				zap.String("request_type", requestTypeName),
				zap.Uint64("guild_id", guildId),
				zap.Error(err),
			)
		}
	}

	if failed {
		return ProcessResult{TranscriptsDeleted: transcriptsDeleted, Error: errTranscriptDeletionFailed}
	}

	p.logger.Info("GDPR request completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("request_type", requestTypeName),
		zap.Int("transcripts_deleted", transcriptsDeleted),
	)

	return ProcessResult{TranscriptsDeleted: transcriptsDeleted}
}

func (p *Processor) processSpecificTranscripts(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no server ID provided")}
	}
	if len(request.TicketIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no ticket IDs provided")}
	}

	guildId := request.GuildIds[0]
	scrambledUserId := utils.ScrambleUserId(request.UserId)
	requestTypeName := gdprrelay.LogNames[request.Type]

	if err := p.verifyGuildOwnership(ctx, guildId, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.String("request_type", requestTypeName),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	transcriptsDeleted, err := p.deleteSpecificTranscripts(ctx, guildId, request.TicketIds)
	if err != nil {
		p.logger.Error("Failed to delete transcripts",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.String("request_type", requestTypeName),
			zap.Uint64("guild_id", guildId),
			zap.Error(err),
		)
		return ProcessResult{TranscriptsDeleted: transcriptsDeleted, Error: errTranscriptDeletionFailed}
	}

	p.logger.Info("GDPR request completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("request_type", requestTypeName),
		zap.Int("transcripts_deleted", transcriptsDeleted),
	)

	return ProcessResult{TranscriptsDeleted: transcriptsDeleted}
}

func (p *Processor) processAllMessages(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	var messagesDeleted int
	var err error

	scrambledUserId := utils.ScrambleUserId(request.UserId)
	requestTypeName := gdprrelay.LogNames[request.Type]

	messagesDeleted, err = p.deleteUserMessagesFromGuilds(ctx, request.GuildIds, request.UserId)
	if err != nil {
		p.logger.Error("Failed to anonymize messages",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.String("request_type", requestTypeName),
			zap.Error(err),
		)
		return ProcessResult{MessagesDeleted: messagesDeleted, Error: errAnonymizationFailed}
	}

	p.logger.Info("GDPR request completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("request_type", requestTypeName),
		zap.Int("messages_deleted", messagesDeleted),
	)

	return ProcessResult{
		MessagesDeleted: messagesDeleted,
	}
}

func (p *Processor) processSpecificMessages(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no guild ID provided")}
	}
	if len(request.TicketIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no ticket IDs provided")}
	}

	guildId := request.GuildIds[0]
	scrambledUserId := utils.ScrambleUserId(request.UserId)
	requestTypeName := gdprrelay.LogNames[request.Type]

	messagesDeleted, err := p.deleteUserMessagesFromTickets(ctx, guildId, request.TicketIds, request.UserId)
	if err != nil {
		p.logger.Error("Failed to anonymize messages",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.String("request_type", requestTypeName),
			zap.Error(err),
		)
		return ProcessResult{MessagesDeleted: messagesDeleted, Error: errAnonymizationFailed}
	}

	p.logger.Info("GDPR request completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("request_type", requestTypeName),
		zap.Int("messages_deleted", messagesDeleted),
	)

	return ProcessResult{
		MessagesDeleted: messagesDeleted,
	}
}

// Transcript deletion helpers
func (p *Processor) deleteAllTranscripts(ctx context.Context, guildId uint64) (int, error) {
	ticketIds, err := database.Client.Tickets.GetClosedWithTranscript(ctx, guildId)
	if err != nil {
		return 0, fmt.Errorf("failed to query tickets: %w", err)
	}
	return p.deleteTranscripts(ctx, guildId, ticketIds)
}

func (p *Processor) deleteSpecificTranscripts(ctx context.Context, guildId uint64, ticketIds []int) (int, error) {
	if len(ticketIds) == 0 {
		return 0, nil
	}

	validIds, err := database.Client.Tickets.FilterClosedWithTranscript(ctx, guildId, ticketIds)
	if err != nil {
		return 0, fmt.Errorf("failed to query tickets: %w", err)
	}
	return p.deleteTranscripts(ctx, guildId, validIds)
}

func (p *Processor) deleteTranscripts(ctx context.Context, guildId uint64, ticketIds []int) (int, error) {
	deleted := 0
	var lastErr error
	for _, ticketId := range ticketIds {
		if err := p.deleteTranscript(ctx, guildId, ticketId); err != nil {
			lastErr = fmt.Errorf("ticket %d: %w", ticketId, err)
			continue
		}

		deleted++
		if err := database.Client.Tickets.SetHasTranscript(ctx, guildId, ticketId, false); err != nil {
			p.logger.Error("Failed to update has_transcript flag after deletion",
				zap.Uint64("guild_id", guildId),
				zap.Int("ticket_id", ticketId),
				zap.Error(err),
			)
		}
	}

	if lastErr != nil {
		return deleted, fmt.Errorf("%d of %d transcripts could not be deleted: %w", len(ticketIds)-deleted, len(ticketIds), lastErr)
	}
	return deleted, nil
}

func (p *Processor) deleteTranscript(ctx context.Context, guildId uint64, ticketId int) error {
	if archiver.Proxy == nil {
		return fmt.Errorf("archiver proxy not initialized")
	}

	if err := archiver.Proxy.DeleteTicket(ctx, guildId, ticketId); err != nil && !errors.Is(err, archiverclient.ErrNotFound) {
		return err
	}
	return nil
}

// Message deletion helpers
func (p *Processor) deleteUserMessagesFromGuilds(ctx context.Context, guildIds []uint64, userId uint64) (messagesDeleted int, err error) {
	refs, err := database.Client.Tickets.GetUserTranscriptRefs(ctx, userId)
	if err != nil {
		return 0, fmt.Errorf("failed to query user tickets in guilds: %w", err)
	}
	return p.cleanUserMessagesInTickets(ctx, ticketsInGuilds(refs, guildIds), userId)
}

func (p *Processor) deleteUserMessagesFromTickets(ctx context.Context, guildId uint64, ticketIds []int, userId uint64) (messagesDeleted int, err error) {
	validIds, err := database.Client.Tickets.FilterClosedWithTranscript(ctx, guildId, ticketIds)
	if err != nil {
		return 0, fmt.Errorf("failed to query tickets: %w", err)
	}

	tickets := make([]dbmodel.TranscriptRef, len(validIds))
	for i, id := range validIds {
		tickets[i] = dbmodel.TranscriptRef{GuildId: guildId, TicketId: id}
	}

	return p.cleanUserMessagesInTickets(ctx, tickets, userId)
}

func ticketsInGuilds(refs []dbmodel.TranscriptRef, guildIds []uint64) []dbmodel.TranscriptRef {
	return slices.DeleteFunc(refs, func(ref dbmodel.TranscriptRef) bool {
		return !slices.Contains(guildIds, ref.GuildId)
	})
}

func (p *Processor) cleanUserMessagesInTickets(ctx context.Context, tickets []dbmodel.TranscriptRef, userId uint64) (messagesDeleted int, err error) {
	var lastErr error
	for _, ticket := range tickets {
		count, err := p.cleanUserMessages(ctx, ticket.GuildId, ticket.TicketId, userId)
		if err != nil {
			lastErr = fmt.Errorf("ticket %d in guild %d: %w", ticket.TicketId, ticket.GuildId, err)
			continue
		}
		messagesDeleted += count
	}

	return messagesDeleted, lastErr
}

func (p *Processor) cleanUserMessages(ctx context.Context, guildId uint64, ticketId int, userId uint64) (int, error) {
	if archiver.Client == nil {
		return 0, fmt.Errorf("archiver client not configured")
	}

	ticket, err := database.Client.Tickets.Get(ctx, ticketId, guildId)
	if err != nil {
		return 0, fmt.Errorf("ticket %d not found in guild %d", ticketId, guildId)
	}
	if !ticket.HasTranscript {
		return 0, nil
	}

	transcript, err := p.getTranscript(ctx, guildId, ticketId)
	if err != nil {
		if archiverclient.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}

	count := p.cleanMessagesInTranscript(&transcript, userId)
	if count == 0 {
		return 0, nil
	}

	if err := archiver.Client.StoreTranscript(ctx, guildId, ticketId, transcript); err != nil {
		return 0, fmt.Errorf("failed to store cleaned transcript: %w", err)
	}

	if err := database.Client.Tickets.SetHasTranscript(ctx, guildId, ticketId, true); err != nil {
		p.logger.Error("Failed to update has_transcript flag after message cleaning",
			zap.Uint64("guild_id", guildId),
			zap.Int("ticket_id", ticketId),
			zap.Error(err),
		)
	}

	return count, nil
}

func (p *Processor) getTranscript(ctx context.Context, guildId uint64, ticketId int) (v2.Transcript, error) {
	transcript, err := archiver.Client.Get(ctx, guildId, ticketId)
	if err != nil {
		if strings.Contains(err.Error(), "magic number") || strings.Contains(err.Error(), "invalid input") {
			return v2.Transcript{}, fmt.Errorf("transcript format incompatible with cleaning")
		}
		return v2.Transcript{}, fmt.Errorf("failed to retrieve transcript: %w", err)
	}
	return transcript, nil
}

func (p *Processor) cleanMessagesInTranscript(transcript *v2.Transcript, userId uint64) int {
	if transcript.Entities.Users == nil {
		transcript.Entities.Users = make(map[uint64]v2.User)
	}
	if transcript.Entities.Channels == nil {
		transcript.Entities.Channels = make(map[uint64]v2.Channel)
	}
	if transcript.Entities.Roles == nil {
		transcript.Entities.Roles = make(map[uint64]v2.Role)
	}

	anonymizedUser := v2.User{
		Id:       0,
		Username: "Removed for privacy",
		Avatar:   "",
		Bot:      false,
	}
	transcript.Entities.Users[userId] = anonymizedUser
	transcript.Entities.Users[0] = anonymizedUser

	count := 0
	for i, msg := range transcript.Messages {
		if msg.AuthorId == userId {
			count++
			msg.AuthorId = 0
			msg.Content = "[This message was removed in accordance with data protection regulations]"
			msg.Embeds = nil
			msg.Attachments = nil
			transcript.Messages[i] = msg
		}
	}

	return count
}
