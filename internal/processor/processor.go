package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"go.uber.org/zap"
)

type Processor struct {
	logger      *zap.Logger
	rateLimiter *ratelimit.Ratelimiter
}

func New(logger *zap.Logger) *Processor {
	store := ratelimit.NewMemoryStore()
	return &Processor{
		logger:      logger,
		rateLimiter: ratelimit.NewRateLimiter(store, 0),
	}
}

type ProcessResult struct {
	TotalDeleted    int
	MessagesDeleted int
	Error           error
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
	default:
		return ProcessResult{Error: fmt.Errorf("unknown GDPR request type: %d", request.Type)}
	}
}

func (p *Processor) verifyGuildOwnership(ctx context.Context, guildId, userId uint64) error {
	if config.Conf.Discord.Token == "" {
		p.logger.Warn("Discord token not configured, skipping ownership verification",
			zap.Uint64("guild_id", guildId),
			zap.Uint64("user_id", userId),
		)
		return nil
	}

	guild, err := rest.GetGuild(ctx, config.Conf.Discord.Token, p.rateLimiter, guildId)
	if err != nil {
		p.logger.Error("Failed to fetch guild for ownership verification",
			zap.Uint64("guild_id", guildId),
			zap.Uint64("user_id", userId),
			zap.Error(err),
		)
		return fmt.Errorf("failed to verify guild ownership: unable to fetch guild information")
	}

	if guild.OwnerId != userId {
		p.logger.Warn("Ownership verification failed",
			zap.Uint64("guild_id", guildId),
			zap.Uint64("user_id", userId),
			zap.Uint64("actual_owner_id", guild.OwnerId),
		)
		return fmt.Errorf("you are not the owner of this server (ID: %d)", guildId)
	}

	p.logger.Debug("Guild ownership verified",
		zap.Uint64("guild_id", guildId),
		zap.Uint64("user_id", userId),
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

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed",
			zap.Uint64("user_id", request.UserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	totalDeleted := 0
	var lastError error

	for _, guildId := range request.GuildIds {
		deleted, err := p.deleteAllTranscripts(ctx, guildId)
		if err != nil {
			lastError = err
			p.logger.Error("Failed to delete transcripts",
				zap.Uint64("guild_id", guildId),
				zap.Error(err),
			)
			continue
		}
		totalDeleted += deleted
	}

	if totalDeleted > 0 {
		p.logger.Info("GDPR request completed",
			zap.String("type", "all_transcripts"),
			zap.Uint64("user_id", request.UserId),
			zap.Int("total_deleted", totalDeleted),
			zap.Int("guilds", len(request.GuildIds)),
		)
	}

	result := ProcessResult{
		TotalDeleted: totalDeleted,
	}

	if totalDeleted == 0 && lastError != nil {
		result.Error = fmt.Errorf("failed to delete any transcripts: %w", lastError)
	}

	return result
}

func (p *Processor) processSpecificTranscripts(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no server ID provided")}
	}
	if len(request.TicketIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no ticket IDs provided")}
	}

	guildId := request.GuildIds[0]

	if err := p.verifyGuildOwnership(ctx, guildId, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed",
			zap.Uint64("guild_id", guildId),
			zap.Uint64("user_id", request.UserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	deleted, err := p.deleteSpecificTranscripts(ctx, guildId, request.TicketIds)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to delete specific transcripts: %w", err)}
	}

	p.logger.Info("GDPR request completed",
		zap.String("type", "specific_transcripts"),
		zap.Uint64("guild_id", guildId),
		zap.Uint64("user_id", request.UserId),
		zap.Int("deleted", deleted),
	)

	return ProcessResult{TotalDeleted: deleted}
}

func (p *Processor) processAllMessages(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	messagesDeleted, err := p.deleteAllUserMessages(ctx, request.UserId)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to delete all user messages: %w", err)}
	}

	p.logger.Info("GDPR request completed",
		zap.String("type", "all_messages"),
		zap.Uint64("user_id", request.UserId),
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
	messagesDeleted, err := p.deleteUserMessagesFromTickets(ctx, guildId, request.TicketIds, request.UserId)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to delete specific user messages: %w", err)}
	}

	p.logger.Info("GDPR request completed",
		zap.String("type", "specific_messages"),
		zap.Uint64("guild_id", guildId),
		zap.Uint64("user_id", request.UserId),
		zap.Int("messages_deleted", messagesDeleted),
	)

	return ProcessResult{
		MessagesDeleted: messagesDeleted,
	}
}

// Transcript deletion helpers

func (p *Processor) deleteAllTranscripts(ctx context.Context, guildId uint64) (int, error) {
	ticketIds, err := p.getTranscriptTicketIds(ctx, guildId, nil)
	if err != nil {
		return 0, err
	}
	return p.deleteTranscripts(ctx, guildId, ticketIds)
}

func (p *Processor) deleteSpecificTranscripts(ctx context.Context, guildId uint64, ticketIds []int) (int, error) {
	if len(ticketIds) == 0 {
		return 0, nil
	}

	validIds, err := p.getTranscriptTicketIds(ctx, guildId, ticketIds)
	if err != nil {
		return 0, err
	}
	return p.deleteTranscripts(ctx, guildId, validIds)
}

func (p *Processor) getTranscriptTicketIds(ctx context.Context, guildId uint64, filterIds []int) ([]int, error) {
	var query string
	var args []interface{}

	if filterIds == nil {
		query = `SELECT id FROM tickets WHERE guild_id = $1 AND has_transcript = true AND open = false`
		args = []interface{}{guildId}
	} else {
		query = `SELECT id FROM tickets WHERE guild_id = $1 AND id = ANY($2) AND has_transcript = true AND open = false`
		args = []interface{}{guildId, filterIds}
	}

	rows, err := database.Client.Tickets.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query tickets: %w", err)
	}
	defer rows.Close()

	var ticketIds []int
	for rows.Next() {
		var ticketId int
		if err := rows.Scan(&ticketId); err == nil {
			ticketIds = append(ticketIds, ticketId)
		}
	}

	return ticketIds, nil
}

func (p *Processor) deleteTranscripts(ctx context.Context, guildId uint64, ticketIds []int) (int, error) {
	deleted := 0
	for _, ticketId := range ticketIds {
		if err := p.deleteTranscript(ctx, guildId, ticketId); err == nil {
			deleted++
			if err := database.Client.Tickets.SetHasTranscript(ctx, guildId, ticketId, false); err != nil {
				p.logger.Error("Failed to update has_transcript flag after deletion",
					zap.Uint64("guild_id", guildId),
					zap.Int("ticket_id", ticketId),
					zap.Error(err),
				)
			}
		}
	}
	return deleted, nil
}

func (p *Processor) deleteTranscript(ctx context.Context, guildId uint64, ticketId int) error {
	if archiver.Proxy == nil {
		return fmt.Errorf("archiver proxy not initialized")
	}
	return archiver.Proxy.DeleteTicket(ctx, guildId, ticketId)
}

// Message deletion helpers

type ticketInfo struct {
	ID      int
	GuildID uint64
}

func (p *Processor) deleteAllUserMessages(ctx context.Context, userId uint64) (messagesDeleted int, err error) {
	tickets, err := p.getUserTickets(ctx, userId)
	if err != nil {
		return 0, err
	}
	return p.cleanUserMessagesInTickets(ctx, tickets, userId)
}

func (p *Processor) deleteUserMessagesFromTickets(ctx context.Context, guildId uint64, ticketIds []int, userId uint64) (messagesDeleted int, err error) {
	if len(ticketIds) == 0 {
		return 0, nil
	}

	tickets := make([]ticketInfo, 0, len(ticketIds))
	for _, id := range ticketIds {
		tickets = append(tickets, ticketInfo{ID: id, GuildID: guildId})
	}

	validTickets := p.validateTicketsForMessageCleaning(ctx, tickets)

	return p.cleanUserMessagesInTickets(ctx, validTickets, userId)
}

func (p *Processor) getUserTickets(ctx context.Context, userId uint64) ([]ticketInfo, error) {
	query := `
	SELECT DISTINCT t.id, t.guild_id
	FROM tickets t
	LEFT JOIN ticket_members tm ON t.guild_id = tm.guild_id AND t.id = tm.ticket_id
	WHERE (tm.user_id = $1 OR t.user_id = $1)
	AND t.open = false
	AND t.has_transcript = true
	ORDER BY t.id
	`

	rows, err := database.Client.Tickets.Query(ctx, query, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to query user tickets: %w", err)
	}
	defer rows.Close()

	var tickets []ticketInfo
	for rows.Next() {
		var ticket ticketInfo
		if err := rows.Scan(&ticket.ID, &ticket.GuildID); err == nil {
			tickets = append(tickets, ticket)
		}
	}

	return tickets, nil
}

func (p *Processor) validateTicketsForMessageCleaning(ctx context.Context, tickets []ticketInfo) []ticketInfo {
	if len(tickets) == 0 {
		return nil
	}

	// Group tickets by guild_id for efficient querying
	ticketsByGuild := make(map[uint64][]int)
	ticketMap := make(map[uint64]map[int]ticketInfo)

	for _, ticket := range tickets {
		ticketsByGuild[ticket.GuildID] = append(ticketsByGuild[ticket.GuildID], ticket.ID)
		if ticketMap[ticket.GuildID] == nil {
			ticketMap[ticket.GuildID] = make(map[int]ticketInfo)
		}
		ticketMap[ticket.GuildID][ticket.ID] = ticket
	}

	var validTickets []ticketInfo

	// Query each guild's tickets in a single query
	for guildId, ticketIds := range ticketsByGuild {
		query := `SELECT id FROM tickets WHERE guild_id = $1 AND id = ANY($2) AND open = false AND has_transcript = true`
		rows, err := database.Client.Tickets.Query(ctx, query, guildId, ticketIds)
		if err != nil {
			p.logger.Error("Failed to validate tickets for message cleaning",
				zap.Uint64("guild_id", guildId),
				zap.Error(err),
			)
			continue
		}

		for rows.Next() {
			var ticketId int
			if err := rows.Scan(&ticketId); err == nil {
				if ticket, exists := ticketMap[guildId][ticketId]; exists {
					validTickets = append(validTickets, ticket)
				}
			}
		}
		rows.Close()
	}

	return validTickets
}

func (p *Processor) cleanUserMessagesInTickets(ctx context.Context, tickets []ticketInfo, userId uint64) (messagesDeleted int, err error) {
	var lastErr error
	for _, ticket := range tickets {
		count, err := p.cleanUserMessages(ctx, ticket.GuildID, ticket.ID, userId)
		if err != nil {
			lastErr = err
			continue
		}
		if count > 0 {
			messagesDeleted += count
		}
	}

	if messagesDeleted == 0 && lastErr != nil {
		return 0, lastErr
	}

	return messagesDeleted, nil
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
		return 0, err
	}

	count := p.cleanMessagesInTranscript(&transcript, userId)
	if count == 0 {
		return 0, nil
	}

	if err := p.storeTranscript(ctx, guildId, ticketId, transcript); err != nil {
		return 0, fmt.Errorf("failed to store cleaned transcript: %w", err)
	}

	if err := database.Client.Tickets.SetHasTranscript(ctx, guildId, ticketId, true); err != nil {
		p.logger.Error("Failed to update has_transcript flag after message cleaning",
			zap.Uint64("guild_id", guildId),
			zap.Int("ticket_id", ticketId),
			zap.Error(err),
		)
		// Don't return error - the transcript was successfully cleaned and stored
	}

	return count, nil
}

func (p *Processor) getTranscript(ctx context.Context, guildId uint64, ticketId int) (v2.Transcript, error) {
	transcript, err := archiver.Client.Get(ctx, guildId, ticketId)
	if err != nil {
		if err == archiverclient.ErrNotFound {
			return v2.Transcript{}, fmt.Errorf("transcript not found")
		}
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

func (p *Processor) storeTranscript(ctx context.Context, guildId uint64, ticketId int, transcript v2.Transcript) error {
	data, err := json.Marshal(transcript)
	if err != nil {
		return fmt.Errorf("failed to serialize transcript: %w", err)
	}

	return archiver.Client.ImportTranscript(ctx, guildId, ticketId, data)
}
