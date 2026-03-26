package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/encryption"
	"github.com/TicketsBot-cloud/gdl/cache"
	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/gdl/rest"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	"github.com/TicketsBot-cloud/logarchiver/pkg/model"
	v1 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v1"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/TicketsBot-cloud/logarchiver/pkg/export/user"
	"github.com/TicketsBot-cloud/logarchiver/pkg/s3client"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Processor handles the execution of GDPR data deletion and export requests
type Processor struct {
	logger      *zap.Logger
	rateLimiter *ratelimit.Ratelimiter
	s3Client    *s3client.S3Client
	aesKey      []byte
	cachePool   *pgxpool.Pool
}

func New(logger *zap.Logger, s3Client *s3client.S3Client, aesKey []byte, cachePool *pgxpool.Pool) *Processor {
	store := ratelimit.NewMemoryStore()
	return &Processor{
		logger:      logger,
		rateLimiter: ratelimit.NewRateLimiter(store, 0),
		s3Client:    s3Client,
		aesKey:      aesKey,
		cachePool:   cachePool,
	}
}

// ProcessResult contains the outcome of processing a GDPR request
type ProcessResult struct {
	TranscriptsDeleted int    // Number of transcript archives deleted from archiver
	MessagesDeleted    int    // Number of ticket messages deleted from database
	Error              error  // Error if the processing failed, nil on success
	ExportData         []byte // ZIP file bytes for export requests
	ExportFileName     string // Suggested filename for the export archive
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
	case gdprrelay.RequestTypeExportGuild:
		return p.processExportGuild(ctx, request)
	case gdprrelay.RequestTypeExportUser:
		return p.processExportUser(ctx, request)
	default:
		return ProcessResult{Error: fmt.Errorf("unknown GDPR request type: %d", request.Type)}
	}
}

func (p *Processor) verifyGuildOwnership(ctx context.Context, guildId, userId uint64) error {
	scrambledUserId := utils.ScrambleUserId(userId)

	if config.Conf.Discord.Token == "" {
		p.logger.Warn("Discord token not configured, skipping ownership verification",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Uint64("guild_id", guildId),
		)
		return nil
	}

	guild, err := rest.GetGuild(ctx, config.Conf.Discord.Token, p.rateLimiter, guildId)
	if err != nil {
		p.logger.Error("Failed to fetch guild for ownership verification",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Uint64("guild_id", guildId),
			zap.Error(err),
		)
		return fmt.Errorf("failed to verify guild ownership: unable to fetch guild information")
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
	requestTypeName := utils.GetRequestTypeName(int(request.Type))

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	transcriptsDeleted := 0
	var lastError error

	for _, guildId := range request.GuildIds {
		deleted, err := p.deleteAllTranscripts(ctx, guildId)
		if err != nil {
			lastError = err
			p.logger.Error("Failed to delete transcripts",
				zap.String("scrambled_user_id", scrambledUserId),
				zap.String("request_type", requestTypeName),
				zap.Error(err),
			)
			continue
		}
		transcriptsDeleted += deleted
	}

	if transcriptsDeleted > 0 {
		p.logger.Info("GDPR request completed",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.String("request_type", requestTypeName),
			zap.Int("transcripts_deleted", transcriptsDeleted),
		)
	}

	result := ProcessResult{
		TranscriptsDeleted: transcriptsDeleted,
	}

	if transcriptsDeleted == 0 && lastError != nil {
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
	scrambledUserId := utils.ScrambleUserId(request.UserId)
	requestTypeName := utils.GetRequestTypeName(int(request.Type))

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
		return ProcessResult{Error: fmt.Errorf("failed to delete specific transcripts: %w", err)}
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
	requestTypeName := utils.GetRequestTypeName(int(request.Type))

	messagesDeleted, err = p.deleteUserMessagesFromGuilds(ctx, request.GuildIds, request.UserId)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to delete all user messages: %w", err)}
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
	requestTypeName := utils.GetRequestTypeName(int(request.Type))

	messagesDeleted, err := p.deleteUserMessagesFromTickets(ctx, guildId, request.TicketIds, request.UserId)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to delete specific user messages: %w", err)}
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
func (p *Processor) deleteUserMessagesFromGuilds(ctx context.Context, guildIds []uint64, userId uint64) (messagesDeleted int, err error) {
	tickets, err := p.getUserTicketsInGuilds(ctx, userId, guildIds)
	if err != nil {
		return 0, err
	}
	return p.cleanUserMessagesInTickets(ctx, tickets, userId)
}

type ticketInfo struct {
	ID      int
	GuildID uint64
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

func (p *Processor) getUserTicketsInGuilds(ctx context.Context, userId uint64, guildIds []uint64) ([]ticketInfo, error) {
	query := `
	SELECT DISTINCT t.id, t.guild_id
	FROM tickets t
	LEFT JOIN ticket_members tm ON t.guild_id = tm.guild_id AND t.id = tm.ticket_id
	WHERE (tm.user_id = $1 OR t.user_id = $1)
	AND t.guild_id = ANY($2)
	AND t.open = false
	AND t.has_transcript = true
	ORDER BY t.id
	`

	rows, err := database.Client.Tickets.Query(ctx, query, userId, guildIds)
	if err != nil {
		return nil, fmt.Errorf("failed to query user tickets in guilds: %w", err)
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

// processExportGuild exports all transcript data for the specified guilds into a ZIP archive.
func (p *Processor) processExportGuild(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no server ID provided")}
	}

	if p.s3Client == nil {
		return ProcessResult{Error: fmt.Errorf("S3 client not configured, cannot process export")}
	}

	scrambledUserId := utils.ScrambleUserId(request.UserId)

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed for export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	zipBuilder := export.NewZipBuilder()

	for _, guildId := range request.GuildIds {
		keys, err := p.s3Client.GetAllKeysForGuild(ctx, guildId)
		if err != nil {
			p.logger.Error("Failed to list S3 keys for guild export",
				zap.Uint64("guild_id", guildId),
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Error(err),
			)
			continue
		}

		if len(keys) == 0 {
			p.logger.Info("No transcripts found for guild export",
				zap.Uint64("guild_id", guildId),
				zap.String("scrambled_user_id", scrambledUserId),
			)
			continue
		}

		// Use errgroup with bounded concurrency for parallel downloads
		type transcriptEntry struct {
			ticketId int
			data     []byte
		}

		var mu sync.Mutex
		var entries []transcriptEntry

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(15)

		for _, key := range keys {
			key := key
			g.Go(func() error {
				// Extract ticket ID from key format: {guildId}/{ticketId}
				parts := strings.SplitN(key, "/", 2)
				if len(parts) != 2 {
					p.logger.Warn("Skipping S3 key with unexpected format",
						zap.String("key", key),
						zap.Uint64("guild_id", guildId),
					)
					return nil
				}

				ticketId, err := strconv.Atoi(parts[1])
				if err != nil {
					p.logger.Warn("Skipping S3 key with non-numeric ticket ID",
						zap.String("key", key),
						zap.Uint64("guild_id", guildId),
						zap.Error(err),
					)
					return nil
				}

				rawData, err := p.s3Client.GetTicket(gCtx, guildId, ticketId)
				if err != nil {
					if errors.Is(err, s3client.ErrTicketNotFound) {
						p.logger.Debug("Transcript not found in S3, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
						)
						return nil
					}
					p.logger.Warn("Failed to download transcript from S3",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					return nil
				}

				decrypted, err := encryption.Decrypt(p.aesKey, rawData)
				if err != nil {
					p.logger.Warn("Failed to decrypt transcript, skipping",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					return nil
				}

				var transcript v2.Transcript
				version := model.GetVersion(decrypted)
				switch version {
				case model.V1:
					var messages []message.Message
					if err := json.Unmarshal(decrypted, &messages); err != nil {
						p.logger.Warn("Failed to unmarshal V1 transcript, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
							zap.Error(err),
						)
						return nil
					}
					transcript = v1.ConvertToV2(messages)
				case model.V2:
					if err := json.Unmarshal(decrypted, &transcript); err != nil {
						p.logger.Warn("Failed to unmarshal V2 transcript, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
							zap.Error(err),
						)
						return nil
					}
				default:
					p.logger.Warn("Unknown transcript version, skipping",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Int("version", version.Int()),
					)
					return nil
				}

				prettyData, err := json.MarshalIndent(transcript, "", "  ")
				if err != nil {
					p.logger.Warn("Failed to marshal transcript for export",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					return nil
				}

				mu.Lock()
				entries = append(entries, transcriptEntry{ticketId: ticketId, data: prettyData})
				mu.Unlock()

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			p.logger.Error("Error during parallel transcript download",
				zap.Uint64("guild_id", guildId),
				zap.Error(err),
			)
		}

		// Add all collected entries to the ZIP
		for _, entry := range entries {
			var fileName string
			if len(request.GuildIds) > 1 {
				fileName = fmt.Sprintf("%d/%d.json", guildId, entry.ticketId)
			} else {
				fileName = fmt.Sprintf("%d.json", entry.ticketId)
			}

			if err := zipBuilder.AddFile(fileName, entry.data); err != nil {
				p.logger.Error("Failed to add transcript to ZIP",
					zap.Uint64("guild_id", guildId),
					zap.Int("ticket_id", entry.ticketId),
					zap.Error(err),
				)
			}
		}
	}

	zipData, err := zipBuilder.Close()
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to finalise export ZIP: %w", err)}
	}

	var exportFileName string
	if len(request.GuildIds) == 1 {
		exportFileName = fmt.Sprintf("guild_%d.zip", request.GuildIds[0])
	} else {
		exportFileName = "guild_export.zip"
	}

	p.logger.Info("Guild export completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("export_file", exportFileName),
		zap.Int("zip_size_bytes", len(zipData)),
	)

	return ProcessResult{
		ExportData:     zipData,
		ExportFileName: exportFileName,
	}
}

// processExportUser exports all personal data for the requesting user into a ZIP archive.
func (p *Processor) processExportUser(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	scrambledUserId := utils.ScrambleUserId(request.UserId)

	zipBuilder := export.NewZipBuilder()

	// Export database data
	userData, err := user.GetUserData(database.Client, request.UserId)
	if err != nil {
		p.logger.Error("Failed to get user database data for export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: fmt.Errorf("failed to retrieve database data: %w", err)}
	}

	dbJson, err := json.MarshalIndent(userData, "", "  ")
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to serialise database data: %w", err)}
	}

	if err := zipBuilder.AddFile("database.json", dbJson); err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to add database.json to export: %w", err)}
	}

	// Export cache data if cache pool is available
	if p.cachePool != nil {
		pgCache := cache.NewPgCache(p.cachePool, cache.CacheOptions{
			Users:   true,
			Members: true,
		})

		cacheData, err := user.GetCacheData(&pgCache, request.UserId)
		if err != nil {
			p.logger.Warn("Failed to get user cache data for export, continuing without it",
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Error(err),
			)
		} else {
			cacheJson, err := json.MarshalIndent(cacheData, "", "  ")
			if err != nil {
				p.logger.Warn("Failed to serialise cache data",
					zap.String("scrambled_user_id", scrambledUserId),
					zap.Error(err),
				)
			} else {
				if err := zipBuilder.AddFile("cache.json", cacheJson); err != nil {
					p.logger.Warn("Failed to add cache.json to export",
						zap.String("scrambled_user_id", scrambledUserId),
						zap.Error(err),
					)
				}
			}
		}
	}

	// Collect all ticket IDs where the user participated or is the creator
	transcriptIds := make(map[uint64][]int)

	// Query tickets where user is a participant
	{
		query := `SELECT participant.guild_id, participant.ticket_id FROM participant INNER JOIN tickets ON participant.guild_id = tickets.guild_id AND tickets.id = participant.ticket_id WHERE participant.user_id = $1 AND tickets.has_transcript='t' AND tickets.open='f';`
		rows, err := database.Client.Participants.Query(ctx, query, request.UserId)
		if err != nil {
			p.logger.Error("Failed to query participant tickets for user export",
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Error(err),
			)
			return ProcessResult{Error: fmt.Errorf("failed to query participant tickets: %w", err)}
		}

		for rows.Next() {
			var guildId uint64
			var ticketId int
			if err := rows.Scan(&guildId, &ticketId); err != nil {
				p.logger.Warn("Failed to scan participant ticket row",
					zap.String("scrambled_user_id", scrambledUserId),
					zap.Error(err),
				)
				continue
			}
			transcriptIds[guildId] = append(transcriptIds[guildId], ticketId)
		}
		rows.Close()
	}

	// Query tickets where user is the creator
	{
		query := `SELECT guild_id, id FROM tickets WHERE user_id = $1 AND has_transcript='t' AND open='f';`
		rows, err := database.Client.Tickets.Query(ctx, query, request.UserId)
		if err != nil {
			p.logger.Error("Failed to query user-created tickets for export",
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Error(err),
			)
			return ProcessResult{Error: fmt.Errorf("failed to query user tickets: %w", err)}
		}

		for rows.Next() {
			var guildId uint64
			var ticketId int
			if err := rows.Scan(&guildId, &ticketId); err != nil {
				p.logger.Warn("Failed to scan user ticket row",
					zap.String("scrambled_user_id", scrambledUserId),
					zap.Error(err),
				)
				continue
			}
			transcriptIds[guildId] = append(transcriptIds[guildId], ticketId)
		}
		rows.Close()
	}

	// Deduplicate ticket IDs per guild
	for guildId, ticketIds := range transcriptIds {
		seen := make(map[int]struct{})
		deduped := make([]int, 0, len(ticketIds))
		for _, id := range ticketIds {
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				deduped = append(deduped, id)
			}
		}
		transcriptIds[guildId] = deduped
	}

	// Download, decrypt, filter and add transcripts to ZIP
	if p.s3Client != nil {
		for guildId, ticketIds := range transcriptIds {
			for _, ticketId := range ticketIds {
				rawData, err := p.s3Client.GetTicket(ctx, guildId, ticketId)
				if err != nil {
					if errors.Is(err, s3client.ErrTicketNotFound) {
						p.logger.Debug("Transcript not found for user export, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
						)
						continue
					}
					p.logger.Warn("Failed to download transcript for user export",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					continue
				}

				decrypted, err := encryption.Decrypt(p.aesKey, rawData)
				if err != nil {
					p.logger.Warn("Failed to decrypt transcript for user export, skipping",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					continue
				}

				var transcript v2.Transcript
				version := model.GetVersion(decrypted)
				switch version {
				case model.V1:
					var messages []message.Message
					if err := json.Unmarshal(decrypted, &messages); err != nil {
						p.logger.Warn("Failed to unmarshal V1 transcript for user export, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
							zap.Error(err),
						)
						continue
					}
					transcript = v1.ConvertToV2(messages)
				case model.V2:
					if err := json.Unmarshal(decrypted, &transcript); err != nil {
						p.logger.Warn("Failed to unmarshal V2 transcript for user export, skipping",
							zap.Uint64("guild_id", guildId),
							zap.Int("ticket_id", ticketId),
							zap.Error(err),
						)
						continue
					}
				default:
					p.logger.Warn("Unknown transcript version for user export, skipping",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Int("version", version.Int()),
					)
					continue
				}

				// Filter to only the user's data
				transcript.Entities.Channels = nil
				transcript.Entities.Roles = nil

				userEntity, ok := transcript.Entities.Users[request.UserId]
				if !ok {
					transcript.Entities.Users = nil
				} else {
					transcript.Entities.Users = map[uint64]v2.User{
						userEntity.Id: userEntity,
					}
				}

				var filteredMessages []v2.Message
				for _, msg := range transcript.Messages {
					if msg.AuthorId == request.UserId {
						filteredMessages = append(filteredMessages, msg)
					}
				}
				transcript.Messages = filteredMessages

				prettyData, err := json.MarshalIndent(transcript, "", "  ")
				if err != nil {
					p.logger.Warn("Failed to marshal filtered transcript for user export",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
					continue
				}

				fileName := fmt.Sprintf("transcripts/%d-%d.json", guildId, ticketId)
				if err := zipBuilder.AddFile(fileName, prettyData); err != nil {
					p.logger.Error("Failed to add transcript to user export ZIP",
						zap.Uint64("guild_id", guildId),
						zap.Int("ticket_id", ticketId),
						zap.Error(err),
					)
				}
			}
		}
	} else {
		p.logger.Warn("S3 client not configured, skipping transcript export for user",
			zap.String("scrambled_user_id", scrambledUserId),
		)
	}

	zipData, err := zipBuilder.Close()
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to finalise user export ZIP: %w", err)}
	}

	exportFileName := fmt.Sprintf("user_%d.zip", request.UserId)

	p.logger.Info("User export completed",
		zap.String("scrambled_user_id", scrambledUserId),
		zap.String("export_file", exportFileName),
		zap.Int("zip_size_bytes", len(zipData)),
	)

	return ProcessResult{
		ExportData:     zipData,
		ExportFileName: exportFileName,
	}
}
