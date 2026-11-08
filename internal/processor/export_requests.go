package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type transcriptRef struct {
	guildId  uint64
	ticketId int
}

type transcriptEntry struct {
	ref  transcriptRef
	name string
	data []byte
}

type archiveFile struct {
	name string
	data []byte
}

func (p *Processor) exportConcurrency() int {
	if config.Conf.Export.Concurrency > 0 {
		return config.Conf.Export.Concurrency
	}
	return 15
}

func transcriptPath(ref transcriptRef) string {
	return fmt.Sprintf("transcripts/%d/%d.json", ref.guildId, ref.ticketId)
}

// A part has to fit both Discord ceilings on its own, or it could never be delivered.
func maxPartBytes() int {
	size := config.Conf.Export.MaxPartBytes
	for _, limit := range []int{config.Conf.Export.MaxAttachmentBytes, config.Conf.Export.MaxMessageBytes} {
		if limit > 0 && limit < size {
			size = limit
		}
	}

	return size
}

func (p *Processor) processExportGuild(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	if len(request.GuildIds) == 0 {
		return ProcessResult{Error: fmt.Errorf("no server ID provided")}
	}

	scrambledUserId := utils.ScrambleUserId(request.UserId)

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed for export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: err}
	}

	manifest := &export.Manifest{
		Version:     export.ManifestVersion,
		Kind:        "guild",
		GeneratedAt: time.Now().UTC(),
	}

	var refs []transcriptRef
	for _, guildId := range request.GuildIds {
		found, err := p.transcriptRefsForGuild(ctx, guildId)
		if err != nil {
			p.logger.Error("Failed to query tickets for guild export",
				zap.Uint64("guild_id", guildId),
				zap.String("scrambled_user_id", scrambledUserId),
				zap.Error(err),
			)
			return ProcessResult{Error: fmt.Errorf("failed to list transcripts for server %d: %w", guildId, err)}
		}

		manifest.Add(fmt.Sprintf("transcripts/%d", guildId), len(found))
		refs = append(refs, found...)
	}

	baseName := "guild_export"
	if len(request.GuildIds) == 1 {
		baseName = fmt.Sprintf("guild_export_%d", request.GuildIds[0])
	}

	return p.buildTranscriptExport(ctx, exportJob{
		refs:            refs,
		baseName:        baseName,
		manifest:        manifest,
		scrambledUserId: scrambledUserId,
		logLabel:        "guild export",
	})
}

func (p *Processor) processExportUser(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	scrambledUserId := utils.ScrambleUserId(request.UserId)

	manifest := &export.Manifest{
		Version:     export.ManifestVersion,
		Kind:        "user",
		GeneratedAt: time.Now().UTC(),
	}

	userData, err := export.GetUserData(ctx, database.Pool, request.UserId, manifest)
	if err != nil {
		p.logger.Error("Failed to collect user database records for export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: fmt.Errorf("failed to retrieve database data: %w", err)}
	}

	dbJson, err := json.Marshal(userData)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to serialise database data: %w", err)}
	}

	extras := []archiveFile{{name: "database.json", data: dbJson}}

	if cacheJson := p.userCacheData(ctx, request.UserId, manifest); cacheJson != nil {
		extras = append(extras, archiveFile{name: "cache.json", data: cacheJson})
	}

	refs, err := p.transcriptRefsForUser(ctx, request.UserId)
	if err != nil {
		p.logger.Error("Failed to query tickets for user export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return ProcessResult{Error: fmt.Errorf("failed to query user tickets: %w", err)}
	}

	manifest.Add("transcripts", len(refs))

	return p.buildTranscriptExport(ctx, exportJob{
		refs:            refs,
		baseName:        "user_export",
		manifest:        manifest,
		scrambledUserId: scrambledUserId,
		logLabel:        "user export",
		extras:          extras,
		filterFor:       request.UserId,
	})
}

type exportJob struct {
	refs            []transcriptRef
	baseName        string
	manifest        *export.Manifest
	scrambledUserId string
	logLabel        string
	extras          []archiveFile
	// When non-zero, each transcript is reduced to that user's own messages.
	filterFor uint64
}

// Streamed into the archive as they arrive, so peak memory tracks the concurrency limit rather
// than the size of the whole export.
func (p *Processor) buildTranscriptExport(ctx context.Context, job exportJob) ProcessResult {
	builder := export.NewBuilder(maxPartBytes())

	for _, extra := range job.extras {
		if err := builder.Add(extra.name, extra.data); err != nil {
			return ProcessResult{Error: err}
		}
	}

	results := make(chan transcriptEntry, p.exportConcurrency())
	done := make(chan struct{})

	var downloadErr error
	var missing atomic.Int64

	go func() {
		defer close(done)

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(p.exportConcurrency())

		for _, ref := range job.refs {
			g.Go(func() error {
				entry, absent, err := p.fetchTranscript(gCtx, ref, job.filterFor)
				if err != nil {
					return err
				}

				if absent {
					missing.Add(1)
				}

				if entry == nil {
					return nil
				}

				select {
				case results <- *entry:
					return nil
				case <-gCtx.Done():
					return gCtx.Err()
				}
			})
		}

		downloadErr = g.Wait()
		close(results)
	}()

	var addErr error
	for entry := range results {
		if addErr != nil {
			continue
		}
		if err := builder.Add(entry.name, entry.data); err != nil {
			addErr = err
		}
	}

	<-done

	if downloadErr != nil {
		p.logger.Error("Transcript download failed during export",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
			zap.Error(downloadErr),
		)
		return ProcessResult{Error: fmt.Errorf("failed to download transcripts: %w", downloadErr)}
	}

	if addErr != nil {
		return ProcessResult{Error: addErr}
	}

	if absent := int(missing.Load()); absent > 0 {
		job.manifest.Withhold("transcripts.unavailable", fmt.Sprintf("%d recorded transcripts are no longer held by the archive store", absent))
		p.logger.Warn("Transcripts recorded in the database were absent from the archive store",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
			zap.Int("count", absent),
		)
	}

	if oversized := builder.Oversized(); len(oversized) > 0 {
		job.manifest.Files.Withheld = oversized
		p.logger.Warn("Entries too large to deliver were withheld from export",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.Int("count", len(oversized)),
		)
	}

	// Reported as no-data rather than delivering an empty archive.
	if builder.Count() == 0 {
		p.logger.Info("Export produced no files",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
		)
		return ProcessResult{}
	}

	job.manifest.Files.Written = builder.Count()

	if manifestJson, err := json.Marshal(job.manifest); err == nil {
		if err := builder.Add("manifest.json", manifestJson); err != nil {
			return ProcessResult{Error: err}
		}
	}

	parts, err := builder.Finish(job.baseName)
	if err != nil {
		return ProcessResult{Error: fmt.Errorf("failed to finalise export archive: %w", err)}
	}

	total := 0
	for _, part := range parts {
		total += len(part.Data)
	}

	p.logger.Info("Export completed",
		zap.String("scrambled_user_id", job.scrambledUserId),
		zap.String("export", job.logLabel),
		zap.Int("files", builder.Count()),
		zap.Int("parts", len(parts)),
		zap.Int("bytes", total),
	)

	return ProcessResult{
		ExportParts:   parts,
		ExportedFiles: builder.Count(),
	}
}

// Returns nil for a transcript that is absent or, when filtering, holds nothing of the
// requester's. The bool marks the absent case, which is ordinary rather than a failure: the object
// can be purged independently of the database row.
func (p *Processor) fetchTranscript(ctx context.Context, ref transcriptRef, filterFor uint64) (*transcriptEntry, bool, error) {
	transcript, err := archiver.Client.Get(ctx, ref.guildId, ref.ticketId)
	if err != nil {
		if isTranscriptMissing(err) {
			p.logger.Debug("Transcript absent from archive store, skipping",
				zap.Uint64("guild_id", ref.guildId),
				zap.Int("ticket_id", ref.ticketId),
				zap.Error(err),
			)
			return nil, true, nil
		}
		return nil, false, err
	}

	if filterFor != 0 {
		if !filterTranscriptToUser(&transcript, filterFor) {
			return nil, false, nil
		}
	}

	data, err := json.Marshal(transcript)
	if err != nil {
		return nil, false, fmt.Errorf("failed to serialise transcript %d/%d: %w", ref.guildId, ref.ticketId, err)
	}

	return &transcriptEntry{ref: ref, name: transcriptPath(ref), data: data}, false, nil
}

// The archiver proxy only maps a 404 to ErrNotFound; an object missing from the bucket surfaces as
// the storage backend's own wording behind another status, so the text has to be matched too.
func isTranscriptMissing(err error) bool {
	if errors.Is(err, archiverclient.ErrNotFound) {
		return true
	}

	msg := strings.ToLower(err.Error())
	for _, want := range []string{"does not exist", "no such key", "not found"} {
		if strings.Contains(msg, want) {
			return true
		}
	}

	return false
}

// Reports whether anything of the requester's remains.
func filterTranscriptToUser(transcript *v2.Transcript, userId uint64) bool {
	var messages []v2.Message
	for _, msg := range transcript.Messages {
		if msg.AuthorId == userId {
			messages = append(messages, msg)
		}
	}

	if len(messages) == 0 {
		return false
	}

	transcript.Messages = messages
	transcript.Entities.Channels = nil
	transcript.Entities.Roles = nil

	// Key by the requested id, not by the entity's own Id: once the anonymization flow has run the
	// stored entity carries Id 0, and keying by that would leave the messages unresolvable.
	entity := transcript.Entities.Users[userId]
	if entity.Id == 0 {
		entity.Id = userId
	}

	transcript.Entities.Users = map[uint64]v2.User{userId: entity}

	return true
}

func (p *Processor) transcriptRefsForGuild(ctx context.Context, guildId uint64) ([]transcriptRef, error) {
	// Open tickets are excluded: the transcript only reaches the archive store on close.
	query := `SELECT "id" FROM tickets WHERE "guild_id" = $1 AND "has_transcript" = true AND "open" = false ORDER BY "id";`

	rows, err := database.Client.Tickets.Query(ctx, query, guildId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []transcriptRef
	for rows.Next() {
		var ticketId int
		if err := rows.Scan(&ticketId); err != nil {
			return nil, err
		}
		refs = append(refs, transcriptRef{guildId: guildId, ticketId: ticketId})
	}

	return refs, rows.Err()
}

// The anonymization flow reads ticket_members and the previous export read participant; taking
// either alone silently drops transcripts the user appears in.
func (p *Processor) transcriptRefsForUser(ctx context.Context, userId uint64) ([]transcriptRef, error) {
	query := `
	SELECT s.guild_id, s.ticket_id
	FROM (
		SELECT "guild_id", "id" AS ticket_id FROM tickets WHERE "user_id" = $1
		UNION
		SELECT "guild_id", "ticket_id" FROM participant WHERE "user_id" = $1
		UNION
		SELECT "guild_id", "ticket_id" FROM ticket_members WHERE "user_id" = $1
	) s
	INNER JOIN tickets t ON t."guild_id" = s.guild_id AND t."id" = s.ticket_id
	WHERE t."has_transcript" = true AND t."open" = false
	ORDER BY s.guild_id, s.ticket_id;`

	rows, err := database.Client.Tickets.Query(ctx, query, userId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []transcriptRef
	for rows.Next() {
		var ref transcriptRef
		if err := rows.Scan(&ref.guildId, &ref.ticketId); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}

	return refs, rows.Err()
}

// Returns nil when the cache database is unconfigured or unreadable; the manifest records which.
func (p *Processor) userCacheData(ctx context.Context, userId uint64, manifest *export.Manifest) []byte {
	if p.cachePool == nil {
		manifest.Withhold("cache", "cache database not configured for this deployment")
		return nil
	}

	data := make(map[string]interface{})

	var user json.RawMessage
	if err := p.cachePool.QueryRow(ctx, `SELECT "data" FROM users WHERE "user_id" = $1;`, userId).Scan(&user); err == nil {
		data["user"] = user
	}

	members := make(map[string]json.RawMessage)

	rows, err := p.cachePool.Query(ctx, `SELECT "guild_id", "data" FROM members WHERE "user_id" = $1;`, userId)
	if err != nil {
		manifest.Withhold("cache", "cache database could not be read")
		p.logger.Warn("Failed to read cache member data for export",
			zap.String("scrambled_user_id", utils.ScrambleUserId(userId)),
			zap.Error(err),
		)
		return nil
	}

	defer rows.Close()

	for rows.Next() {
		var guildId uint64
		var raw json.RawMessage
		if err := rows.Scan(&guildId, &raw); err != nil {
			continue
		}
		members[fmt.Sprintf("%d", guildId)] = raw
	}

	data["member_data"] = members

	encoded, err := json.Marshal(data)
	if err != nil {
		manifest.Withhold("cache", "cache data could not be serialised")
		return nil
	}

	manifest.Add("cache", len(members))

	return encoded
}
