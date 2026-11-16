package processor

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/utils"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var errExportFailed = errors.New("the export file couldn't be created, please try again later")

type transcriptEntry struct {
	ref  dbmodel.TranscriptRef
	data []byte
}

type categoryFile struct {
	name    string
	key     string
	cat     export.Category
	arg     uint64
	guildId uint64
}

func (p *Processor) exportConcurrency() int {
	if config.Conf.Export.Concurrency > 0 {
		return config.Conf.Export.Concurrency
	}
	return 15
}

func transcriptPath(kind dbmodel.GdprExportType, ref dbmodel.TranscriptRef) string {
	if kind == dbmodel.GdprExportTypeServer {
		return fmt.Sprintf("%d/transcripts/%d.json", ref.GuildId, ref.TicketId)
	}
	return fmt.Sprintf("transcripts/%d/%d.json", ref.GuildId, ref.TicketId)
}

func exportCategories(kind dbmodel.GdprExportType, request gdprrelay.GDPRRequest) ([]string, error) {
	categories, ok := kind.NormaliseCategories(request.Categories)
	if !ok {
		return nil, fmt.Errorf("invalid export categories: %q", request.Categories)
	}

	if len(request.TicketIds) > 0 && (!slices.Contains(categories, dbmodel.GdprExportCategoryTranscripts) || len(request.GuildIds) != 1) {
		return nil, fmt.Errorf("specific ticket IDs need the transcripts category and exactly one server")
	}

	return categories, nil
}

func guildTranscriptIds(ctx context.Context, guildId uint64, ticketIds []int) ([]int, error) {
	if len(ticketIds) > 0 {
		return database.Client.Tickets.FilterClosedWithTranscript(ctx, guildId, ticketIds)
	}
	return database.Client.Tickets.GetClosedWithTranscript(ctx, guildId)
}

func (p *Processor) guildExportJob(ctx context.Context, request gdprrelay.GDPRRequest) (*exportJob, error) {
	if len(request.GuildIds) == 0 {
		return nil, fmt.Errorf("no server ID provided")
	}

	categories, err := exportCategories(dbmodel.GdprExportTypeServer, request)
	if err != nil {
		return nil, err
	}

	scrambledUserId := utils.ScrambleUserId(request.UserId)

	if err := p.verifyAllGuildsOwnership(ctx, request.GuildIds, request.UserId); err != nil {
		p.logger.Error("Guild ownership verification failed for export",
			zap.String("scrambled_user_id", scrambledUserId),
			zap.Error(err),
		)
		return nil, err
	}

	job := &exportJob{
		manifest:        export.NewManifest(dbmodel.GdprExportTypeServer, request.ExportId, request.UserId, request.GuildIds, categories, request.TicketIds),
		scrambledUserId: scrambledUserId,
		logLabel:        "guild export",
	}

	for _, guildId := range request.GuildIds {
		for _, key := range categories {
			if key != dbmodel.GdprExportCategoryTranscripts {
				job.files = append(job.files, categoryFile{fmt.Sprintf("%d/%s.json", guildId, key), key, export.ServerCategories[key], guildId, guildId})
				continue
			}

			ticketIds, err := guildTranscriptIds(ctx, guildId, request.TicketIds)
			if err != nil {
				p.logger.Error("Failed to query tickets for guild export",
					zap.Uint64("guild_id", guildId),
					zap.String("scrambled_user_id", scrambledUserId),
					zap.Error(err),
				)
				return nil, errExportFailed
			}

			job.transcriptGuilds = append(job.transcriptGuilds, guildId)
			for _, ticketId := range ticketIds {
				job.refs = append(job.refs, dbmodel.TranscriptRef{GuildId: guildId, TicketId: ticketId})
			}
		}
	}

	return job, nil
}

func (p *Processor) userExportJob(ctx context.Context, request gdprrelay.GDPRRequest) (*exportJob, error) {
	categories, err := exportCategories(dbmodel.GdprExportTypeUser, request)
	if err != nil {
		return nil, err
	}

	scrambledUserId := utils.ScrambleUserId(request.UserId)

	job := &exportJob{
		manifest:        export.NewManifest(dbmodel.GdprExportTypeUser, request.ExportId, request.UserId, request.GuildIds, categories, request.TicketIds),
		scrambledUserId: scrambledUserId,
		logLabel:        "user export",
		filterFor:       request.UserId,
	}

	for _, key := range categories {
		switch key {
		case export.CategoryDiscordData:
			job.discordData = true

		case dbmodel.GdprExportCategoryTranscripts:
			refs, err := database.Client.Tickets.GetUserTranscriptRefs(ctx, request.UserId)
			if err != nil {
				p.logger.Error("Failed to query tickets for user export",
					zap.String("scrambled_user_id", scrambledUserId),
					zap.Error(err),
				)
				return nil, errExportFailed
			}

			if len(request.TicketIds) > 0 {
				refs = slices.DeleteFunc(refs, func(ref dbmodel.TranscriptRef) bool {
					return ref.GuildId != request.GuildIds[0] || !slices.Contains(request.TicketIds, ref.TicketId)
				})
			}

			job.refs = refs
			job.transcriptGuilds = []uint64{0}

		default:
			cat := export.UserCategories[key]
			if key == "affiliate" && p.reviewsFlaggedReferrals(ctx, request.UserId) {
				cat = export.AffiliateCategory(true)
			}

			job.files = append(job.files, categoryFile{key + ".json", key, cat, request.UserId, 0})
		}
	}

	return job, nil
}

// Same audience as the dashboard's flagged-referrals view (/admin/affiliate/flagged): the bot owner and admin-tier bot staff.
func (p *Processor) reviewsFlaggedReferrals(ctx context.Context, userId uint64) bool {
	if userId == config.Conf.Owner {
		return true
	}

	rows, err := p.db.Query(ctx, `SELECT "tier" FROM bot_staff WHERE "user_id" = $1`, userId)
	if err != nil {
		return false
	}
	defer rows.Close()

	var tier string
	return rows.Next() && rows.Scan(&tier) == nil && tier == string(dbmodel.BotStaffTierAdmin)
}

type exportJob struct {
	refs            []dbmodel.TranscriptRef
	manifest        *export.Manifest
	scrambledUserId string
	logLabel        string
	files           []categoryFile
	discordData     bool
	// Guild 0 stands for every guild at once.
	transcriptGuilds []uint64
	// When non-zero, each transcript is reduced to that user's own messages.
	filterFor uint64
}

func (p *Processor) processExport(ctx context.Context, request gdprrelay.GDPRRequest) ProcessResult {
	ctx, cancel := context.WithTimeout(ctx, config.Conf.Export.Timeout)
	defer cancel()

	if request.ExportId == 0 {
		return ProcessResult{Error: fmt.Errorf("export request has no export ID")}
	}

	build := p.userExportJob
	if request.Type == gdprrelay.RequestTypeExportGuild {
		build = p.guildExportJob
	}

	job, err := build(ctx, request)
	if err != nil {
		return ProcessResult{Error: err}
	}

	bucket := config.Conf.ExportS3.Bucket
	key := export.ObjectKey(job.manifest.Kind, request.ExportId)

	if err := export.Upload(ctx, p.store, bucket, key, func(ctx context.Context, zw *zip.Writer) error {
		return p.writeArchive(ctx, zw, job)
	}); err != nil {
		p.logger.Error("Failed to upload export",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.Int64("export_id", request.ExportId),
			zap.Error(err),
		)
		return ProcessResult{Error: errExportFailed}
	}

	if !slices.ContainsFunc(job.manifest.Sources, func(source export.ManifestSource) bool { return source.Records > 0 }) {
		if err := p.store.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil && minio.ToErrorResponse(err).Code != minio.NoSuchKey {
			p.logger.Warn("Failed to remove empty export",
				zap.String("scrambled_user_id", job.scrambledUserId),
				zap.Int64("export_id", request.ExportId),
				zap.Error(err),
			)
		}

		if err := database.Client.GdprExports.SetStatus(ctx, request.ExportId, dbmodel.GdprExportStatusNoData); err != nil {
			p.logger.Error("Failed to record empty export",
				zap.String("scrambled_user_id", job.scrambledUserId),
				zap.Int64("export_id", request.ExportId),
				zap.Error(err),
			)
			return ProcessResult{Error: errExportFailed}
		}

		p.logger.Info("Export found no data",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
		)
		return ProcessResult{}
	}

	if err := database.Client.GdprExports.SetReady(ctx, request.ExportId, key); err != nil {
		p.logger.Error("Failed to mark export ready",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.Int64("export_id", request.ExportId),
			zap.Error(err),
		)
		return ProcessResult{Error: errExportFailed}
	}

	p.logger.Info("Export completed",
		zap.String("scrambled_user_id", job.scrambledUserId),
		zap.String("export", job.logLabel),
		zap.Int64("export_id", request.ExportId),
		zap.Int("files", job.manifest.Files.Written),
	)

	return ProcessResult{ExportedFiles: job.manifest.Files.Written}
}

// Streamed into the archive as they arrive, so peak memory tracks the concurrency limit rather
// than the size of the whole export.
func (p *Processor) writeArchive(ctx context.Context, zw *zip.Writer, job *exportJob) error {
	for _, file := range job.files {
		w, err := createEntry(zw, file.name)
		if err == nil {
			err = export.WriteCategory(ctx, p.db, w, job.manifest, file.key, file.cat, file.arg, file.guildId)
		}
		if err != nil {
			return fmt.Errorf("failed to export %s: %w", file.name, err)
		}

		job.manifest.Files.Written++
	}

	if job.discordData {
		if data := p.userCacheData(ctx, job.manifest.UserId, job.manifest); data != nil {
			if err := writeEntry(zw, "discord_data.json", data); err != nil {
				return err
			}

			job.manifest.Files.Written++
		}
	}

	results := make(chan transcriptEntry)
	done := make(chan struct{})

	var downloadErr error

	go func() {
		defer close(done)

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(p.exportConcurrency())

		for _, ref := range job.refs {
			g.Go(func() error {
				entry, err := p.fetchTranscript(gCtx, ref, job.filterFor)
				if err != nil {
					return err
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

	written := make(map[uint64]int)
	missing := make(map[uint64]int)

	var addErr error
	for entry := range results {
		if addErr != nil {
			continue
		}

		if entry.data == nil {
			missing[entry.ref.GuildId]++
			missing[0]++
			continue
		}

		if err := writeEntry(zw, transcriptPath(job.manifest.Kind, entry.ref), entry.data); err != nil {
			addErr = err
			continue
		}

		written[entry.ref.GuildId]++
		written[0]++
	}

	<-done

	if downloadErr != nil {
		p.logger.Error("Transcript download failed during export",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
			zap.Error(downloadErr),
		)
		return fmt.Errorf("failed to download transcripts: %w", downloadErr)
	}

	if addErr != nil {
		return addErr
	}

	job.manifest.Files.Written += written[0]

	for _, guildId := range job.transcriptGuilds {
		job.manifest.Add(dbmodel.GdprExportCategoryTranscripts, guildId, "transcripts", written[guildId])

		if absent := missing[guildId]; absent > 0 {
			job.manifest.Withhold(dbmodel.GdprExportCategoryTranscripts, guildId, "transcripts.unavailable", fmt.Sprintf("%d recorded transcripts are no longer held by the archive store", absent))
		}
	}

	if absent := missing[0]; absent > 0 {
		p.logger.Warn("Transcripts recorded in the database were absent from the archive store",
			zap.String("scrambled_user_id", job.scrambledUserId),
			zap.String("export", job.logLabel),
			zap.Int("count", absent),
		)
	}

	manifest, err := json.Marshal(job.manifest)
	if err != nil {
		return err
	}

	return writeEntry(zw, "manifest.json", manifest)
}

func createEntry(zw *zip.Writer, name string) (io.Writer, error) {
	return zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate, Modified: time.Now().UTC()})
}

func writeEntry(zw *zip.Writer, name string, data []byte) error {
	w, err := createEntry(zw, name)
	if err == nil {
		_, err = w.Write(data)
	}

	return err
}

// An absent transcript comes back without data, which is ordinary rather than a failure: the object
// can be purged independently of the database row. Returns nil when filtering leaves nothing of the
// requester's.
func (p *Processor) fetchTranscript(ctx context.Context, ref dbmodel.TranscriptRef, filterFor uint64) (*transcriptEntry, error) {
	transcript, err := archiver.Client.Get(ctx, ref.GuildId, ref.TicketId)
	if err != nil {
		if archiverclient.IsNotFound(err) {
			p.logger.Debug("Transcript absent from archive store, skipping",
				zap.Uint64("guild_id", ref.GuildId),
				zap.Int("ticket_id", ref.TicketId),
				zap.Error(err),
			)
			return &transcriptEntry{ref: ref}, nil
		}
		return nil, err
	}

	if filterFor != 0 {
		if !filterTranscriptToUser(&transcript, filterFor) {
			return nil, nil
		}
	}

	data, err := json.Marshal(transcript)
	if err != nil {
		return nil, fmt.Errorf("failed to serialise transcript %d/%d: %w", ref.GuildId, ref.TicketId, err)
	}

	return &transcriptEntry{ref: ref, data: data}, nil
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

	// The requester's entity can be missing from the transcript; key it by the requested id so the messages still resolve.
	entity := transcript.Entities.Users[userId]
	if entity.Id == 0 {
		entity.Id = userId
	}

	transcript.Entities.Users = map[uint64]v2.User{userId: entity}

	return true
}

// Returns nil when the cache database is unconfigured or unreadable; the manifest records which.
func (p *Processor) userCacheData(ctx context.Context, userId uint64, manifest *export.Manifest) []byte {
	if p.cachePool == nil {
		manifest.Withhold(export.CategoryDiscordData, 0, "cache", "cache database not configured for this deployment")
		return nil
	}

	data := make(map[string]interface{})

	var user json.RawMessage
	if err := p.cachePool.QueryRow(ctx, `SELECT "data" FROM users WHERE "user_id" = $1;`, userId).Scan(&user); err == nil {
		data["user"] = user
	}

	members := make(map[string]json.RawMessage)

	rows, err := p.cachePool.Query(ctx, `SELECT "guild_id", "data" FROM members WHERE "user_id" = $1;`, userId)
	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var guildId uint64
			var raw json.RawMessage
			if err := rows.Scan(&guildId, &raw); err != nil {
				continue
			}
			members[fmt.Sprintf("%d", guildId)] = raw
		}

		err = rows.Err()
	}

	if err != nil {
		manifest.Withhold(export.CategoryDiscordData, 0, "cache", "cache database could not be read")
		p.logger.Warn("Failed to read cache member data for export",
			zap.String("scrambled_user_id", utils.ScrambleUserId(userId)),
			zap.Error(err),
		)
		return nil
	}

	data["member_data"] = members

	encoded, err := json.Marshal(data)
	if err == nil {
		encoded, err = export.Normalise(string(encoded))
	}
	if err != nil {
		manifest.Withhold(export.CategoryDiscordData, 0, "cache", "cache data could not be serialised")
		return nil
	}

	records := len(members)
	if user != nil {
		records++
	}

	manifest.Add(export.CategoryDiscordData, 0, "cache", records)

	return encoded
}
