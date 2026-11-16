package cleanup

import (
	"context"
	"time"

	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

func Start(ctx context.Context, client *minio.Client, bucket string, logger *zap.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		removeExpired(ctx, client, bucket, logger)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func removeExpired(ctx context.Context, client *minio.Client, bucket string, logger *zap.Logger) {
	// 500 per hourly run, page until empty if expiries ever outgrow that.
	exports, err := database.Client.GdprExports.ListExpired(ctx, 500)
	if err != nil {
		logger.Error("Failed to list expired GDPR exports", zap.Error(err))
		return
	}

	removed := 0
	for _, e := range exports {
		err := client.RemoveObject(ctx, bucket, export.ObjectKey(e.Type, e.Id), minio.RemoveObjectOptions{})
		if err != nil && minio.ToErrorResponse(err).Code != minio.NoSuchKey {
			logger.Error("Failed to remove expired GDPR export", zap.Int64("export_id", e.Id), zap.Error(err))
			continue
		}

		// Expire the log while the row still exists, so a failed update is retried on the next run.
		if e.Status == dbmodel.GdprExportStatusReady {
			if err := database.Client.GdprLogs.UpdateLogStatus(e.GdprLogId, dbmodel.GdprLogStatusExpired); err != nil {
				logger.Error("Failed to mark GDPR log as expired", zap.Int("request_id", e.GdprLogId), zap.Error(err))
				continue
			}
		}

		if err := database.Client.GdprExports.Delete(ctx, e.Id); err != nil {
			logger.Error("Failed to delete expired GDPR export", zap.Int64("export_id", e.Id), zap.Error(err))
			continue
		}

		removed++
	}

	if removed > 0 {
		logger.Info("Removed expired GDPR exports", zap.Int("count", removed))
	}
}
