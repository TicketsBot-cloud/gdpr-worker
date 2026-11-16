package i18n

type MessageId string

var (
	GdprCompletedTitle               MessageId = "gdpr.completed.title"
	GdprCompletedAllTranscripts      MessageId = "gdpr.completed.all_transcripts"
	GdprCompletedAllTranscriptsMulti MessageId = "gdpr.completed.all_transcripts_multi"
	GdprCompletedSpecificTranscripts MessageId = "gdpr.completed.specific_transcripts"
	GdprCompletedAllMessages         MessageId = "gdpr.completed.all_messages"
	GdprCompletedAllMessagesMulti    MessageId = "gdpr.completed.all_messages_multi"
	GdprCompletedSpecificMessages    MessageId = "gdpr.completed.specific_messages"
	GdprCompletedError               MessageId = "gdpr.completed.error"
	GdprFollowupError                MessageId = "gdpr.followup.error"
	GdprFollowupNoData               MessageId = "gdpr.followup.no_data"
	GdprFollowupSuccess              MessageId = "gdpr.followup.success"
	GdprCompletedExportGuild         MessageId = "gdpr.completed.export_guild"
	GdprCompletedExportGuildMulti    MessageId = "gdpr.completed.export_guild_multi"
	GdprCompletedExportUser          MessageId = "gdpr.completed.export_user"
	GdprCompletedExportReady         MessageId = "gdpr.completed.export_ready"
	GdprCompletedExportNoData        MessageId = "gdpr.completed.export_no_data"
	GdprCompletedDownloadButton      MessageId = "gdpr.completed.download_button"
)
