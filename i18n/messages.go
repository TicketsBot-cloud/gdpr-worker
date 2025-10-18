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
	GdprCompletedNoData              MessageId = "gdpr.completed.no_data"
	GdprCompletedError               MessageId = "gdpr.completed.error"
	GdprFollowupError                MessageId = "gdpr.followup.error"
	GdprFollowupNoData               MessageId = "gdpr.followup.no_data"
	GdprFollowupSuccess              MessageId = "gdpr.followup.success"
)
