package export

type Source struct {
	Name  string
	Query string
}

type Withheld struct {
	Name   string
	Reason string
}

type Category struct {
	Sources  []Source
	Withheld []Withheld
}

const CategoryDiscordData = "discord_data"

const (
	guildPanels     = `"panel_id" IN (SELECT "panel_id" FROM panels WHERE "guild_id" = $1)`
	guildTeams      = `"team_id" IN (SELECT "id" FROM support_team WHERE "guild_id" = $1)`
	guildFormInputs = `"form_input_id" IN (SELECT i."id" FROM form_input i JOIN forms f ON f."form_id" = i."form_id" WHERE f."guild_id" = $1)`

	ownTickets      = `JOIN tickets t ON t."guild_id" = x."guild_id" AND t."id" = x."ticket_id" WHERE t."user_id" = $1`
	ownBot          = `JOIN whitelabel w ON w."bot_id" = x."bot_id" WHERE w."user_id" = $1`
	ownIntegrations = `JOIN custom_integrations i ON i."id" = x."integration_id" WHERE i."owner_id" = $1`

	galleryListingColumns = `"listing_type", "name", "description", "category", "status", "review_note", "reviewed_at", "import_count",
		"featured", "snapshot_data", "title", "content", "colour", "image_url", "thumbnail_url", "button_style", "button_label",
		"emoji_name", "welcome_message", "created_at", "updated_at"`

	affiliateReferralColumns = `"id", "affiliate_code_id", "referred_tier", "referred_sku_id", "purchased_days", "credit_days", "status", "created_at",
		"redeemable_at", "redeemed_at", "entitlement_id", "voided_at"`
)

const (
	purchaserRecord     = "billing record of the purchaser, included in their own export"
	whitelabelOwner     = "belongs to the whitelabel bot owner's account"
	trustAndSafety      = "Tickets trust and safety record"
	integrationOwner    = "belongs to the integration's owner"
	internalBookkeeping = "internal bookkeeping"
	writeOnlySecret     = "secret values are write-only, as on the dashboard"
	auditCredentials    = "credential values inside entries are replaced with [withheld]"
	staffReviewer       = "identifies the Tickets staff member who reviewed it"
	moderationNote      = "internal moderation note"
	affiliateOwner      = "identifies the affiliate whose code you used"
	flagReviewNote      = "shown only to Tickets admins reviewing flagged referrals"
	voidedReasonNote    = "not shown for voided referrals, even to Tickets admins"
)

var ServerCategories = map[string]Category{
	"settings": {
		Sources: []Source{
			{"settings", `SELECT "context_menu_permission_level", "context_menu_add_sender", "context_menu_panel", "anonymise_dashboard_responses", "ticket_limit" FROM settings WHERE "guild_id" = $1`},
			{"claim_settings", `SELECT "switch_panel_claim_behavior" FROM claim_settings WHERE "guild_id" = $1`},
			{"custom_colours", `SELECT "colour_id", "colour_code" FROM custom_colours WHERE "guild_id" = $1`},
			{"active_language", `SELECT "language" FROM active_language WHERE "guild_id" = $1`},
			{"channel_category", `SELECT "category_id" FROM channel_category WHERE "guild_id" = $1`},
			{"staff_override", `SELECT "expires" FROM staff_override WHERE "guild_id" = $1`},
			{"dashboard_onboarding", `SELECT "onboarding_completed", "onboarding_completed_at", "current_step", "skipped" FROM dashboard_onboarding WHERE "guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"premium_guilds", purchaserRecord},
			{"entitlements", purchaserRecord},
			{"used_keys", purchaserRecord},
			{"legacy_premium_entitlement_guilds", purchaserRecord},
			{"discord_entitlements", purchaserRecord},
			{"whitelabel_guilds", whitelabelOwner},
			{"whitelabel_allowed_guilds", whitelabelOwner},
			{"guild_leave_time", "internal deletion schedule"},
			{"experiment_exposures", "internal feature rollout record"},
		},
	},
	"panels": {
		Sources: []Source{
			{"panels", `SELECT "panel_id", "message_id", "channel_id", "title", "content", "colour", "target_category", "emoji_name", "emoji_id",
				"emoji_animated", "welcome_message", "default_team", "custom_id", "image_url", "thumbnail_url", "button_style", "button_label",
				"form_id", "naming_scheme", "force_disabled", "disabled", "exit_survey_form_id", "pending_category", "mention_behaviour",
				"transcript_channel_id", "use_threads", "ticket_notification_channel", "cooldown_seconds", "ticket_limit", "hide_close_button",
				"hide_close_with_reason_button", "hide_claim_button", "show_in_open_command", "store_transcripts", "overflow_enabled",
				"overflow_category_id", "users_can_close", "close_confirmation", "feedback_enabled", "support_can_view", "support_can_type"
				FROM panels WHERE "guild_id" = $1`},
			{"embeds", `SELECT "id", "title", "description", "url", "colour", "author_name", "author_icon_url", "author_url", "image_url",
				"thumbnail_url", "footer_text", "footer_icon_url", "timestamp" FROM embeds WHERE "guild_id" = $1`},
			{"embed_fields", `SELECT "id", "embed_id", "name", "value", "inline" FROM embed_fields WHERE "embed_id" IN (SELECT "id" FROM embeds WHERE "guild_id" = $1)`},
			{"panel_access_control_rules", `SELECT "panel_id", "role_id", "position", "action" FROM panel_access_control_rules WHERE ` + guildPanels},
			{"panel_auto_close", `SELECT "panel_id", "enabled", "since_open_with_no_response", "since_last_message", "on_user_leave" FROM panel_auto_close WHERE ` + guildPanels},
			{"panel_close_reasons", `SELECT "panel_id", "reasons", "allow_custom" FROM panel_close_reasons WHERE ` + guildPanels},
			{"panel_here_mentions", `SELECT "panel_id", "should_mention_here" FROM panel_here_mentions WHERE ` + guildPanels},
			{"panel_user_mentions", `SELECT "panel_id", "should_mention_user" FROM panel_user_mentions WHERE ` + guildPanels},
			{"panel_role_mentions", `SELECT "panel_id", "role_id" FROM panel_role_mentions WHERE ` + guildPanels},
			{"panel_teams", `SELECT "panel_id", "team_id" FROM panel_teams WHERE ` + guildPanels},
			{"panel_ticket_permissions", `SELECT "panel_id", "add_reactions", "send_tts_messages", "embed_links", "attach_files", "use_external_emojis",
				"use_external_stickers", "send_voice_messages" FROM panel_ticket_permissions WHERE ` + guildPanels},
			{"panel_kb_categories", `SELECT "panel_id", "category_id" FROM panel_kb_categories WHERE ` + guildPanels},
		},
	},
	"support_hours": {
		Sources: []Source{
			{"panel_support_hours", `SELECT "id", "panel_id", "day_of_week", "start_time", "end_time", "enabled", "timezone" FROM panel_support_hours WHERE ` + guildPanels},
			{"panel_support_hours_settings", `SELECT "panel_id", "out_of_hours_behaviour", "out_of_hours_title", "out_of_hours_message", "out_of_hours_colour"
				FROM panel_support_hours_settings WHERE ` + guildPanels},
		},
	},
	"multi_panels": {
		Sources: []Source{
			{"multi_panels", `SELECT "id", "message_id", "channel_id", "select_menu", "select_menu_placeholder", "embed" FROM multi_panels WHERE "guild_id" = $1`},
			{"multi_panel_targets", `SELECT "multi_panel_id", "panel_id", "position", "custom_label", "description", "custom_emoji_name", "custom_emoji_id"
				FROM multi_panel_targets WHERE "multi_panel_id" IN (SELECT "id" FROM multi_panels WHERE "guild_id" = $1)`},
		},
	},
	"forms": {
		Sources: []Source{
			{"forms", `SELECT "form_id", "title", "custom_id" FROM forms WHERE "guild_id" = $1`},
			{"form_input", `SELECT "id", "form_id", "type", "position", "custom_id", "style", "label", "description", "placeholder", "required",
				"min_length", "max_length", "content" FROM form_input WHERE "form_id" IN (SELECT "form_id" FROM forms WHERE "guild_id" = $1)`},
			{"form_input_option", `SELECT "id", "form_input_id", "position", "label", "description", "value" FROM form_input_option WHERE ` + guildFormInputs},
			{"form_input_api_config", `SELECT "id", "form_input_id", "endpoint_url", "method", "cache_duration_seconds", "no_options_message", "body_template",
				"created_at", "updated_at" FROM form_input_api_config WHERE ` + guildFormInputs},
			{"form_input_api_headers", `SELECT "id", "api_config_id", "header_name", CASE WHEN "is_secret" THEN NULL ELSE "header_value" END AS "header_value", "is_secret"
				FROM form_input_api_headers WHERE "api_config_id" IN (SELECT "id" FROM form_input_api_config WHERE ` + guildFormInputs + `)`},
		},
		Withheld: []Withheld{
			{"form_input_api_headers.header_value", writeOnlySecret},
		},
	},
	"tags": {
		Sources: []Source{
			{"tags", `SELECT "tag_id", "content", "embed", "application_command_id", "kb_article_id" FROM tags WHERE "guild_id" = $1`},
		},
	},
	"staff": {
		Sources: []Source{
			{"permissions", `SELECT "user_id", "support", "admin" FROM permissions WHERE "guild_id" = $1`},
			{"role_permissions", `SELECT "role_id", "support", "admin" FROM role_permissions WHERE "guild_id" = $1`},
			{"on_call", `SELECT "user_id", "is_on_call" FROM on_call WHERE "guild_id" = $1`},
			{"guild_metadata", `SELECT "on_call_role" FROM guild_metadata WHERE "guild_id" = $1`},
			{"support_team", `SELECT "id", "name", "on_call_role_id" FROM support_team WHERE "guild_id" = $1`},
			{"support_team_members", `SELECT "team_id", "user_id" FROM support_team_members WHERE ` + guildTeams},
			{"support_team_roles", `SELECT "team_id", "role_id" FROM support_team_roles WHERE ` + guildTeams},
			{"support_team_permissions", `SELECT "team_id", "add_reactions", "send_messages", "send_tts_messages", "embed_links", "attach_files",
				"mention_everyone", "use_external_emojis", "use_application_commands", "use_external_stickers", "send_voice_messages"
				FROM support_team_permissions WHERE ` + guildTeams},
		},
		Withheld: []Withheld{
			{"user_guilds", "each member's cached server list, included in their own export"},
		},
	},
	"blacklist": {
		Sources: []Source{
			{"blacklist", `SELECT "user_id" FROM blacklist WHERE "guild_id" = $1`},
			{"role_blacklist", `SELECT "role_id" FROM role_blacklist WHERE "guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"server_blacklist", trustAndSafety},
			{"submission_blacklist", trustAndSafety},
		},
	},
	"knowledge_base": {
		Sources: []Source{
			{"kb_settings", `SELECT "primary_bg", "card_bg", "text_colour", "accent_colour", "logo_url", "hide_branding" FROM kb_settings WHERE "guild_id" = $1`},
			{"kb_categories", `SELECT "id", "name", "emoji", "position" FROM kb_categories WHERE "guild_id" = $1`},
			{"kb_articles", `SELECT "id", "title", "slug", "description", "content", "embed", "category_ids", "keywords", "position", "published",
				"show_helpful_count", "helpful_count", "not_helpful_count", "created_at", "updated_at" FROM kb_articles WHERE "guild_id" = $1`},
			{"kb_article_feedback", `SELECT "id", "article_id", "panel_id", "helpful", "created_at" FROM kb_article_feedback WHERE "guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"kb_articles.search_vector", "search index built from each article's title, content and keywords"},
			{"kb_article_feedback.user_id", "identifies who voted; the dashboard shows only totals"},
		},
	},
	"integrations": {
		Sources: []Source{
			{"custom_integration_guilds", `SELECT g."integration_id", i."name", i."description", i."image_url", i."privacy_policy_url", i."public", i."approved"
				FROM custom_integration_guilds g JOIN custom_integrations i ON i."id" = g."integration_id" WHERE g."guild_id" = $1`},
			{"custom_integration_secret_values", `SELECT v."integration_id", s."name" AS "secret_name"
				FROM custom_integration_secret_values v JOIN custom_integration_secrets s ON s."id" = v."secret_id" WHERE v."guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"custom_integration_secret_values.value", writeOnlySecret},
			{"custom_integrations.owner_id", integrationOwner},
			{"custom_integrations.webhook_url", integrationOwner},
			{"custom_integrations.validation_url", integrationOwner},
			{"custom_integration_headers", integrationOwner},
			{"custom_integration_placeholders", integrationOwner},
		},
	},
	"tickets": {
		Sources: []Source{
			{"tickets", `SELECT "id", "user_id", "channel_id", "open", "open_time", "close_time", "status", "source", "panel_id", "has_transcript",
				"is_thread", "welcome_message_id", "join_message_id", "notes_thread_id" FROM tickets WHERE "guild_id" = $1`},
			{"close_reason", `SELECT "ticket_id", "close_reason", "closed_by" FROM close_reason WHERE "guild_id" = $1`},
			{"close_request", `SELECT "ticket_id", "user_id", "close_at", "close_reason" FROM close_request WHERE "guild_id" = $1`},
			{"service_ratings", `SELECT "ticket_id", "rating" FROM service_ratings WHERE "guild_id" = $1`},
			{"ticket_claims", `SELECT "ticket_id", "user_id" FROM ticket_claims WHERE "guild_id" = $1`},
			{"ticket_members", `SELECT "ticket_id", "user_id" FROM ticket_members WHERE "guild_id" = $1`},
			{"participant", `SELECT "ticket_id", "user_id" FROM participant WHERE "guild_id" = $1`},
			{"exit_survey_responses", `SELECT "ticket_id", "form_id", "question_id", "response" FROM exit_survey_responses WHERE "guild_id" = $1`},
			{"first_response_time", `SELECT "ticket_id", "user_id", "response_time" FROM first_response_time WHERE "guild_id" = $1`},
			{"ticket_message_counts", `SELECT "ticket_id", "staff_messages", "user_messages" FROM ticket_message_counts WHERE "guild_id" = $1`},
			{"ticket_labels", `SELECT "label_id", "name", "colour" FROM ticket_labels WHERE "guild_id" = $1`},
			{"ticket_label_assignments", `SELECT "ticket_id", "label_id" FROM ticket_label_assignments WHERE "guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"webhooks", "per-ticket Discord webhook credentials"},
			{"archive_messages", internalBookkeeping},
			{"archive_dm_messages", internalBookkeeping},
			{"auto_close_exclude", internalBookkeeping},
			{"category_update_queue", internalBookkeeping},
			{"ticket_last_message", internalBookkeeping},
			{"guild_ticket_counters", internalBookkeeping},
		},
	},
	"audit_log": {
		Sources: []Source{
			{"audit_logs", `SELECT "id", "category", "user_id", "action_type", "resource_type", "resource_id", "old_data", "new_data", "metadata", "created_at"
				FROM audit_logs WHERE "guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"audit_logs.credentials", auditCredentials},
			{"audit_logs.reviewed_by", staffReviewer},
		},
	},
	"gallery": {
		Sources: []Source{
			{"gallery_listings", `SELECT "id", "submitter_user_id", ` + galleryListingColumns + ` FROM gallery_listings WHERE "source_guild_id" = $1`},
			{"gallery_listing_tags", `SELECT t."listing_id", t."tag" FROM gallery_listing_tags t JOIN gallery_listings l ON l."id" = t."listing_id" WHERE l."source_guild_id" = $1`},
		},
		Withheld: []Withheld{
			{"gallery_listings.reviewed_by", staffReviewer},
		},
	},
}

var UserCategories = map[string]Category{
	"settings": {
		Sources: []Source{
			{"notification_preferences", `SELECT "category", "discord_dm", "email", "in_app" FROM notification_preferences WHERE "user_id" = $1`},
			{"user_emails", `SELECT "email", "verified", "created_at", "updated_at" FROM user_emails WHERE "user_id" = $1`},
		},
		Withheld: []Withheld{
			{"email_verification_codes", "one-time security code"},
		},
	},
	"account": {
		Sources: []Source{
			{"dashboard_users", `SELECT "last_seen" FROM dashboard_users WHERE "user_id" = $1`},
			{"user_guilds", `SELECT "guild_id", "name", "owner", "permissions", "icon" FROM user_guilds WHERE "user_id" = $1`},
			{"bot_staff", `SELECT "tier", "global_view" FROM bot_staff WHERE "user_id" = $1`},
			{"experiment_exposures", `SELECT "experiment_key", "variation_id", "feature_key", "exposed_at" FROM experiment_exposures
				WHERE "identifier_type" IN ('user_id', 'dashboard_user_id') AND "identifier" = ($1::int8)::text`},
		},
	},
	"notifications": {
		Sources: []Source{
			{"notifications", `SELECT "id", "category", "title", "body", "link", "read", "created_at" FROM notifications WHERE "user_id" = $1 AND left("category", 6) <> 'admin_'`},
		},
		Withheld: []Withheld{
			{"notifications.admin", "staff review notifications quote other users"},
		},
	},
	"tickets": {
		Sources: []Source{
			{"tickets", `SELECT "id", "guild_id", "channel_id", "open", "open_time", "close_time", "status", "source", "panel_id", "has_transcript",
				"is_thread", "welcome_message_id", "join_message_id" FROM tickets WHERE "user_id" = $1`},
			{"participant", `SELECT "guild_id", "ticket_id" FROM participant WHERE "user_id" = $1`},
			{"ticket_members", `SELECT "guild_id", "ticket_id" FROM ticket_members WHERE "user_id" = $1`},
			{"close_reason", `SELECT x."guild_id", x."ticket_id", x."close_reason" FROM close_reason x ` + ownTickets},
			{"ticket_last_message", `SELECT "guild_id", "ticket_id", "last_message_time", "user_is_staff" FROM ticket_last_message WHERE "user_id" = $1`},
			{"archive_dm_messages", `SELECT x."guild_id", x."ticket_id", x."message_id" FROM archive_dm_messages x ` + ownTickets},
		},
		Withheld: []Withheld{
			{"close_reason.closed_by", "identifies the staff member who closed the ticket"},
			{"tickets.notes_thread_id", "points to the staff-only notes thread"},
			{"ticket_message_counts", "message totals that include other participants' messages"},
		},
	},
	"staff_activity": {
		Sources: []Source{
			{"permissions", `SELECT "guild_id", "support", "admin" FROM permissions WHERE "user_id" = $1`},
			{"support_team_members", `SELECT x."team_id", t."guild_id", t."name" FROM support_team_members x JOIN support_team t ON t."id" = x."team_id" WHERE x."user_id" = $1`},
			{"on_call", `SELECT "guild_id", "is_on_call" FROM on_call WHERE "user_id" = $1`},
			{"ticket_claims", `SELECT "guild_id", "ticket_id" FROM ticket_claims WHERE "user_id" = $1`},
			{"close_request", `SELECT "guild_id", "ticket_id", "close_at", "close_reason" FROM close_request WHERE "user_id" = $1`},
			{"first_response_time", `SELECT "guild_id", "ticket_id", "response_time" FROM first_response_time WHERE "user_id" = $1`},
			{"tickets_closed", `SELECT "guild_id", "ticket_id", "close_reason" FROM close_reason WHERE "closed_by" = $1`},
		},
	},
	"premium": {
		Sources: []Source{
			{"entitlements", `SELECT "id", "guild_id", "sku_id", "source", "expires_at" FROM entitlements WHERE "user_id" = $1`},
			{"legacy_premium_entitlements", `SELECT "tier", "sku_label", "sku_id", "is_legacy", "expires_at" FROM legacy_premium_entitlements WHERE "user_id" = $1`},
			{"legacy_premium_entitlement_guilds", `SELECT "guild_id", "entitlement_id" FROM legacy_premium_entitlement_guilds WHERE "user_id" = $1`},
			{"patreon_entitlements", `SELECT "entitlement_id" FROM patreon_entitlements WHERE "user_id" = $1`},
			{"discord_entitlements", `SELECT x."discord_id", x."entitlement_id" FROM discord_entitlements x JOIN entitlements e ON e."id" = x."entitlement_id" WHERE e."user_id" = $1`},
			{"polar_entitlements", `SELECT "polar_subscription_id", "entitlement_id", "polar_product_id", "status" FROM polar_entitlements WHERE "user_id" = $1`},
			{"used_keys", `SELECT "guild_id", "key" FROM used_keys WHERE "activated_by" = $1`},
			{"votes", `SELECT "vote_time" FROM votes WHERE "user_id" = $1`},
			{"vote_credits", `SELECT "credits" FROM vote_credits WHERE "user_id" = $1`},
		},
	},
	"affiliate": AffiliateCategory(false),
	"whitelabel": {
		Sources: []Source{
			{"whitelabel", `SELECT "bot_id", "public_key" FROM whitelabel WHERE "user_id" = $1`},
			{"whitelabel_users", `SELECT "expiry" FROM whitelabel_users WHERE "user_id" = $1`},
			{"whitelabel_errors", `SELECT "error", "error_time" FROM whitelabel_errors WHERE "user_id" = $1`},
			{"whitelabel_guilds", `SELECT x."guild_id" FROM whitelabel_guilds x ` + ownBot},
			{"whitelabel_statuses", `SELECT x."status", x."status_type" FROM whitelabel_statuses x ` + ownBot},
		},
		Withheld: []Withheld{
			{"whitelabel.token", "live bot credential withheld; rotate it from the dashboard if you need a copy"},
			{"whitelabel_allowed_guilds", "not used by any current feature"},
		},
	},
	"integrations": {
		Sources: []Source{
			{"custom_integrations", `SELECT "id", "name", "description", "http_method", "webhook_url", "validation_url", "image_url", "privacy_policy_url",
				"public", "approved", "rejection_reason", "reviewed_at" FROM custom_integrations WHERE "owner_id" = $1`},
			{"custom_integration_secrets", `SELECT x."id", x."integration_id", x."name", x."description" FROM custom_integration_secrets x ` + ownIntegrations},
			{"custom_integration_headers", `SELECT x."id", x."integration_id", x."name" FROM custom_integration_headers x ` + ownIntegrations},
			{"custom_integration_placeholders", `SELECT x."id", x."integration_id", x."name", x."json_path" FROM custom_integration_placeholders x ` + ownIntegrations},
			{"custom_integration_guilds", `SELECT x."integration_id", COUNT(*) AS "guild_count" FROM custom_integration_guilds x ` + ownIntegrations + ` GROUP BY x."integration_id"`},
		},
		Withheld: []Withheld{
			{"custom_integrations.reviewed_by", staffReviewer},
			{"custom_integration_headers.value", "header values are credentials"},
			{"custom_integration_guilds.guild_id", "identifies the servers that use the integration; only the count is included"},
			{"custom_integration_secret_values", "entered by the servers that use the integration"},
		},
	},
	"gallery": {
		Sources: []Source{
			{"gallery_listings", `SELECT "id", "source_guild_id", ` + galleryListingColumns + ` FROM gallery_listings WHERE "submitter_user_id" = $1`},
			{"gallery_listing_tags", `SELECT t."listing_id", t."tag" FROM gallery_listing_tags t JOIN gallery_listings l ON l."id" = t."listing_id" WHERE l."submitter_user_id" = $1`},
		},
		Withheld: []Withheld{
			{"gallery_listings.reviewed_by", staffReviewer},
		},
	},
	"feedback": {
		Sources: []Source{
			{"kb_article_feedback", `SELECT "guild_id", "article_id", "panel_id", "helpful", "created_at" FROM kb_article_feedback WHERE "user_id" = $1`},
			{"service_ratings", `SELECT x."guild_id", x."ticket_id", x."rating" FROM service_ratings x ` + ownTickets},
			{"exit_survey_responses", `SELECT x."guild_id", x."ticket_id", x."form_id", x."question_id", x."response" FROM exit_survey_responses x ` + ownTickets},
		},
	},
	"blacklists": {
		Sources: []Source{
			{"blacklist", `SELECT "guild_id" FROM blacklist WHERE "user_id" = $1`},
			{"global_blacklist", `SELECT "user_id" FROM global_blacklist WHERE "user_id" = $1`},
			{"submission_blacklist", `SELECT "feature" FROM submission_blacklist WHERE "target_type" = 'user' AND "target_id" = $1`},
			{"server_blacklist", `SELECT "guild_id" FROM server_blacklist WHERE "owner_id" = $1 OR "real_owner_id" = $1`},
		},
		Withheld: []Withheld{
			{"global_blacklist.reason", moderationNote},
			{"submission_blacklist.reason", moderationNote},
			{"server_blacklist.reason", moderationNote},
		},
	},
	"audit_log": {
		Sources: []Source{
			// Bot staff changes were logged before audit categories existed, so their old rows have no category.
			{"audit_logs", `SELECT "id", "category", "guild_id", "action_type", "resource_type", "resource_id", "old_data", "new_data", "metadata", "created_at"
				FROM audit_logs WHERE "user_id" = $1 AND "category" IS DISTINCT FROM 3 AND NOT ("category" IS NULL AND "action_type" IN (300, 301))`},
		},
		Withheld: []Withheld{
			{"audit_logs.credentials", auditCredentials},
			{"audit_logs.reviewed_by", staffReviewer},
			{"audit_logs.staff", "Tickets staff actions are internal moderation records about other users"},
			{"audit_logs.about_you", "entries other people recorded about you are not included"},
		},
	},
	"gdpr_requests": {
		Sources: []Source{
			{"gdpr_logs", `SELECT "id", "request_type", "request_date", "status" FROM gdpr_logs WHERE "requester" = encode(sha256(convert_to(($1::int8)::text, 'UTF8')), 'hex')`},
			{"gdpr_exports", `SELECT "id", "type", "guild_ids", "categories", "ticket_ids", "status", "created_at", "expires_at" FROM gdpr_exports WHERE "requester_id" = $1`},
		},
	},
}

func AffiliateCategory(flagReviewer bool) Category {
	referrals := `SELECT ` + affiliateReferralColumns
	reasonNote := flagReviewNote
	if flagReviewer {
		referrals += `, CASE WHEN "status" = 'flagged' THEN "voided_reason" END AS "voided_reason"`
		reasonNote = voidedReasonNote
	}

	return Category{
		Sources: []Source{
			{"affiliate_codes", `SELECT "id", "code", "polar_discount_id", "status", "discount_basis_points", "credit_percentage", "created_at", "approved_at", "revoked_at"
				FROM affiliate_codes WHERE "user_id" = $1`},
			{"affiliate_referrals", referrals + ` FROM affiliate_referrals WHERE "affiliate_user_id" = $1`},
			{"affiliate_referrals_received", `SELECT "id", "referred_tier", "referred_sku_id", "purchased_days", "created_at" FROM affiliate_referrals WHERE "referred_user_id" = $1`},
		},
		Withheld: []Withheld{
			{"affiliate_codes.approved_by", staffReviewer},
			{"affiliate_referrals.referred_user_id", "identifies the customer who used your code"},
			{"affiliate_referrals.polar_subscription_id", "identifies the referred customer's subscription"},
			{"affiliate_referrals.voided_reason", reasonNote},
			{"affiliate_referrals_received.affiliate_user_id", affiliateOwner},
			{"affiliate_referrals_received.affiliate_code_id", affiliateOwner},
		},
	}
}
