package export

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
)

const UserDataVersion = 1

type Querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

type UserData struct {
	Version     int                                 `json:"version"`
	UserId      string                              `json:"user_id"`
	GeneratedAt time.Time                           `json:"generated_at"`
	Tables      map[string][]map[string]interface{} `json:"tables"`
}

// Every user-keyed table in the database module is either listed here or withheld in GetUserData,
// so the manifest can account for all of them.
type source struct {
	name  string
	query string
}

var userSources = []source{
	{"blacklist", `SELECT "guild_id" FROM blacklist WHERE "user_id" = $1;`},
	{"close_request", `SELECT "guild_id", "ticket_id", "close_at", "close_reason" FROM close_request WHERE "user_id" = $1;`},
	{"first_response_time", `SELECT "guild_id", "ticket_id", "response_time" FROM first_response_time WHERE "user_id" = $1;`},
	{"participant", `SELECT "guild_id", "ticket_id" FROM participant WHERE "user_id" = $1;`},
	{"permissions", `SELECT "guild_id", "support", "admin" FROM permissions WHERE "user_id" = $1;`},
	{"support_team_members", `SELECT "team_id" FROM support_team_members WHERE "user_id" = $1;`},
	{"ticket_claims", `SELECT "guild_id", "ticket_id" FROM ticket_claims WHERE "user_id" = $1;`},
	{"ticket_members", `SELECT "guild_id", "ticket_id" FROM ticket_members WHERE "user_id" = $1;`},
	{"tickets", `SELECT "id", "guild_id", "channel_id", "open", "open_time", "welcome_message_id", "panel_id", "has_transcript" FROM tickets WHERE "user_id" = $1;`},
	{"used_keys", `SELECT "guild_id", "key", "activated_by" FROM used_keys WHERE "activated_by" = $1;`},
	{"user_guilds", `SELECT "guild_id", "name", "owner", "permissions", "icon" FROM user_guilds WHERE "user_id" = $1;`},
	{"whitelabel_users", `SELECT "expiry" FROM whitelabel_users WHERE "user_id" = $1;`},

	// Not read by the previous export helper at all.
	{"dashboard_users", `SELECT "last_seen" FROM dashboard_users WHERE "user_id" = $1;`},
	{"votes", `SELECT "vote_time" FROM votes WHERE "user_id" = $1;`},
	{"audit_logs", `SELECT "id", "guild_id", "action_type", "resource_type", "resource_id", "old_data", "new_data", "metadata", "created_at" FROM audit_logs WHERE "user_id" = $1;`},
	{"entitlements", `SELECT "id", "guild_id", "sku_id", "source", "expires_at" FROM entitlements WHERE "user_id" = $1;`},
	{"legacy_premium_entitlements", `SELECT "tier", "sku_label", "sku_id", "is_legacy", "expires_at" FROM legacy_premium_entitlements WHERE "user_id" = $1;`},
	{"legacy_premium_entitlement_guilds", `SELECT "guild_id", "entitlement_id" FROM legacy_premium_entitlement_guilds WHERE "user_id" = $1;`},
	{"patreon_entitlements", `SELECT "entitlement_id" FROM patreon_entitlements WHERE "user_id" = $1;`},
	{"discord_entitlements", `SELECT "entitlement_id" FROM discord_entitlements WHERE "discord_id" = $1;`},
	{
		"exit_survey_responses",
		`SELECT e."guild_id", e."ticket_id", e."form_id", e."question_id", e."response"
		 FROM exit_survey_responses e
		 INNER JOIN tickets t ON t."guild_id" = e."guild_id" AND t."id" = e."ticket_id"
		 WHERE t."user_id" = $1;`,
	},
}

// Unlike the upstream helper this replaces, it takes a context, so a slow export is bounded by the
// caller's deadline instead of running unbounded on context.Background().
func GetUserData(ctx context.Context, q Querier, userId uint64, manifest *Manifest) (*UserData, error) {
	data := &UserData{
		Version:     UserDataVersion,
		UserId:      fmt.Sprintf("%d", userId),
		GeneratedAt: time.Now().UTC(),
		Tables:      make(map[string][]map[string]interface{}),
	}

	for _, src := range userSources {
		rows, err := q.Query(ctx, src.query, userId)
		if err != nil {
			return nil, fmt.Errorf("failed to query %s: %w", src.name, err)
		}

		records, err := rowsToMaps(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", src.name, err)
		}

		data.Tables[src.name] = records
		manifest.Add(src.name, len(records))
	}

	if err := addWhitelabel(ctx, q, userId, data, manifest); err != nil {
		return nil, err
	}

	return data, nil
}

// The bot token is a live credential. Shipping it in an archive delivered over Discord would hand
// anyone who sees that message full control of the user's bot, so it is withheld.
func addWhitelabel(ctx context.Context, q Querier, userId uint64, data *UserData, manifest *Manifest) error {
	rows, err := q.Query(ctx, `SELECT "bot_id", "public_key" FROM whitelabel WHERE "user_id" = $1;`, userId)
	if err != nil {
		return fmt.Errorf("failed to query whitelabel: %w", err)
	}

	records, err := rowsToMaps(rows)
	if err != nil {
		return fmt.Errorf("failed to read whitelabel: %w", err)
	}

	data.Tables["whitelabel"] = records
	manifest.Add("whitelabel", len(records))

	if len(records) > 0 {
		manifest.Withhold("whitelabel.token", "live bot credential withheld; rotate it from the dashboard if you need a copy")
	}

	return nil
}

func rowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()

	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		record := make(map[string]interface{}, len(fields))
		for i, field := range fields {
			if i < len(values) {
				record[string(field.Name)] = normalise(values[i])
			}
		}

		out = append(out, record)
	}

	return out, rows.Err()
}

// Discord snowflakes exceed the range a JSON number survives in most parsers, so anything that
// large is emitted as a string.
func normalise(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case int64:
		if t > 1<<53 || t < -(1<<53) {
			return fmt.Sprintf("%d", t)
		}
		return t
	case uint64:
		if t > 1<<53 {
			return fmt.Sprintf("%d", t)
		}
		return t
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", t[0:4], t[4:6], t[6:8], t[8:10], t[10:16])
	case []byte:
		if json.Valid(t) {
			return json.RawMessage(t)
		}
		return string(t)
	default:
		return v
	}
}
