package export

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/TicketsBot-cloud/database"
)

var registries = map[database.GdprExportType]map[string]Category{
	database.GdprExportTypeServer: ServerCategories,
	database.GdprExportTypeUser:   UserCategories,
}

func TestRegistryMatchesDatabase(t *testing.T) {
	for kind, registry := range registries {
		keys := []string{database.GdprExportCategoryTranscripts}
		if kind == database.GdprExportTypeUser {
			keys = append(keys, CategoryDiscordData)
		}

		for key, category := range registry {
			keys = append(keys, key)
			if len(category.Sources) == 0 {
				t.Errorf("%s %s has no sources", kind, key)
			}
		}

		want := kind.Categories()
		slices.Sort(keys)
		slices.Sort(want)
		if !slices.Equal(keys, want) {
			t.Errorf("%s categories %v, want %v", kind, keys, want)
		}
	}
}

func TestSourcesExplicitAndScoped(t *testing.T) {
	for kind, registry := range registries {
		for key, category := range registry {
			seen := make(map[string]bool)
			for _, source := range category.Sources {
				if seen[source.Name] {
					t.Errorf("%s %s: duplicate source %s", kind, key, source.Name)
				}
				seen[source.Name] = true

				if !strings.Contains(source.Query, "$1") || strings.Contains(source.Query, "$2") {
					t.Errorf("%s %s %s: must bind $1 and nothing else", kind, key, source.Name)
				}
				if strings.HasSuffix(strings.TrimSpace(source.Query), ";") {
					t.Errorf("%s %s %s: trailing semicolon", kind, key, source.Name)
				}
				if strings.Contains(strings.ReplaceAll(selectList(source.Query), "COUNT(*)", ""), "*") {
					t.Errorf("%s %s %s: select list uses *", kind, key, source.Name)
				}
			}
		}
	}
}

func TestNoSourceSelectsWithheldColumns(t *testing.T) {
	type denied struct{ column, category, entry string }

	deniedColumns := map[database.GdprExportType][]denied{
		database.GdprExportTypeServer: {
			{"whitelabel.token", "", ""},
			{"webhooks.webhook_token", "tickets", "webhooks"},
			{"custom_integration_secret_values.value", "integrations", ""},
			{"custom_integration_headers.value", "integrations", "custom_integration_headers"},
			{"email_verification_codes.code", "", ""},
			{"kb_articles.search_vector", "knowledge_base", ""},
			{"global_blacklist.reason", "", ""},
			{"server_blacklist.reason", "blacklist", "server_blacklist"},
			{"submission_blacklist.reason", "blacklist", "submission_blacklist"},
			{"form_input_api_headers.header_value", "forms", ""},
			{"gallery_listings.reviewed_by", "gallery", ""},
			{"kb_article_feedback.user_id", "knowledge_base", ""},
			{"custom_integrations.webhook_url", "integrations", ""},
			{"custom_integrations.validation_url", "integrations", ""},
			{"custom_integrations.owner_id", "integrations", ""},
		},
		database.GdprExportTypeUser: {
			{"whitelabel.token", "whitelabel", ""},
			{"webhooks.webhook_token", "", ""},
			{"custom_integration_secret_values.value", "integrations", "custom_integration_secret_values"},
			{"custom_integration_headers.value", "integrations", ""},
			{"email_verification_codes.code", "settings", "email_verification_codes"},
			{"kb_articles.search_vector", "", ""},
			{"global_blacklist.reason", "blacklists", ""},
			{"server_blacklist.reason", "blacklists", ""},
			{"submission_blacklist.reason", "blacklists", ""},
			{"form_input_api_headers.header_value", "", ""},
			{"affiliate_codes.approved_by", "affiliate", ""},
			{"affiliate_referrals.referred_user_id", "affiliate", ""},
			{"affiliate_referrals.affiliate_user_id", "affiliate", "affiliate_referrals_received.affiliate_user_id"},
			{"affiliate_referrals.polar_subscription_id", "affiliate", ""},
			{"affiliate_referrals.voided_reason", "affiliate", ""},
			{"affiliate_referrals_received.affiliate_code_id", "affiliate", ""},
			{"gallery_listings.reviewed_by", "gallery", ""},
			{"custom_integrations.reviewed_by", "integrations", ""},
			{"close_reason.closed_by", "tickets", ""},
			{"tickets.notes_thread_id", "tickets", ""},
			{"custom_integration_guilds.guild_id", "integrations", ""},
		},
	}

	for kind, deny := range deniedColumns {
		registry := registries[kind]

		for _, d := range deny {
			table, column, _ := strings.Cut(d.column, ".")
			readsTable := regexp.MustCompile(`(?i)\b(FROM|JOIN)\s+(\w+\.)?"?` + table + `\b`)
			selects := regexp.MustCompile(`(?i)\b` + column + `\b`)
			// A source name counts as its table, so an entry can target one of two views of the same table.
			reads := func(s Source) bool { return s.Name == table || readsTable.MatchString(s.Query) }

			for key, category := range registry {
				for _, source := range category.Sources {
					list := strings.Replace(selectList(source.Query), `CASE WHEN "is_secret" THEN NULL ELSE "header_value" END AS "header_value"`, "", 1)
					if reads(source) && selects.MatchString(list) {
						t.Errorf("%s %s %s selects %s", kind, key, source.Name, d.column)
					}
				}
			}

			if d.category == "" {
				continue
			}

			entry := d.entry
			if entry == "" {
				entry = d.column

				if !slices.ContainsFunc(registry[d.category].Sources, reads) {
					t.Errorf("%s %s does not read %s", kind, d.category, table)
				}
			}

			if !slices.ContainsFunc(registry[d.category].Withheld, func(w Withheld) bool { return w.Name == entry }) {
				t.Errorf("%s %s does not withhold %s", kind, d.category, entry)
			}
		}
	}
}

func oneLine(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func selectList(query string) string {
	query = oneLine(query)
	upper := strings.ToUpper(query)

	depth := 0
	for i := range query {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
		}

		if depth == 0 && strings.HasPrefix(upper[i:], " FROM ") {
			return strings.TrimPrefix(query[:i], "SELECT ")
		}
	}

	return query
}

func TestAffiliateFlagReasons(t *testing.T) {
	reviewer := AffiliateCategory(true).Sources[1]
	if reviewer.Name != "affiliate_referrals" || !strings.Contains(reviewer.Query, `CASE WHEN "status" = 'flagged' THEN "voided_reason" END`) {
		t.Errorf("reviewer query does not limit voided_reason to flagged referrals: %s", reviewer.Query)
	}

	if strings.Contains(UserCategories["affiliate"].Sources[1].Query, "voided_reason") {
		t.Error("default affiliate category selects voided_reason")
	}
}
