package export

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/TicketsBot-cloud/database"
	"github.com/jackc/pgx/v4"
)

type fakeRows struct {
	pgx.Rows
	rows []string
	next int
}

func (r *fakeRows) Next() bool {
	r.next++
	return r.next <= len(r.rows)
}

func (r *fakeRows) Scan(dest ...interface{}) error {
	*dest[0].(*string) = r.rows[r.next-1]
	return nil
}

func (r *fakeRows) Err() error { return nil }

func (r *fakeRows) Close() {}

type fakeQuerier map[string][]string

func (q fakeQuerier) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	query, ok := strings.CutPrefix(sql, "SELECT row_to_json(t)::text FROM (")
	if !ok {
		return nil, fmt.Errorf("source not wrapped in row_to_json: %s", sql)
	}

	return &fakeRows{rows: q[strings.TrimSuffix(query, ") t")]}, nil
}

func sourceQuery(t *testing.T, registry map[string]Category, key, name string) string {
	t.Helper()

	for _, source := range registry[key].Sources {
		if source.Name == name {
			return source.Query
		}
	}

	t.Fatalf("no source %s in %s", name, key)
	return ""
}

type categoryFile struct {
	name     string
	key      string
	category Category
	arg      uint64
	guildId  uint64
}

func writeArchive(t *testing.T, q Querier, m *Manifest, files []categoryFile) map[string][]byte {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	for _, file := range files {
		w, err := zw.Create(file.name)
		if err != nil {
			t.Fatal(err)
		}

		if err := WriteCategory(context.Background(), q, w, m, file.key, file.category, file.arg, file.guildId); err != nil {
			t.Fatal(err)
		}
	}

	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	w, err := zw.Create("manifest.json")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write(manifest); err != nil {
		t.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	entries := make(map[string][]byte)
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}

		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}

		entries[f.Name] = data
	}

	return entries
}

func readManifest(t *testing.T, entries map[string][]byte) Manifest {
	t.Helper()

	var manifest Manifest
	if err := json.Unmarshal(entries["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}

	return manifest
}

func TestArchiveNeverContainsSecrets(t *testing.T) {
	const snowflake = "123456789012345678"

	q := fakeQuerier{
		sourceQuery(t, UserCategories, "audit_log", "audit_logs"): {
			`{"id":1,"category":2,"guild_id":` + snowflake + `,"action_type":201,"old_data":{"reviewed_by":"SECRET-4"},"new_data":{"headers":[{"id":3,"name":"Authorization","value":"SECRET-1"}]}}`,
		},
		sourceQuery(t, UserCategories, "whitelabel", "whitelabel"): {
			`{"bot_id":` + snowflake + `,"public_key":"key","token":"SECRET-2"}`,
		},
		sourceQuery(t, ServerCategories, "forms", "form_input_api_headers"): {
			`{"id":1,"api_config_id":1,"header_name":"X-Api-Key","header_value":"SECRET-3","is_secret":true}`,
			`{"id":2,"api_config_id":1,"header_name":"Accept","header_value":"application/json","is_secret":false}`,
		},
		sourceQuery(t, ServerCategories, "integrations", "custom_integration_secret_values"): {
			`{"integration_id":1,"secret_name":"api_key"}`,
		},
	}

	entries := writeArchive(t, q, NewManifest(database.GdprExportTypeUser, 1, 1, nil, nil, nil), []categoryFile{
		{"audit_log.json", "audit_log", UserCategories["audit_log"], 1, 0},
		{"whitelabel.json", "whitelabel", UserCategories["whitelabel"], 1, 0},
		{"2/forms.json", "forms", ServerCategories["forms"], 2, 2},
		{"2/integrations.json", "integrations", ServerCategories["integrations"], 2, 2},
	})

	for name, data := range entries {
		for _, secret := range []string{"SECRET-1", "SECRET-2", "SECRET-3", "SECRET-4"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Errorf("%s contains %s", name, secret)
			}
		}
	}

	var auditLog struct {
		AuditLogs []struct {
			GuildId interface{} `json:"guild_id"`
		} `json:"audit_logs"`
	}
	if err := json.Unmarshal(entries["audit_log.json"], &auditLog); err != nil {
		t.Fatal(err)
	}

	if len(auditLog.AuditLogs) != 1 || auditLog.AuditLogs[0].GuildId != snowflake {
		t.Errorf("snowflake is not a JSON string: %s", entries["audit_log.json"])
	}

	if !bytes.Contains(entries["2/forms.json"], []byte(`"header_value":"application/json"`)) {
		t.Errorf("plain header value is missing: %s", entries["2/forms.json"])
	}

	manifest := readManifest(t, entries)
	for _, name := range []string{"audit_logs.credentials", "audit_logs.reviewed_by", "whitelabel.token", "form_input_api_headers.header_value", "custom_integration_secret_values.value"} {
		if !slices.ContainsFunc(manifest.Sources, func(s ManifestSource) bool { return s.Name == name && !s.Included }) {
			t.Errorf("manifest does not withhold %s", name)
		}
	}
}

func TestEmptySources(t *testing.T) {
	settings := UserCategories["settings"]
	entries := writeArchive(t, fakeQuerier{}, NewManifest(database.GdprExportTypeUser, 1, 1, nil, nil, nil), []categoryFile{{"settings.json", "settings", settings, 1, 0}})

	var file map[string]json.RawMessage
	if err := json.Unmarshal(entries["settings.json"], &file); err != nil {
		t.Fatal(err)
	}

	if len(file) != len(settings.Sources) {
		t.Errorf("settings.json holds %s", entries["settings.json"])
	}

	for _, source := range settings.Sources {
		if string(file[source.Name]) != "[]" {
			t.Errorf("%s: got %s, want []", source.Name, file[source.Name])
		}
	}
}
