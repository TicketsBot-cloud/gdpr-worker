package export

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v4"
)

const withheldValue = "[withheld]"

type Querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

func WriteCategory(ctx context.Context, q Querier, w io.Writer, m *Manifest, key string, cat Category, arg, guildId uint64) error {
	// bufio keeps the first write error and the final Flush returns it.
	bw := bufio.NewWriter(w)
	bw.WriteByte('{')

	for i, src := range cat.Sources {
		if i > 0 {
			bw.WriteByte(',')
		}
		bw.WriteString(`"` + src.Name + `":[`)

		records, err := writeRows(ctx, q, bw, src.Query, arg)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", src.Name, err)
		}

		bw.WriteByte(']')
		m.Add(key, guildId, src.Name, records)
	}

	bw.WriteByte('}')

	for _, withheld := range cat.Withheld {
		m.Withhold(key, guildId, withheld.Name, withheld.Reason)
	}

	return bw.Flush()
}

func writeRows(ctx context.Context, q Querier, w *bufio.Writer, query string, arg uint64) (int, error) {
	rows, err := q.Query(ctx, "SELECT row_to_json(t)::text FROM ("+query+") t", arg)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	records := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return records, err
		}

		row, err := Normalise(raw)
		if err != nil {
			return records, err
		}

		if records > 0 {
			w.WriteByte(',')
		}
		w.Write(row)
		records++
	}

	return records, rows.Err()
}

func Normalise(raw string) ([]byte, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()

	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}

	return json.Marshal(normaliseValue(v))
}

func normaliseValue(v interface{}) interface{} {
	switch t := v.(type) {
	case json.Number:
		if strings.ContainsAny(string(t), ".eE") {
			return t
		}
		if n, err := t.Int64(); err != nil || n > 1<<53 || n < -(1<<53) {
			return string(t)
		}
	case []interface{}:
		for i := range t {
			t[i] = normaliseValue(t[i])
		}
	case map[string]interface{}:
		for key, child := range t {
			t[key] = normaliseValue(child)
		}
		redact(t)
	}

	return v
}

func redact(m map[string]interface{}) {
	withhold(m, "token")
	withhold(m, "webhook_token")
	// Audit actions 202 and 403 log whole integrations and gallery listings, which carry the reviewer's ID.
	withhold(m, "reviewed_by")

	if m["is_secret"] == true {
		withhold(m, "header_value")
	}

	// Audit action 201 logs an integration's headers together with their values.
	if headers, ok := m["headers"].([]interface{}); ok {
		for _, header := range headers {
			if header, ok := header.(map[string]interface{}); ok {
				withhold(header, "value")
			}
		}
	}
}

func withhold(m map[string]interface{}, key string) {
	if _, ok := m[key]; ok {
		m[key] = withheldValue
	}
}
