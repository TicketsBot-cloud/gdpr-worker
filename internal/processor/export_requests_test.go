package processor

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/export"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
)

type fakeRows struct {
	pgx.Rows
	rows []string
}

func (r *fakeRows) Next() bool { return len(r.rows) > 0 }

func (r *fakeRows) Scan(dest ...interface{}) error {
	*dest[0].(*string), r.rows = r.rows[0], r.rows[1:]
	return nil
}

func (r *fakeRows) Err() error { return nil }

func (r *fakeRows) Close() {}

type blacklistQuerier struct{}

func (blacklistQuerier) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	if strings.Contains(sql, " FROM blacklist ") {
		return &fakeRows{rows: []string{`{}`}}, nil
	}
	return &fakeRows{}, nil
}

func writeTestArchive(t *testing.T, p *Processor, job *exportJob) ([]string, export.Manifest) {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	if err := p.writeArchive(context.Background(), zw, job); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	var manifest export.Manifest
	for _, f := range zr.File {
		names = append(names, f.Name)
		if f.Name != "manifest.json" {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(rc).Decode(&manifest)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	return names, manifest
}

func TestExportWritesSelectedCategories(t *testing.T) {
	fakeDiscord(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"id":"2","owner_id":"1"}`))
	})

	p := &Processor{logger: zap.NewNop(), db: blacklistQuerier{}}

	for _, tc := range []struct {
		build      func(context.Context, gdprrelay.GDPRRequest) (*exportJob, error)
		request    gdprrelay.GDPRRequest
		files      []string
		categories []string
		withheld   string
	}{
		{
			p.userExportJob,
			gdprrelay.GDPRRequest{UserId: 1, ExportId: 7, Categories: []string{"blacklists", "settings"}},
			[]string{"blacklists.json", "manifest.json", "settings.json"},
			[]string{"settings", "blacklists"},
			"email_verification_codes",
		},
		{
			p.guildExportJob,
			gdprrelay.GDPRRequest{UserId: 1, ExportId: 8, GuildIds: []uint64{2}, Categories: []string{"blacklist", "staff"}},
			[]string{"2/blacklist.json", "2/staff.json", "manifest.json"},
			[]string{"staff", "blacklist"},
			"server_blacklist",
		},
	} {
		job, err := tc.build(context.Background(), tc.request)
		if err != nil {
			t.Fatal(err)
		}

		names, manifest := writeTestArchive(t, p, job)

		if names[len(names)-1] != "manifest.json" {
			t.Errorf("manifest.json is not the last entry: %v", names)
		}

		slices.Sort(names)
		if !slices.Equal(names, tc.files) {
			t.Errorf("archive holds %v, want %v", names, tc.files)
		}

		if manifest.ExportId != tc.request.ExportId || manifest.Files.Written != len(tc.files)-1 {
			t.Errorf("manifest export_id %d, files %d", manifest.ExportId, manifest.Files.Written)
		}

		if !slices.Equal(manifest.Categories, tc.categories) {
			t.Errorf("manifest categories %v, want %v", manifest.Categories, tc.categories)
		}

		for _, source := range manifest.Sources {
			if !slices.Contains(tc.categories, source.Category) {
				t.Errorf("manifest lists %s from unselected %s", source.Name, source.Category)
			}
		}

		if !slices.ContainsFunc(manifest.Sources, func(s export.ManifestSource) bool { return s.Name == tc.withheld && !s.Included }) {
			t.Errorf("manifest does not withhold %s", tc.withheld)
		}
	}
}

type tierQuerier struct {
	tier    string
	queries []string
}

func (q *tierQuerier) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	q.queries = append(q.queries, sql)
	if strings.Contains(sql, "FROM bot_staff") && q.tier != "" {
		return &fakeRows{rows: []string{q.tier}}, nil
	}
	return &fakeRows{}, nil
}

func TestFlagReasonsFollowDashboardAccess(t *testing.T) {
	config.Conf.Owner = 99
	t.Cleanup(func() { config.Conf.Owner = 0 })

	for _, tc := range []struct {
		userId uint64
		tier   string
		sees   bool
	}{
		{1, "", false},
		{1, "helper", false},
		{1, "admin", true},
		{99, "", true},
	} {
		q := &tierQuerier{tier: tc.tier}
		p := &Processor{logger: zap.NewNop(), db: q}
		job, err := p.userExportJob(context.Background(), gdprrelay.GDPRRequest{UserId: tc.userId, Categories: []string{"affiliate"}})
		if err != nil {
			t.Fatal(err)
		}
		writeTestArchive(t, p, job)

		selected := slices.ContainsFunc(q.queries, func(sql string) bool { return strings.Contains(sql, `"voided_reason"`) })
		if selected != tc.sees {
			t.Errorf("user %d, tier %q: selects voided_reason = %v, want %v", tc.userId, tc.tier, selected, tc.sees)
		}
	}
}

func TestExportCategoriesTicketIds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request gdprrelay.GDPRRequest
		ok      bool
	}{
		{"no categories", gdprrelay.GDPRRequest{GuildIds: []uint64{1}}, false},
		{"ticket IDs without transcripts", gdprrelay.GDPRRequest{Categories: []string{"settings"}, GuildIds: []uint64{1}, TicketIds: []int{1}}, false},
		{"ticket IDs across two servers", gdprrelay.GDPRRequest{Categories: []string{dbmodel.GdprExportCategoryTranscripts}, GuildIds: []uint64{1, 2}, TicketIds: []int{1}}, false},
		{"ticket IDs for one server", gdprrelay.GDPRRequest{Categories: []string{dbmodel.GdprExportCategoryTranscripts}, GuildIds: []uint64{1}, TicketIds: []int{1}}, true},
	} {
		if _, err := exportCategories(dbmodel.GdprExportTypeServer, tc.request); (err == nil) != tc.ok {
			t.Errorf("%s: err %v", tc.name, err)
		}
	}
}

func TestPersonalTranscriptHoldsOnlyRequester(t *testing.T) {
	transcript := func() v2.Transcript {
		return v2.Transcript{
			Entities: v2.Entities{
				Users:    map[uint64]v2.User{1: {Id: 1, Username: "requester"}, 2: {Id: 2, Username: "other"}},
				Channels: map[uint64]v2.Channel{9: {Id: 9}},
				Roles:    map[uint64]v2.Role{8: {Id: 8}},
			},
			Messages: []v2.Message{{Id: 10, AuthorId: 1}, {Id: 11, AuthorId: 2}, {Id: 12, AuthorId: 1}},
		}
	}

	tr := transcript()
	if !filterTranscriptToUser(&tr, 1) {
		t.Fatal("transcript with the requester's messages was dropped")
	}
	if len(tr.Messages) != 2 || tr.Messages[0].Id != 10 || tr.Messages[1].Id != 12 {
		t.Errorf("kept messages %+v, want only the requester's", tr.Messages)
	}
	if len(tr.Entities.Users) != 1 || tr.Entities.Users[1].Username != "requester" || tr.Entities.Channels != nil || tr.Entities.Roles != nil {
		t.Errorf("kept entities %+v, want only the requester", tr.Entities)
	}

	tr = transcript()
	if filterTranscriptToUser(&tr, 3) {
		t.Error("transcript holding only other participants' messages was kept")
	}

	tr = transcript()
	delete(tr.Entities.Users, 1)
	if !filterTranscriptToUser(&tr, 1) || len(tr.Entities.Users) != 1 || tr.Entities.Users[1].Id != 1 {
		t.Errorf("missing requester entity not keyed by the requested id: %+v", tr.Entities.Users)
	}
}
