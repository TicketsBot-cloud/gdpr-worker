package processor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdl/rest/request"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"go.uber.org/zap"
)

func TestTicketsInGuilds(t *testing.T) {
	refs := []dbmodel.TranscriptRef{{GuildId: 1, TicketId: 5}, {GuildId: 2, TicketId: 6}, {GuildId: 3, TicketId: 7}, {GuildId: 3, TicketId: 8}}

	want := []dbmodel.TranscriptRef{refs[0], refs[2], refs[3]}
	if got := ticketsInGuilds(slices.Clone(refs), []uint64{3, 1}); !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got := ticketsInGuilds(refs, nil); len(got) != 0 {
		t.Errorf("no servers selected, got %v", got)
	}
}

// gdl's pre-request hooks can't be removed and run in registration order, so the latest fake wins.
func fakeDiscord(t *testing.T, handler http.HandlerFunc) {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	request.RegisterPreRequestHook(func(_ string, req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = srv.Listener.Addr().String()
	})
}

func TestOwnershipErrorBlamesBotOnlyWhenMissing(t *testing.T) {
	// Each guild ID doubles as the HTTP status the fake Discord answers with.
	fakeDiscord(t, func(w http.ResponseWriter, r *http.Request) {
		status, _ := strconv.Atoi(path.Base(r.URL.Path))
		w.WriteHeader(status)
		w.Write([]byte(`{"message":"error","code":0}`))
	})

	p := &Processor{logger: zap.NewNop(), rateLimiter: ratelimit.NewRateLimiter(ratelimit.NewMemoryStore(), 0)}

	for guildId, botMissing := range map[uint64]bool{403: true, 404: true, 500: false, 502: false} {
		err := p.verifyGuildOwnership(context.Background(), guildId, 1)
		if err == nil {
			t.Fatalf("guild %d: expected an error", guildId)
		}
		if got := strings.Contains(err.Error(), "the bot is not in server"); got != botMissing {
			t.Errorf("guild %d: %q", guildId, err)
		}
	}
}

func TestTranscriptDeletionCountsOnlyRealDeletions(t *testing.T) {
	statuses := map[string]int{"1": http.StatusNoContent, "2": http.StatusNotFound, "3": http.StatusInternalServerError}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statuses[r.URL.Query().Get("id")])
		w.Write([]byte(`{"message":"client not found"}`))
	}))
	t.Cleanup(srv.Close)

	prevProxy, prevClient := archiver.Proxy, archiver.Client
	archiver.Proxy = archiverclient.NewProxyRetriever(srv.URL)
	archiver.Client = archiverclient.NewArchiverClient(archiver.Proxy, nil)
	t.Cleanup(func() { archiver.Proxy, archiver.Client = prevProxy, prevClient })

	p := &Processor{logger: zap.NewNop()}
	ctx := context.Background()

	for ticketId, wantDeleted := range map[int]bool{1: true, 2: true, 3: false} {
		if err := p.deleteTranscript(ctx, 9, ticketId); (err == nil) != wantDeleted {
			t.Errorf("ticket %d: err %v, want deleted %v", ticketId, err, wantDeleted)
		}
	}

	// Only a failing ticket, so the has_transcript update (database) is never reached.
	if deleted, err := p.deleteTranscripts(ctx, 9, []int{3}); deleted != 0 || err == nil {
		t.Errorf("got %d, %v; want 0 and an error", deleted, err)
	}

	if _, err := p.getTranscript(ctx, 9, 2); err == nil || !archiverclient.IsNotFound(err) {
		t.Errorf("missing transcript: got %v, want an error IsNotFound accepts", err)
	}

	if _, err := p.getTranscript(ctx, 9, 3); err == nil || archiverclient.IsNotFound(err) {
		t.Errorf("unloaded bucket client: got %v, want an error IsNotFound rejects", err)
	}
}

func TestGuildExportRejectsNonOwner(t *testing.T) {
	fakeDiscord(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"2","owner_id":"9"}`))
	})

	p := &Processor{logger: zap.NewNop(), rateLimiter: ratelimit.NewRateLimiter(ratelimit.NewMemoryStore(), 0)}

	_, err := p.guildExportJob(context.Background(), gdprrelay.GDPRRequest{Type: gdprrelay.RequestTypeExportGuild, UserId: 1, ExportId: 7, GuildIds: []uint64{2}, Categories: []string{"settings"}})
	if err == nil || !strings.Contains(err.Error(), "not the owner") {
		t.Errorf("got %v, want a not-the-owner error", err)
	}
}
