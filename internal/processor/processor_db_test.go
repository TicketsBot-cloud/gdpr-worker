package processor

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/TicketsBot-cloud/archiverclient"
	"github.com/TicketsBot-cloud/common/gdprrelay"
	dbmodel "github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdl/rest/ratelimit"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/archiver"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/database"
	"github.com/TicketsBot-cloud/logarchiver/pkg/model"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

func TestAnonymizationFailsUnlessEveryTranscriptIsCleaned(t *testing.T) {
	dsn := os.Getenv("GDPR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GDPR_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	prevDb := database.Client
	database.Client = dbmodel.NewDatabase(pool)
	t.Cleanup(func() { database.Client = prevDb })
	database.Client.CreateTables(ctx, pool)

	var mu sync.Mutex
	objects := map[string][]byte{}
	failGet, failDelete := map[string]bool{}, map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		key := r.URL.Query().Get("guild") + "/" + r.URL.Query().Get("id")
		switch {
		case r.Method == http.MethodPost:
			objects[key], _ = io.ReadAll(r.Body)
		case failGet[key], r.Method == http.MethodDelete && failDelete[key]:
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"message":"client not found"}`))
		case objects[key] == nil:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"message":"ticket not found"}`))
		case r.Method == http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Write(objects[key])
		}
	}))
	t.Cleanup(srv.Close)

	prevProxy, prevClient := archiver.Proxy, archiver.Client
	archiver.Proxy = archiverclient.NewProxyRetriever(srv.URL)
	archiver.Client = archiverclient.NewArchiverClient(archiver.Proxy, []byte("0123456789abcdef0123456789abcdef"))
	t.Cleanup(func() { archiver.Proxy, archiver.Client = prevProxy, prevClient })

	guildId, userId, otherId := rand.Uint64N(1<<62)+1, rand.Uint64N(1<<62)+1, rand.Uint64N(1<<62)+1
	for id := 1; id <= 3; id++ {
		if _, err := pool.Exec(ctx, `INSERT INTO tickets (id, guild_id, user_id, open, open_time, has_transcript, status) VALUES ($1, $2, $3, false, NOW(), true, 'CLOSED'::ticket_status)`, id, guildId, userId); err != nil {
			t.Fatal(err)
		}
	}

	store := func(ticketId int, authors ...uint64) {
		transcript := v2.Transcript{Version: model.V2}
		for _, author := range authors {
			transcript.Messages = append(transcript.Messages, v2.Message{AuthorId: author, Content: "hi"})
		}
		if err := archiver.Client.StoreTranscript(ctx, guildId, ticketId, transcript); err != nil {
			t.Fatal(err)
		}
	}

	// Ticket 1 holds two of the user's messages, ticket 2 is missing from the archive, ticket 3 can't be read.
	store(1, userId, otherId, userId)
	store(3, userId)
	failGet[strconv.FormatUint(guildId, 10)+"/3"] = true

	fakeDiscord(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"id":"%d","owner_id":"%d"}`, guildId, userId)
	})

	p := &Processor{logger: zap.NewNop(), rateLimiter: ratelimit.NewRateLimiter(ratelimit.NewMemoryStore(), 0)}
	all := gdprrelay.GDPRRequest{Type: gdprrelay.RequestTypeAllMessages, UserId: userId, GuildIds: []uint64{guildId}}

	if res := p.Process(ctx, all); res.MessagesDeleted != 2 || res.Error != errAnonymizationFailed {
		t.Errorf("one unreadable transcript: got %d, %v; want 2, %v", res.MessagesDeleted, res.Error, errAnonymizationFailed)
	}

	mu.Lock()
	clear(failGet)
	mu.Unlock()

	if res := p.Process(ctx, all); res.MessagesDeleted != 1 || res.Error != nil {
		t.Errorf("retry: got %d, %v; want 1, nil", res.MessagesDeleted, res.Error)
	}

	missingOnly := gdprrelay.GDPRRequest{Type: gdprrelay.RequestTypeSpecificMessages, UserId: userId, GuildIds: []uint64{guildId}, TicketIds: []int{2}}
	if res := p.Process(ctx, missingOnly); res.MessagesDeleted != 0 || res.Error != nil {
		t.Errorf("missing transcript only: got %d, %v; want 0, nil", res.MessagesDeleted, res.Error)
	}

	// Ticket 1 is deleted, ticket 2 is already missing from the archive and ticket 3's delete fails.
	mu.Lock()
	failDelete[strconv.FormatUint(guildId, 10)+"/3"] = true
	mu.Unlock()

	allTranscripts := gdprrelay.GDPRRequest{Type: gdprrelay.RequestTypeAllTranscripts, UserId: userId, GuildIds: []uint64{guildId}}
	if res := p.Process(ctx, allTranscripts); res.TranscriptsDeleted != 2 || res.Error != errTranscriptDeletionFailed {
		t.Errorf("one failed delete: got %d, %v; want 2, %v", res.TranscriptsDeleted, res.Error, errTranscriptDeletionFailed)
	}
}
