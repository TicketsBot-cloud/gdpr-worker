package gdprrelay

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/TicketsBot-cloud/common/gdprrelay"
	"github.com/TicketsBot-cloud/gdpr-worker/internal/config"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// The keys are the production names, so this only runs against a throwaway Redis.
func testRedis(t *testing.T) *redis.Client {
	url := os.Getenv("GDPR_TEST_REDIS_URL")
	if url == "" {
		t.Skip("GDPR_TEST_REDIS_URL is not set")
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(opts)
	reset := func() { client.Del(context.Background(), gdprrelay.KeyPending, keyProcessing, keyFailed) }
	reset()
	t.Cleanup(func() {
		reset()
		client.Close()
	})

	return client
}

func push(t *testing.T, client *redis.Client, key string, queued gdprrelay.QueuedRequest) {
	data, _ := json.Marshal(queued)
	if err := client.LPush(context.Background(), key, string(data)).Err(); err != nil {
		t.Fatal(err)
	}
}

func list(t *testing.T, client *redis.Client, key string) []gdprrelay.QueuedRequest {
	items, err := client.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}

	queued := make([]gdprrelay.QueuedRequest, len(items))
	for i, item := range items {
		if err := json.Unmarshal([]byte(item), &queued[i]); err != nil {
			t.Fatal(err)
		}
	}

	return queued
}

func TestRejectRetriesThenDeadLetters(t *testing.T) {
	client := testRedis(t)
	ctx := context.Background()
	logger := zap.NewNop()

	prev := config.Conf.MaxRetries
	config.Conf.MaxRetries = 2
	t.Cleanup(func() { config.Conf.MaxRetries = prev })

	// Two identical requests, with a's item behind b's, so only a RequestID match finds a.
	request := gdprrelay.GDPRRequest{Type: gdprrelay.RequestTypeAllTranscripts, UserId: 1, GuildIds: []uint64{2}}
	a, b := gdprrelay.QueuedRequest{Request: request, RequestID: 1}, gdprrelay.QueuedRequest{Request: request, RequestID: 2}
	push(t, client, keyProcessing, a)
	push(t, client, keyProcessing, b)

	if deadLettered, err := Reject(ctx, client, a, 3, 0, logger); deadLettered || err != nil {
		t.Fatalf("first reject: dead-lettered %v, err %v", deadLettered, err)
	}
	if pending := list(t, client, gdprrelay.KeyPending); len(pending) != 1 || pending[0].RequestID != 1 || pending[0].RetryCount != 1 || pending[0].TranscriptsDeleted != 3 {
		t.Fatalf("pending %+v, want request 1 with retry_count 1 and 3 deleted", pending)
	}

	if err := client.RPopLPush(ctx, gdprrelay.KeyPending, keyProcessing).Err(); err != nil {
		t.Fatal(err)
	}

	if deadLettered, err := Reject(ctx, client, a, 3, 0, logger); !deadLettered || err != nil {
		t.Fatalf("last reject: dead-lettered %v, err %v", deadLettered, err)
	}
	if failed := list(t, client, keyFailed); len(failed) != 1 || failed[0].RequestID != 1 || failed[0].RetryCount != 2 || failed[0].TranscriptsDeleted != 3 {
		t.Fatalf("failed %+v, want request 1 with retry_count 2 and 3 deleted", failed)
	}

	if err := Acknowledge(ctx, client, b, logger); err != nil {
		t.Fatal(err)
	}
	if processing := list(t, client, keyProcessing); len(processing) != 0 {
		t.Errorf("processing %+v, want empty", processing)
	}
}

func TestRecoverCountsStalledAttempt(t *testing.T) {
	client := testRedis(t)

	push(t, client, keyProcessing, gdprrelay.QueuedRequest{RequestID: 1, RetryCount: 1})
	if err := recoverStalledRequests(context.Background(), client, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	if pending := list(t, client, gdprrelay.KeyPending); len(pending) != 1 || pending[0].RetryCount != 2 {
		t.Errorf("pending %+v, want the request with retry_count 2", pending)
	}
	if processing := list(t, client, keyProcessing); len(processing) != 0 {
		t.Errorf("processing %+v, want empty", processing)
	}
}

func TestRestartOnlyCountsStartedRequest(t *testing.T) {
	client := testRedis(t)

	for id := 1; id <= 3; id++ {
		push(t, client, gdprrelay.KeyPending, gdprrelay.QueuedRequest{RequestID: id})
	}

	ch := make(chan gdprrelay.QueuedRequest)
	go Listen(client, ch, make(chan struct{}, 1), zap.NewNop())

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("no request dequeued")
	}
	// Give Listen time to pop a waiting request if it were going to.
	time.Sleep(300 * time.Millisecond)

	if err := recoverStalledRequests(context.Background(), client, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	pending, retries := list(t, client, gdprrelay.KeyPending), 0
	for _, queued := range pending {
		retries += queued.RetryCount
	}
	if len(pending) != 3 || retries != 1 {
		t.Errorf("pending %+v, want all 3 requests with one retry between them", pending)
	}
}
