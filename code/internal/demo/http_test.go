package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/notification-platform/delivery"
)

func testEngine() *Engine { return NewEngine(time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)) }
func request(t *testing.T, h http.Handler, method, path, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, want, w.Body.String())
	}
	return w
}
func decodeSnapshot(t *testing.T, w *httptest.ResponseRecorder) Snapshot {
	t.Helper()
	var s Snapshot
	// Notification.Plan has custom output seconds; tests only inspect channels.
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestDefenseScenarios(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   delivery.State
		via     delivery.Channel
		submits int
	}{
		{"push-success", delivery.StateDelivered, delivery.Push, 1},
		{"failover", delivery.StateDelivered, delivery.SMS, 2},
		{"late-ack", delivery.StateDelivered, delivery.SMS, 2},
		{"retry", delivery.StateDelivered, delivery.SMS, 1},
		{"expired", delivery.StateExpired, "", 1},
		{"all-failed", delivery.StateFailed, "", 0},
		{"email-only", delivery.StateSent, delivery.Email, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testEngine()
			h := NewHandler(e)
			w := request(t, h, "POST", "/api/demo/scenarios/"+tc.name, "", 200)
			s := decodeSnapshot(t, w)
			if s.Notification.State != tc.state || s.Notification.DeliveredVia != tc.via || len(s.Submissions) != tc.submits {
				t.Fatalf("unexpected scenario result: %s", w.Body.String())
			}
			if len(s.Timers) != 0 || s.PendingRetries != 0 {
				t.Fatal("completed scenario has pending work")
			}
			if tc.name == "failover" && s.Submissions[1].Provider != "sms-aggregator-b" {
				t.Fatal("no provider failover")
			}
			if tc.name == "late-ack" && e.Overview().CrossChannelDuplicates != 1 {
				t.Fatal("missing duplicate metric")
			}
			if tc.name == "retry" {
				count := 0
				for _, event := range s.Events {
					if event.Type == "worker_retry" {
						count++
					}
				}
				if count != 2 {
					t.Fatalf("retry events=%d", count)
				}
			}
		})
	}
}
func TestManualFailoverReplayAndLateAck(t *testing.T) {
	e := testEngine()
	h := NewHandler(e)
	body := `{"idempotency_key":"manual","user_id":42,"simulation":{"push":"silent","sms":"silent"}}`
	first := decodeSnapshot(t, request(t, h, "POST", "/api/notifications", body, 201))
	id := first.Notification.ID
	if first.Notification.State != delivery.StateSending || len(first.Commands) != 1 {
		t.Fatal("push not started")
	}
	request(t, h, "POST", "/api/demo/advance", `{"seconds":10}`, 200)
	s := decodeSnapshot(t, request(t, h, "GET", "/api/notifications/"+id, "", 200))
	if s.Notification.StepIdx != 1 || len(s.Submissions) != 2 {
		t.Fatal("SMS not started")
	}
	s = decodeSnapshot(t, request(t, h, "POST", "/api/notifications/"+id+"/replay", "", 200))
	if len(s.Submissions) != 2 {
		t.Fatal("duplicate command produced another SMS")
	}
	request(t, h, "POST", "/api/notifications/"+id+"/receipts", `{"step_idx":1,"channel":"sms","status":"DELIVERED"}`, 200)
	late := `{"step_idx":0,"channel":"push","status":"DELIVERED"}`
	request(t, h, "POST", "/api/notifications/"+id+"/receipts", late, 200)
	request(t, h, "POST", "/api/notifications/"+id+"/receipts", late, 200)
	if e.Overview().CrossChannelDuplicates != 1 {
		t.Fatal("replayed receipt inflated duplicate count")
	}
	metrics := request(t, h, "GET", "/metrics", "", 200).Body.String()
	if !strings.Contains(metrics, "notification_demo_cross_channel_duplicates_total 1") {
		t.Fatal(metrics)
	}
}
func TestIdempotencyAndConflict(t *testing.T) {
	e := testEngine()
	h := NewHandler(e)
	body := `{"idempotency_key":"same-key","user_id":42,"simulation":{"push":"ack"}}`
	a := decodeSnapshot(t, request(t, h, "POST", "/api/notifications", body, 201))
	request(t, h, "POST", "/api/demo/advance", `{"seconds":100}`, 200)
	b := decodeSnapshot(t, request(t, h, "POST", "/api/notifications", body, 200))
	if a.Notification.ID != b.Notification.ID || !a.Notification.ExpiresAt.Equal(b.Notification.ExpiresAt) || len(b.Submissions) != 1 {
		t.Fatal("idempotency changed identity, TTL or submissions")
	}
	request(t, h, "POST", "/api/notifications", strings.Replace(body, `42`, `43`, 1), 409)
	if len(e.Overview().Notifications) != 1 {
		t.Fatal("conflicting request mutated state")
	}
}
func TestAdvanceProcessesEveryDeadlineAndUnknownOutcome(t *testing.T) {
	t.Run("all deadlines", func(t *testing.T) {
		e := testEngine()
		s, _, err := e.Create(context.Background(), CreateRequest{IdempotencyKey: "all", UserID: 1, Simulation: Simulation{SMS: "silent"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = e.Advance(context.Background(), 70*time.Second); err != nil {
			t.Fatal(err)
		}
		s, err = e.Snapshot(context.Background(), s.Notification.ID)
		if err != nil {
			t.Fatal(err)
		}
		if s.Notification.State != delivery.StateSent || len(s.Commands) != 3 {
			t.Fatal("jump skipped a deadline")
		}
	})
	t.Run("unknown outcome", func(t *testing.T) {
		e := testEngine()
		s, _, err := e.Create(context.Background(), CreateRequest{IdempotencyKey: "unknown", UserID: 1, Profile: &delivery.Profile{HasPhone: true}, Simulation: Simulation{SMS: "unknown_once"}})
		if err != nil {
			t.Fatal(err)
		}
		if s.Notification.State != delivery.StateSending || s.PendingRetries != 1 {
			t.Fatal("unknown outcome was treated as permanent error")
		}
		if _, err = e.Advance(context.Background(), 35*time.Second); err != nil {
			t.Fatal(err)
		}
		s, err = e.Snapshot(context.Background(), s.Notification.ID)
		if err != nil {
			t.Fatal(err)
		}
		if s.Notification.State != delivery.StateDelivered || len(s.Submissions) != 1 {
			t.Fatal("unknown outcome duplicated SMS or never recovered")
		}
	})
}
func TestAPIValidationAndOfflineAssets(t *testing.T) {
	h := NewHandler(testEngine())
	for _, body := range []string{`{}`, `null`, `{"idempotency_key":"a","user_id":1,"extra":true}`, `{"idempotency_key":"a","user_id":1,"ttl_seconds":-1}`, `{"idempotency_key":"a","user_id":1} {}`, `{"idempotency_key":"a","user_id":1,"profile":{"preferred_channel":"fax"}}`} {
		request(t, h, "POST", "/api/notifications", body, 400)
	}
	request(t, h, "GET", "/api/notifications/missing", "", 404)
	request(t, h, "POST", "/api/demo/advance", `{"seconds":0}`, 400)
	request(t, h, "POST", "/api/demo/advance", `{"seconds":9223372036854775807}`, 400)
	request(t, h, "POST", "/api/demo/scenarios/missing", "", 400)
	for _, path := range []string{"/healthz", "/swagger/", "/assets/vendor/swagger-ui-bundle.js", "/assets/vendor/swagger-ui.css", "/openapi.json"} {
		w := request(t, h, "GET", path, "", 200)
		if w.Body.Len() < 10 {
			t.Fatalf("empty asset %s", path)
		}
	}
	w := request(t, h, "GET", "/openapi.json", "", 200)
	var spec map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatal("invalid spec version")
	}
	page := request(t, h, "GET", "/swagger/", "", 200).Body.Bytes()
	if bytes.Contains(page, []byte("https://")) {
		t.Fatal("Swagger page depends on a CDN")
	}
}
func TestConcurrentHTTPRequests(t *testing.T) {
	h := NewHandler(testEngine())
	var wg sync.WaitGroup
	errs := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"idempotency_key":"concurrent-%d","user_id":42,"simulation":{"push":"ack"}}`, i)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/api/notifications", strings.NewReader(body)))
			if w.Code != 201 {
				errs <- w.Body.String()
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
func TestEmptyPlanAndReset(t *testing.T) {
	e := testEngine()
	h := NewHandler(e)
	w := request(t, h, "POST", "/api/notifications", `{"idempotency_key":"empty","user_id":42,"profile":{}}`, 201)
	if !strings.Contains(w.Body.String(), `"plan":[]`) {
		t.Fatal("empty plan must be a JSON array")
	}
	s := decodeSnapshot(t, w)
	if s.Notification.State != delivery.StateFailed || len(s.Commands) != 0 {
		t.Fatal("empty plan should fail explicitly")
	}
	request(t, h, "DELETE", "/api/demo", "", 200)
	if len(e.Overview().Notifications) != 0 || len(e.Overview().Timers) != 0 {
		t.Fatal("reset left state behind")
	}
}
