package demo

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"example.com/notification-platform/delivery"
)

//go:embed assets
var assets embed.FS

func NewHandler(engine *Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /swagger/{$}", func(w http.ResponseWriter, r *http.Request) {
		data, err := assets.ReadFile("assets/swagger.html")
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	mux.Handle("GET /assets/", http.FileServer(http.FS(assets)))
	mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, r *http.Request) {
		data, err := assets.ReadFile("assets/openapi.json")
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "mode": "demo"})
	})
	mux.HandleFunc("GET /api/demo", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, engine.Overview()) })
	mux.HandleFunc("GET /api/notifications", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, engine.Overview().Notifications) })
	mux.HandleFunc("POST /api/notifications", func(w http.ResponseWriter, r *http.Request) {
		var req CreateRequest
		if !readJSON(w, r, &req) {
			return
		}
		snapshot, created, err := engine.Create(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Location", "/api/notifications/"+snapshot.Notification.ID)
		code := http.StatusOK
		if created {
			code = http.StatusCreated
		}
		writeJSON(w, code, snapshot)
	})
	mux.HandleFunc("GET /api/notifications/{id}", func(w http.ResponseWriter, r *http.Request) {
		s, err := engine.Snapshot(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, s)
	})
	mux.HandleFunc("POST /api/notifications/{id}/receipts", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			StepIdx *int                   `json:"step_idx"`
			Channel delivery.Channel       `json:"channel"`
			Status  delivery.ReceiptStatus `json:"status"`
			Reason  string                 `json:"reason"`
		}
		if !readJSON(w, r, &req) {
			return
		}
		if req.StepIdx == nil {
			writeError(w, fmt.Errorf("%w: step_idx is required", delivery.ErrInvalid))
			return
		}
		s, err := engine.Receipt(r.Context(), r.PathValue("id"), delivery.Receipt{StepIdx: *req.StepIdx, Channel: req.Channel, Status: req.Status, Reason: req.Reason})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, s)
	})
	mux.HandleFunc("POST /api/notifications/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
		s, err := engine.Replay(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, s)
	})
	mux.HandleFunc("POST /api/demo/advance", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Seconds int `json:"seconds"`
		}
		if !readJSON(w, r, &req) {
			return
		}
		if req.Seconds < 1 || req.Seconds > 86400 {
			writeError(w, fmt.Errorf("%w: seconds must be 1..86400", delivery.ErrInvalid))
			return
		}
		o, err := engine.Advance(r.Context(), time.Duration(req.Seconds)*time.Second)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, o)
	})
	mux.HandleFunc("POST /api/demo/scenarios/{name}", func(w http.ResponseWriter, r *http.Request) {
		s, err := engine.Scenario(r.Context(), r.PathValue("name"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, s)
	})
	mux.HandleFunc("DELETE /api/demo", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, engine.Reset(time.Now())) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		o := engine.Overview()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintln(w, "# HELP notification_demo_notifications Current notifications by dispatch state.")
		fmt.Fprintln(w, "# TYPE notification_demo_notifications gauge")
		for _, state := range []delivery.State{delivery.StateSending, delivery.StateDelivered, delivery.StateSent, delivery.StateFailed, delivery.StateExpired} {
			fmt.Fprintf(w, "notification_demo_notifications{state=%q} %d\n", state, o.States[state])
		}
		fmt.Fprintln(w, "# HELP notification_demo_provider_submissions_total Simulated accepted provider submissions; reset on demo reset.")
		fmt.Fprintln(w, "# TYPE notification_demo_provider_submissions_total counter")
		fmt.Fprintf(w, "notification_demo_provider_submissions_total %d\n", o.ProviderSubmissions)
		fmt.Fprintln(w, "# HELP notification_demo_cross_channel_duplicates_total Confirmed delivery via an additional channel; reset on demo reset.")
		fmt.Fprintln(w, "# TYPE notification_demo_cross_channel_duplicates_total counter")
		fmt.Fprintf(w, "notification_demo_cross_channel_duplicates_total %d\n", o.CrossChannelDuplicates)
		fmt.Fprintln(w, "# HELP notification_demo_pending_commands Commands waiting for retry.")
		fmt.Fprintln(w, "# TYPE notification_demo_pending_commands gauge")
		fmt.Fprintf(w, "notification_demo_pending_commands %d\n", o.PendingCommands)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		mux.ServeHTTP(w, r)
	})
}
func readJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		writeError(w, fmt.Errorf("%w: invalid JSON: %v", delivery.ErrInvalid, err))
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		writeError(w, fmt.Errorf("%w: body must contain one JSON object", delivery.ErrInvalid))
		return false
	}
	return true
}
func writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, delivery.ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, delivery.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, delivery.ErrConflict):
		code = http.StatusConflict
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
