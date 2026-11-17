// Package demo связывает delivery с локальными имитаторами провайдеров.
// Виртуальное время меняется только через Advance; внешних отправок нет.
package demo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"example.com/notification-platform/delivery"
)

type Simulation struct {
	Push             string `json:"push"`  // ack | silent | fail
	SMS              string `json:"sms"`   // delivered | silent | fail | unknown_once
	Email            string `json:"email"` // accepted | fail
	SMSPrimaryDown   bool   `json:"sms_primary_down"`
	ContactErrorOnce bool   `json:"contact_error_once"`
	ReceiptErrorOnce bool   `json:"receipt_error_once"`
}
type CreateRequest struct {
	IdempotencyKey string            `json:"idempotency_key"`
	UserID         int64             `json:"user_id"`
	Category       delivery.Category `json:"category"`
	Profile        *delivery.Profile `json:"profile,omitempty"`
	TTLSeconds     int               `json:"ttl_seconds"`
	Simulation     Simulation        `json:"simulation"`
}
type Submission struct {
	AttemptID string           `json:"attempt_id"`
	Channel   delivery.Channel `json:"channel"`
	Provider  string           `json:"provider"`
	At        time.Time        `json:"at"`
}
type Snapshot struct {
	Now            time.Time              `json:"now"`
	Notification   *delivery.Notification `json:"notification"`
	Events         []delivery.Event       `json:"events"`
	Commands       []delivery.SendCommand `json:"commands"`
	Timers         []delivery.TimerView   `json:"timers"`
	Submissions    []Submission           `json:"provider_submissions"`
	PendingRetries int                    `json:"pending_retries"`
}
type Overview struct {
	Mode                   string                  `json:"mode"`
	Now                    time.Time               `json:"now"`
	Notifications          []delivery.Notification `json:"notifications"`
	States                 map[delivery.State]int  `json:"states"`
	Timers                 []delivery.TimerView    `json:"timers"`
	PendingCommands        int                     `json:"pending_commands"`
	ProviderSubmissions    int                     `json:"provider_submissions"`
	CrossChannelDuplicates int                     `json:"cross_channel_duplicates"`
}
type workItem struct {
	cmd delivery.SendCommand
	at  time.Time
}

type Engine struct {
	mu              sync.Mutex
	now             time.Time
	store           *delivery.MemoryStore
	attempts        *delivery.MemoryAttempts
	orchestrator    *delivery.Orchestrator
	simulations     map[string]Simulation
	faults          map[string]bool
	submissions     map[string]Submission
	submissionOrder []string
	queue           []workItem
	cursor          int
}

func NewEngine(now time.Time) *Engine {
	e := &Engine{}
	e.reset(now)
	return e
}
func (e *Engine) reset(now time.Time) {
	e.now = now.UTC().Truncate(time.Second)
	e.store = delivery.NewMemoryStore()
	e.attempts = delivery.NewMemoryAttempts()
	e.orchestrator = &delivery.Orchestrator{Store: e.store, Now: func() time.Time { return e.now }, SendTimeout: 2 * time.Minute}
	e.simulations = map[string]Simulation{}
	e.faults = map[string]bool{}
	e.submissions = map[string]Submission{}
	e.submissionOrder = nil
	e.queue = nil
	e.cursor = 0
}
func normalize(req CreateRequest) (CreateRequest, error) {
	if len(req.IdempotencyKey) < 1 || len(req.IdempotencyKey) > 128 || req.UserID <= 0 {
		return req, fmt.Errorf("%w: idempotency_key must contain 1..128 bytes; user_id must be positive", delivery.ErrInvalid)
	}
	if req.Category == "" {
		req.Category = delivery.Transactional
	}
	if req.Category != delivery.Transactional && req.Category != delivery.Security {
		return req, fmt.Errorf("%w: unknown category", delivery.ErrInvalid)
	}
	if req.TTLSeconds == 0 {
		req.TTLSeconds = 3600
	}
	if req.TTLSeconds < 1 || req.TTLSeconds > 86400 {
		return req, fmt.Errorf("%w: ttl_seconds must be 1..86400", delivery.ErrInvalid)
	}
	if req.Profile == nil {
		req.Profile = &delivery.Profile{PushReady: true, HasPhone: true, HasEmail: true}
	} else {
		p := *req.Profile
		req.Profile = &p
	}
	if req.Profile.PreferredChannel == "" {
		req.Profile.PreferredChannel = delivery.Push
	}
	if req.Profile.PreferredChannel != delivery.Push && req.Profile.PreferredChannel != delivery.SMS {
		return req, fmt.Errorf("%w: preferred_channel must be push or sms", delivery.ErrInvalid)
	}
	s := &req.Simulation
	if s.Push == "" {
		s.Push = "silent"
	}
	if s.SMS == "" {
		s.SMS = "delivered"
	}
	if s.Email == "" {
		s.Email = "accepted"
	}
	if s.Push != "ack" && s.Push != "silent" && s.Push != "fail" {
		return req, fmt.Errorf("%w: unknown push simulation", delivery.ErrInvalid)
	}
	if s.SMS != "delivered" && s.SMS != "silent" && s.SMS != "fail" && s.SMS != "unknown_once" {
		return req, fmt.Errorf("%w: unknown SMS simulation", delivery.ErrInvalid)
	}
	if s.Email != "accepted" && s.Email != "fail" {
		return req, fmt.Errorf("%w: unknown email simulation", delivery.ErrInvalid)
	}
	return req, nil
}
func (e *Engine) Create(ctx context.Context, req CreateRequest) (Snapshot, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.create(ctx, req)
}
func (e *Engine) create(ctx context.Context, req CreateRequest) (Snapshot, bool, error) {
	req, err := normalize(req)
	if err != nil {
		return Snapshot{}, false, err
	}
	data, err := json.Marshal(req)
	if err != nil {
		return Snapshot{}, false, err
	}
	inputHash := sha256.Sum256(data)
	idHash := sha256.Sum256([]byte("demo-producer\x00" + req.IdempotencyKey))
	id := "n_" + hex.EncodeToString(idHash[:12])
	old, lookupErr := e.store.Current(ctx, id)
	if lookupErr == nil {
		if old.InputHash != hex.EncodeToString(inputHash[:]) {
			return Snapshot{}, false, delivery.ErrConflict
		}
		snapshot, err := e.snapshot(ctx, id)
		return snapshot, false, err
	}
	if !errors.Is(lookupErr, delivery.ErrNotFound) {
		return Snapshot{}, false, lookupErr
	}
	if len(e.store.Notifications()) >= 200 {
		return Snapshot{}, false, fmt.Errorf("%w: demo limit is 200 notifications; reset the demo", delivery.ErrConflict)
	}
	policy := delivery.Policy{PushAckTimeout: 10 * time.Second, SecurityPushAckTimeout: 5 * time.Second, SMSDeliveryTimeout: 60 * time.Second}
	n := &delivery.Notification{ID: id, UserID: req.UserID, Category: req.Category, Plan: delivery.BuildPlan(req.Category, *req.Profile, policy),
		ExpiresAt: e.now.Add(time.Duration(req.TTLSeconds) * time.Second), InputHash: hex.EncodeToString(inputHash[:])}
	if err := e.orchestrator.Accept(ctx, n); err != nil {
		return Snapshot{}, false, err
	}
	e.simulations[id] = req.Simulation
	if err := e.pump(ctx); err != nil {
		return Snapshot{}, false, err
	}
	snapshot, err := e.snapshot(ctx, id)
	return snapshot, true, err
}
func (e *Engine) Snapshot(ctx context.Context, id string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshot(ctx, id)
}
func (e *Engine) snapshot(ctx context.Context, id string) (Snapshot, error) {
	n, err := e.store.Current(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{Now: e.now, Notification: n, Events: e.store.Events(id), Commands: []delivery.SendCommand{}, Timers: []delivery.TimerView{}, Submissions: []Submission{}}
	for _, cmd := range e.store.Commands() {
		if cmd.NotificationID == id {
			s.Commands = append(s.Commands, cmd)
		}
	}
	for _, timer := range e.store.Timers() {
		if timer.NotificationID == id {
			s.Timers = append(s.Timers, timer)
		}
	}
	for _, key := range e.submissionOrder {
		if sub := e.submissions[key]; len(sub.AttemptID) > len(id) && sub.AttemptID[:len(id)+1] == id+":" {
			s.Submissions = append(s.Submissions, sub)
		}
	}
	for _, item := range e.queue {
		if item.cmd.NotificationID == id {
			s.PendingRetries++
		}
	}
	return s, nil
}
func (e *Engine) Overview() Overview {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.overview()
}
func (e *Engine) overview() Overview {
	result := Overview{Mode: "demo / in-memory / virtual clock / simulated providers", Now: e.now, Notifications: e.store.Notifications(),
		States: map[delivery.State]int{}, Timers: e.store.Timers(), PendingCommands: len(e.queue), ProviderSubmissions: len(e.submissions)}
	for _, n := range result.Notifications {
		result.States[n.State]++
	}
	for _, event := range e.store.Events("") {
		if event.Type == "cross_channel_duplicate" {
			result.CrossChannelDuplicates++
		}
	}
	return result
}
func (e *Engine) Receipt(ctx context.Context, id string, r delivery.Receipt) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r.NotificationID = id
	if err := e.orchestrator.OnReceipt(ctx, r); err != nil {
		return Snapshot{}, err
	}
	if err := e.pump(ctx); err != nil {
		return Snapshot{}, err
	}
	return e.snapshot(ctx, id)
}
func (e *Engine) Replay(ctx context.Context, id string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.store.Current(ctx, id); err != nil {
		return Snapshot{}, err
	}
	commands := e.store.Commands()
	for i := len(commands) - 1; i >= 0; i-- {
		if commands[i].NotificationID == id {
			e.queue = append(e.queue, workItem{commands[i], e.now})
			if err := e.event(ctx, id, "command_replayed", commands[i].Channel, commands[i].AttemptID); err != nil {
				return Snapshot{}, err
			}
			if err := e.pump(ctx); err != nil {
				return Snapshot{}, err
			}
			return e.snapshot(ctx, id)
		}
	}
	return Snapshot{}, fmt.Errorf("%w: no command to replay", delivery.ErrInvalid)
}
func (e *Engine) Advance(ctx context.Context, d time.Duration) (Overview, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.advance(ctx, d); err != nil {
		return Overview{}, err
	}
	return e.overview(), nil
}
func (e *Engine) advance(ctx context.Context, d time.Duration) error {
	if d <= 0 || d > 24*time.Hour {
		return fmt.Errorf("%w: advance must be > 0 and <= 86400 seconds", delivery.ErrInvalid)
	}
	target := e.now.Add(d)
	for i := 0; i < 10000; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		next := target
		for _, timer := range e.store.Timers() {
			if timer.FireAt.Before(next) {
				next = timer.FireAt
			}
		}
		for _, item := range e.queue {
			if item.at.Before(next) {
				next = item.at
			}
		}
		if next.Before(e.now) {
			next = e.now
		}
		e.now = next
		if _, err := e.orchestrator.FireDueTimers(ctx, 1000); err != nil {
			return err
		}
		if err := e.pump(ctx); err != nil {
			return err
		}
		if !e.now.Before(target) {
			return nil
		}
	}
	return errors.New("demo processing limit exceeded")
}
func (e *Engine) Reset(now time.Time) Overview {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reset(now)
	return e.overview()
}
func (e *Engine) event(ctx context.Context, id, kind string, ch delivery.Channel, reason string) error {
	return e.store.InTx(ctx, func(tx delivery.Tx) error {
		return tx.AppendEvent(ctx, delivery.Event{NotificationID: id, Type: kind, Channel: ch, Reason: reason, At: e.now})
	})
}
func (e *Engine) pump(ctx context.Context) error {
	for i := 0; i < 1000; i++ {
		commands := e.store.Commands()
		for _, cmd := range commands[e.cursor:] {
			e.queue = append(e.queue, workItem{cmd, e.now})
		}
		e.cursor = len(commands)
		idx := -1
		for j, item := range e.queue {
			if !item.at.After(e.now) {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil
		}
		item := e.queue[idx]
		e.queue = append(e.queue[:idx], e.queue[idx+1:]...)
		if err := e.handle(ctx, item.cmd); err != nil {
			if errors.Is(err, delivery.ErrInvalid) || errors.Is(err, delivery.ErrNotFound) {
				return err
			}
			if ctx.Err() != nil {
				// Отмена HTTP-запроса не должна терять уже принятую команду.
				e.queue = append(e.queue, workItem{item.cmd, e.now.Add(time.Second)})
				return ctx.Err()
			}
			delay := time.Second
			if errors.Is(err, delivery.ErrAttemptBusy) {
				delay = 5 * time.Second
			}
			if err := e.event(ctx, item.cmd.NotificationID, "worker_retry", item.cmd.Channel, err.Error()); err != nil {
				return err
			}
			e.queue = append(e.queue, workItem{item.cmd, e.now.Add(delay)})
		}
	}
	return errors.New("demo command processing limit exceeded")
}
func (e *Engine) submit(ctx context.Context, cmd delivery.SendCommand, provider string) error {
	key := provider + ":" + cmd.AttemptID
	if _, exists := e.submissions[key]; exists {
		return nil
	} // идемпотентность имитатора провайдера
	if err := e.event(ctx, cmd.NotificationID, "provider_submit", cmd.Channel, provider+" / "+cmd.AttemptID); err != nil {
		return err
	}
	e.submissions[key] = Submission{cmd.AttemptID, cmd.Channel, provider, e.now}
	e.submissionOrder = append(e.submissionOrder, key)
	return nil
}
func (e *Engine) once(id, kind string) bool {
	key := id + ":" + kind
	if e.faults[key] {
		return false
	}
	e.faults[key] = true
	return true
}
func (e *Engine) handle(ctx context.Context, cmd delivery.SendCommand) error {
	if cmd.Channel == delivery.SMS {
		adapter := &smsAdapter{e: e, cmd: cmd}
		s := delivery.SMSSender{State: e.store, Attempts: e.attempts, Contacts: adapter, Render: adapter, Router: adapter, Results: adapter, Now: func() time.Time { return e.now }}
		return s.Handle(ctx, cmd)
	}
	n, err := e.store.Current(ctx, cmd.NotificationID)
	if err != nil {
		return err
	}
	if n.State.Terminal() || n.StepIdx != cmd.StepIdx || !e.now.Before(cmd.ExpiresAt) {
		return nil
	}
	sim := e.simulations[n.ID]
	r := delivery.Receipt{NotificationID: n.ID, StepIdx: cmd.StepIdx, Channel: cmd.Channel, Status: delivery.ReceiptAccepted}
	if (cmd.Channel == delivery.Push && sim.Push == "fail") || (cmd.Channel == delivery.Email && sim.Email == "fail") {
		if err := e.event(ctx, n.ID, "provider_rejected", cmd.Channel, "permanent simulated failure"); err != nil {
			return err
		}
		r.Status, r.Reason = delivery.ReceiptFailed, "simulated permanent failure"
		return e.orchestrator.OnReceipt(ctx, r)
	}
	provider := "push-simulator"
	if cmd.Channel == delivery.Email {
		provider = "email-simulator"
	}
	if err := e.submit(ctx, cmd, provider); err != nil {
		return err
	}
	if err := e.orchestrator.OnReceipt(ctx, r); err != nil {
		return err
	}
	if cmd.Channel == delivery.Push && sim.Push == "ack" {
		r.Status = delivery.ReceiptDelivered
		return e.orchestrator.OnReceipt(ctx, r)
	}
	return nil
}

type smsAdapter struct {
	e   *Engine
	cmd delivery.SendCommand
}

func (s *smsAdapter) Phone(context.Context, int64) (string, error) {
	if s.e.simulations[s.cmd.NotificationID].ContactErrorOnce && s.e.once(s.cmd.NotificationID, "contacts") {
		return "", errors.New("simulated profile outage")
	}
	return "+79990000000", nil
}
func (s *smsAdapter) Render(context.Context, string, delivery.Channel) (string, error) {
	return "Демо: списание 1500 RUB, карта *1234", nil
}
func (s *smsAdapter) Send(ctx context.Context, m delivery.SMSMessage) (string, error) {
	sim := s.e.simulations[s.cmd.NotificationID]
	if sim.SMS == "fail" {
		return "", delivery.ErrPermanent
	}
	provider := "sms-aggregator-a"
	if sim.SMSPrimaryDown {
		if err := s.e.event(ctx, s.cmd.NotificationID, "provider_rejected", delivery.SMS, "aggregator A: confirmed 503, fallback to B"); err != nil {
			return "", err
		}
		provider = "sms-aggregator-b"
	}
	if m.ClientMsgID != s.cmd.AttemptID {
		return "", delivery.ErrInvalid
	}
	if err := s.e.submit(ctx, s.cmd, provider); err != nil {
		return "", err
	}
	if sim.SMS == "unknown_once" && s.e.once(s.cmd.NotificationID, "unknown") {
		return "", context.DeadlineExceeded
	}
	return provider, nil
}
func (s *smsAdapter) Publish(ctx context.Context, r delivery.Receipt) error {
	sim := s.e.simulations[s.cmd.NotificationID]
	if sim.ReceiptErrorOnce && s.e.once(s.cmd.NotificationID, "receipt") {
		return errors.New("simulated receipt publication outage")
	}
	if err := s.e.orchestrator.OnReceipt(ctx, r); err != nil {
		return err
	}
	if r.Status == delivery.ReceiptAccepted && (sim.SMS == "delivered" || sim.SMS == "unknown_once") {
		r.Status = delivery.ReceiptDelivered
		return s.e.orchestrator.OnReceipt(ctx, r)
	}
	return nil
}

func (e *Engine) Scenario(ctx context.Context, name string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	req := CreateRequest{IdempotencyKey: name + "-" + rand.Text(), UserID: 42, Simulation: Simulation{Push: "ack"}}
	switch name {
	case "push-success":
	case "failover", "late-ack":
		req.Simulation = Simulation{Push: "silent", SMS: "delivered", SMSPrimaryDown: true}
	case "retry":
		req.Profile = &delivery.Profile{HasPhone: true}
		req.Simulation = Simulation{ContactErrorOnce: true, ReceiptErrorOnce: true}
	case "expired":
		req.TTLSeconds = 5
		req.Simulation.Push = "silent"
	case "all-failed":
		req.Simulation = Simulation{Push: "fail", SMS: "fail", Email: "fail"}
	case "email-only":
		req.Profile = &delivery.Profile{HasEmail: true}
	default:
		return Snapshot{}, fmt.Errorf("%w: unknown scenario", delivery.ErrInvalid)
	}
	snapshot, _, err := e.create(ctx, req)
	if err != nil {
		return Snapshot{}, err
	}
	id := snapshot.Notification.ID
	switch name {
	case "failover", "late-ack":
		err = e.advance(ctx, 10*time.Second)
	case "retry":
		err = e.advance(ctx, 2*time.Second)
	case "expired":
		err = e.advance(ctx, 5*time.Second)
	}
	if err != nil {
		return Snapshot{}, err
	}
	if name == "late-ack" {
		if err := e.orchestrator.OnReceipt(ctx, delivery.Receipt{NotificationID: id, StepIdx: 0, Channel: delivery.Push, Status: delivery.ReceiptDelivered}); err != nil {
			return Snapshot{}, err
		}
	}
	return e.snapshot(ctx, id)
}
