package delivery

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore — транзакционный адаптер для демо и unit-тестов.
// Данные теряются при перезапуске. Копирование снимка не рассчитано на production.
type MemoryStore struct {
	mu       sync.Mutex
	notifs   map[string]Notification
	timers   map[string]map[int]time.Time
	receipts map[string]bool
	commands []SendCommand
	events   []Event
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{notifs: map[string]Notification{}, timers: map[string]map[int]time.Time{}, receipts: map[string]bool{}}
}

type memoryTx struct {
	notifs   map[string]Notification
	timers   map[string]map[int]time.Time
	receipts map[string]bool
	commands []SendCommand
	events   []Event
}

func cloneNotification(n Notification) Notification {
	n.Plan = append([]Step{}, n.Plan...)
	return n
}

func (s *MemoryStore) InTx(ctx context.Context, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := &memoryTx{notifs: map[string]Notification{}, timers: map[string]map[int]time.Time{}, receipts: map[string]bool{}}
	for id, n := range s.notifs {
		tx.notifs[id] = cloneNotification(n)
	}
	for id, steps := range s.timers {
		tx.timers[id] = map[int]time.Time{}
		for step, at := range steps {
			tx.timers[id][step] = at
		}
	}
	for key, value := range s.receipts {
		tx.receipts[key] = value
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.notifs, s.timers, s.receipts = tx.notifs, tx.timers, tx.receipts
	s.commands = append(s.commands, tx.commands...)
	s.events = append(s.events, tx.events...)
	return nil
}

func (t *memoryTx) Insert(_ context.Context, n *Notification) (bool, error) {
	if _, exists := t.notifs[n.ID]; exists {
		return false, nil
	}
	t.notifs[n.ID] = cloneNotification(*n)
	return true, nil
}
func (t *memoryTx) GetForUpdate(_ context.Context, id string) (*Notification, error) {
	n, ok := t.notifs[id]
	if !ok {
		return nil, ErrNotFound
	}
	n = cloneNotification(n)
	return &n, nil
}
func (t *memoryTx) Update(_ context.Context, n *Notification) error {
	t.notifs[n.ID] = cloneNotification(*n)
	return nil
}
func (t *memoryTx) EnqueueCommand(_ context.Context, c SendCommand) error {
	t.commands = append(t.commands, c)
	return nil
}
func (t *memoryTx) AppendEvent(_ context.Context, e Event) error {
	t.events = append(t.events, e)
	return nil
}
func (t *memoryTx) ScheduleTimer(_ context.Context, id string, step int, at time.Time) error {
	if t.timers[id] == nil {
		t.timers[id] = map[int]time.Time{}
	}
	t.timers[id][step] = at
	return nil
}
func (t *memoryTx) CancelTimers(_ context.Context, id string) error { delete(t.timers, id); return nil }
func (t *memoryTx) RememberReceipt(_ context.Context, r Receipt) (bool, error) {
	key := fmt.Sprintf("%s:%d:%s:%s", r.NotificationID, r.StepIdx, r.Channel, r.Status)
	if t.receipts[key] {
		return false, nil
	}
	t.receipts[key] = true
	return true, nil
}
func (t *memoryTx) ClaimDueTimers(_ context.Context, now time.Time, limit int) ([]TimerFired, error) {
	due := []TimerFired{}
	for id, steps := range t.timers {
		for step, at := range steps {
			if !at.After(now) {
				due = append(due, TimerFired{NotificationID: id, StepIdx: step})
			}
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].NotificationID == due[j].NotificationID {
			return due[i].StepIdx < due[j].StepIdx
		}
		return due[i].NotificationID < due[j].NotificationID
	})
	if len(due) > limit {
		due = due[:limit]
	}
	for _, timer := range due {
		delete(t.timers[timer.NotificationID], timer.StepIdx)
	}
	return due, nil
}

func (s *MemoryStore) Current(ctx context.Context, id string) (*Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n, exists := s.notifs[id]
	if !exists {
		return nil, ErrNotFound
	}
	n = cloneNotification(n)
	return &n, nil
}
func (s *MemoryStore) Notifications() []Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]Notification, 0, len(s.notifs))
	for _, n := range s.notifs {
		list = append(list, cloneNotification(n))
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return list[i].ID < list[j].ID
		}
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
	return list
}
func (s *MemoryStore) Commands() []SendCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SendCommand{}, s.commands...)
}
func (s *MemoryStore) Events(id string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []Event{}
	for _, e := range s.events {
		if id == "" || e.NotificationID == id {
			result = append(result, e)
		}
	}
	return result
}

type TimerView struct {
	NotificationID string    `json:"notification_id"`
	StepIdx        int       `json:"step_idx"`
	FireAt         time.Time `json:"fire_at"`
}

func (s *MemoryStore) Timers() []TimerView {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []TimerView{}
	for id, steps := range s.timers {
		for step, at := range steps {
			result = append(result, TimerView{id, step, at})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].FireAt.Equal(result[j].FireAt) {
			return result[i].NotificationID < result[j].NotificationID
		}
		return result[i].FireAt.Before(result[j].FireAt)
	})
	return result
}

type memoryAttempt struct {
	owner  string
	until  time.Time
	result *Receipt
}
type MemoryAttempts struct {
	mu       sync.Mutex
	attempts map[string]memoryAttempt
}

func NewMemoryAttempts() *MemoryAttempts {
	return &MemoryAttempts{attempts: map[string]memoryAttempt{}}
}
func (s *MemoryAttempts) Claim(ctx context.Context, id, owner string, now time.Time, lease time.Duration) (*Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a := s.attempts[id]
	if a.result != nil {
		r := *a.result
		return &r, nil
	}
	if now.Before(a.until) {
		return nil, ErrAttemptBusy
	}
	s.attempts[id] = memoryAttempt{owner: owner, until: now.Add(lease)}
	return nil, nil
}
func (s *MemoryAttempts) Complete(ctx context.Context, id, owner string, r Receipt, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	a, exists := s.attempts[id]
	if !exists || a.owner != owner || a.result != nil || !now.Before(a.until) {
		return ErrLeaseLost
	}
	a.result = &r
	s.attempts[id] = a
	return nil
}
func (s *MemoryAttempts) Release(ctx context.Context, id, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	a, exists := s.attempts[id]
	if exists && a.owner == owner && a.result == nil {
		delete(s.attempts, id)
	}
	return nil
}
