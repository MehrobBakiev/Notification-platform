package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type State string

const (
	StateSending   State = "SENDING"
	StateDelivered State = "DELIVERED" // есть ACK приложения или DLR оператора
	StateSent      State = "SENT"      // email принят провайдером, подтверждения не будет
	StateFailed    State = "FAILED"    // все шаги плана исчерпаны
	StateExpired   State = "EXPIRED"   // истёк TTL уведомления
)

func (s State) Terminal() bool {
	switch s {
	case StateDelivered, StateSent, StateFailed, StateExpired:
		return true
	}
	return false
}

type Notification struct {
	ID           string    `json:"id"`
	UserID       int64     `json:"user_id"`
	Category     Category  `json:"category"`
	Plan         []Step    `json:"plan"`
	StepIdx      int       `json:"step_idx"`
	State        State     `json:"state"`
	DeliveredVia Channel   `json:"delivered_via,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	// InputHash связывает ключ идемпотентности с исходным запросом.
	InputHash string `json:"-"`
}

// SendCommand — команда сервису-отправителю канала (топик cmd.<channel>).
type SendCommand struct {
	NotificationID string    `json:"notification_id"`
	UserID         int64     `json:"user_id"`
	StepIdx        int       `json:"step_idx"`
	Channel        Channel   `json:"channel"`
	ExpiresAt      time.Time `json:"expires_at"`
	// AttemptID детерминирован: повторная публикация той же команды
	// даёт тот же ключ идемпотентности у отправителя и провайдера.
	AttemptID string `json:"attempt_id"`
}

func AttemptID(notificationID string, step int) string {
	return fmt.Sprintf("%s:%d", notificationID, step)
}

type ReceiptStatus string

const (
	ReceiptAccepted  ReceiptStatus = "ACCEPTED"  // провайдер принял, доставка не подтверждена
	ReceiptDelivered ReceiptStatus = "DELIVERED" // ACK приложения / DLR DELIVRD
	ReceiptFailed    ReceiptStatus = "FAILED"    // постоянная ошибка, UNDELIV, displayed=false
)

// Receipt — результат попытки или подтверждение доставки (топик delivery.receipts).
type Receipt struct {
	NotificationID string        `json:"notification_id"`
	StepIdx        int           `json:"step_idx"`
	Channel        Channel       `json:"channel"`
	Status         ReceiptStatus `json:"status"`
	Reason         string        `json:"reason,omitempty"`
}

type TimerFired struct {
	NotificationID string
	StepIdx        int
}

type Event struct {
	NotificationID string    `json:"notification_id"`
	Type           string    `json:"type"`
	Channel        Channel   `json:"channel,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	At             time.Time `json:"at"`
}

var ErrNotFound = errors.New("notification not found")
var ErrInvalid = errors.New("invalid input")
var ErrConflict = errors.New("idempotency key reused with different input")

// Tx — операции внутри одной транзакции PostgreSQL. Команды и события пишутся
// в таблицу outbox той же транзакцией и публикуются в Kafka после COMMIT.
type Tx interface {
	Insert(ctx context.Context, n *Notification) (inserted bool, err error) // ON CONFLICT (id) DO NOTHING
	GetForUpdate(ctx context.Context, id string) (*Notification, error)     // SELECT ... FOR UPDATE
	Update(ctx context.Context, n *Notification) error
	EnqueueCommand(ctx context.Context, c SendCommand) error
	AppendEvent(ctx context.Context, e Event) error
	ScheduleTimer(ctx context.Context, id string, step int, at time.Time) error
	CancelTimers(ctx context.Context, id string) error
	// RememberReceipt дедуплицирует (notification_id, step_idx, channel, status).
	RememberReceipt(ctx context.Context, r Receipt) (inserted bool, err error)
	// ClaimDueTimers возвращает и удаляет таймеры атомарно внутри транзакции.
	// PostgreSQL-адаптер должен блокировать notification, затем timer во ВСЕХ
	// обработчиках. Захват timer перед notification создаёт инверсию блокировок.
	ClaimDueTimers(ctx context.Context, now time.Time, limit int) ([]TimerFired, error)
}

type Store interface {
	InTx(ctx context.Context, fn func(Tx) error) error
}

type Orchestrator struct {
	Store Store
	Now   func() time.Time
	// SendTimeout — страховочный таймер шага без подтверждения (email):
	// если отправитель «завис» и не сообщил результат, шаг считается неудачным.
	SendTimeout time.Duration
}

// Accept регистрирует уведомление (дедупликация по детерминированному ID)
// и запускает первый шаг плана. Повторный вызов с тем же ID ничего не делает.
func (o *Orchestrator) Accept(ctx context.Context, n *Notification) error {
	if n == nil || n.ID == "" || n.UserID <= 0 || n.ExpiresAt.IsZero() ||
		(n.Category != Transactional && n.Category != Security) || o.SendTimeout <= 0 {
		return fmt.Errorf("%w: notification, category, expiry or send timeout", ErrInvalid)
	}
	seen := map[Channel]bool{}
	for _, step := range n.Plan {
		if seen[step.Channel] || step.AckTimeout < 0 ||
			(step.Channel != Push && step.Channel != SMS && step.Channel != Email) ||
			(step.Channel == Email && step.AckTimeout != 0) ||
			(step.Channel != Email && step.AckTimeout <= 0) {
			return fmt.Errorf("%w: invalid channel plan", ErrInvalid)
		}
		seen[step.Channel] = true
	}
	copyN := *n
	copyN.Plan = append([]Step(nil), n.Plan...)
	n = &copyN // ROLLBACK и повтор Accept не меняют объект вызывающей стороны.
	return o.Store.InTx(ctx, func(tx Tx) error {
		n.State, n.StepIdx = StateSending, -1
		n.CreatedAt = o.Now()
		inserted, err := tx.Insert(ctx, n)
		if err != nil {
			return err
		}
		if !inserted {
			existing, err := tx.GetForUpdate(ctx, n.ID)
			if err != nil {
				return err
			}
			if existing.InputHash != n.InputHash {
				return ErrConflict
			}
			return nil
		}
		if !o.Now().Before(n.ExpiresAt) {
			return o.expire(ctx, tx, n, "expired_at_accept")
		}
		if len(n.Plan) == 0 {
			return o.fail(ctx, tx, n, "no_available_channels")
		}
		return o.startStep(ctx, tx, n, 0, "accepted")
	})
}

// OnReceipt обрабатывает результат попытки или подтверждение доставки.
func (o *Orchestrator) OnReceipt(ctx context.Context, r Receipt) error {
	if r.Status != ReceiptAccepted && r.Status != ReceiptDelivered && r.Status != ReceiptFailed {
		return fmt.Errorf("%w: unknown receipt status %q", ErrInvalid, r.Status)
	}
	return o.Store.InTx(ctx, func(tx Tx) error {
		n, err := tx.GetForUpdate(ctx, r.NotificationID)
		if err != nil {
			return err
		}
		if r.StepIdx < 0 || r.StepIdx > n.StepIdx || r.StepIdx >= len(n.Plan) ||
			n.Plan[r.StepIdx].Channel != r.Channel || (r.Status == ReceiptDelivered && r.Channel == Email) {
			return fmt.Errorf("%w: receipt does not match a started attempt", ErrInvalid)
		}
		fresh, err := tx.RememberReceipt(ctx, r)
		if err != nil || !fresh {
			return err
		}
		if n.State.Terminal() {
			kind := "late_receipt"
			if r.Status == ReceiptDelivered {
				kind = "late_delivery"
				if n.State == StateDelivered && n.DeliveredVia != r.Channel {
					kind = "cross_channel_duplicate"
				}
			}
			return o.event(ctx, tx, n.ID, kind, r.Channel, string(r.Status))
		}
		switch r.Status {
		case ReceiptDelivered: // подтверждение по любому шагу завершает доставку
			n.State, n.DeliveredVia = StateDelivered, r.Channel
			if err := tx.CancelTimers(ctx, n.ID); err != nil {
				return err
			}
			if err := tx.Update(ctx, n); err != nil {
				return err
			}
			return o.event(ctx, tx, n.ID, "delivered", r.Channel, "")
		case ReceiptAccepted:
			if r.StepIdx == n.StepIdx && n.Plan[n.StepIdx].AckTimeout == 0 {
				n.State, n.DeliveredVia = StateSent, r.Channel
				if err := tx.CancelTimers(ctx, n.ID); err != nil {
					return err
				}
				if err := tx.Update(ctx, n); err != nil {
					return err
				}
			}
			return o.event(ctx, tx, n.ID, "provider_accepted", r.Channel, "")
		case ReceiptFailed:
			if r.StepIdx != n.StepIdx {
				return nil // ошибка уже пройденного шага — план ушёл дальше
			}
			return o.advance(ctx, tx, n, "channel_failed:"+r.Reason)
		}
		return nil
	})
}

// FireDueTimers вызывается планировщиком каждые ~100 мс на всех экземплярах;
// SKIP LOCKED распределяет таймеры между экземплярами без двойной обработки.
func (o *Orchestrator) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("%w: timer limit must be positive", ErrInvalid)
	}
	fired := 0
	err := o.Store.InTx(ctx, func(tx Tx) error {
		due, err := tx.ClaimDueTimers(ctx, o.Now(), limit)
		if err != nil {
			return err
		}
		for _, t := range due {
			n, err := tx.GetForUpdate(ctx, t.NotificationID)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if n.State.Terminal() || n.StepIdx != t.StepIdx {
				continue // устаревший таймер: уже доставлено или шаг сменился
			}
			if err := o.advance(ctx, tx, n, "ack_timeout"); err != nil {
				return err
			}
			fired++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return fired, nil
}

func (o *Orchestrator) advance(ctx context.Context, tx Tx, n *Notification, reason string) error {
	if err := tx.CancelTimers(ctx, n.ID); err != nil {
		return err
	}
	next := n.StepIdx + 1
	if !o.Now().Before(n.ExpiresAt) {
		return o.expire(ctx, tx, n, reason)
	}
	if next >= len(n.Plan) {
		return o.fail(ctx, tx, n, reason)
	}
	if err := o.event(ctx, tx, n.ID, "failover", n.Plan[next].Channel, reason); err != nil {
		return err
	}
	return o.startStep(ctx, tx, n, next, reason)
}

func (o *Orchestrator) fail(ctx context.Context, tx Tx, n *Notification, reason string) error {
	n.State = StateFailed
	if err := tx.Update(ctx, n); err != nil {
		return err
	}
	return o.event(ctx, tx, n.ID, "all_channels_failed", "", reason)
}

func (o *Orchestrator) expire(ctx context.Context, tx Tx, n *Notification, reason string) error {
	n.State = StateExpired
	if err := tx.CancelTimers(ctx, n.ID); err != nil {
		return err
	}
	if err := tx.Update(ctx, n); err != nil {
		return err
	}
	return o.event(ctx, tx, n.ID, "expired", "", reason)
}

func (o *Orchestrator) event(ctx context.Context, tx Tx, id, kind string, ch Channel, reason string) error {
	return tx.AppendEvent(ctx, Event{NotificationID: id, Type: kind, Channel: ch, Reason: reason, At: o.Now()})
}

func (o *Orchestrator) startStep(ctx context.Context, tx Tx, n *Notification, idx int, reason string) error {
	n.StepIdx, n.State = idx, StateSending
	if err := tx.Update(ctx, n); err != nil {
		return err
	}
	step := n.Plan[idx]
	cmd := SendCommand{
		NotificationID: n.ID,
		UserID:         n.UserID,
		StepIdx:        idx,
		Channel:        step.Channel,
		AttemptID:      AttemptID(n.ID, idx),
		ExpiresAt:      n.ExpiresAt,
	}
	if err := tx.EnqueueCommand(ctx, cmd); err != nil {
		return err
	}
	if err := o.event(ctx, tx, n.ID, "step_started", step.Channel, reason); err != nil {
		return err
	}
	// Таймер покрывает весь шаг: очередь, вызов провайдера и ожидание подтверждения,
	// поэтому сработает даже если отправитель упал и не прислал результат.
	timeout := step.AckTimeout
	if timeout == 0 {
		timeout = o.SendTimeout
	}
	deadline := o.Now().Add(timeout)
	if n.ExpiresAt.Before(deadline) {
		deadline = n.ExpiresAt
	}
	return tx.ScheduleTimer(ctx, n.ID, idx, deadline)
}
