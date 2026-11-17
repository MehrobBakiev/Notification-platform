package delivery

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// StateReader возвращает свежий снимок из источника состояния, не read replica.
type StateReader interface {
	Current(ctx context.Context, notificationID string) (*Notification, error)
}

var (
	ErrAttemptBusy = errors.New("attempt is leased; retry later")
	ErrLeaseLost   = errors.New("attempt lease lost")
	ErrPermanent   = errors.New("permanent channel error")
	// ErrUnavailable означает подтверждённый отказ ДО принятия сообщения.
	ErrUnavailable = errors.New("all providers rejected submission")
)

// AttemptStore — журнал попыток. В production это PostgreSQL, а не Redis-кэш.
// Claim возвращает сохранённый результат, новую аренду (nil, nil) либо Busy.
// Complete и Release проверяют owner; устаревший worker не изменит чужую аренду.
// Результат хранится как минимум весь период replay входных команд.
type AttemptStore interface {
	Claim(ctx context.Context, id, owner string, now time.Time, lease time.Duration) (*Receipt, error)
	Complete(ctx context.Context, id, owner string, r Receipt, now time.Time) error
	Release(ctx context.Context, id, owner string) error
}

type SMSMessage struct {
	To          string
	Text        string
	ClientMsgID string
}

// SMSRouter закрепляет провайдера за попыткой ДО вызова. При неоднозначном
// таймауте сверяет статус/повторяет у того же провайдера с тем же ClientMsgID.
// Переход A -> B допустим только после подтверждённого непринятия A.
// Любая другая ошибка означает неизвестный исход, её нельзя превращать в FAILED.
type SMSRouter interface {
	Send(ctx context.Context, m SMSMessage) (provider string, err error)
}

type Contacts interface {
	Phone(ctx context.Context, userID int64) (string, error)
}
type Renderer interface {
	Render(ctx context.Context, notificationID string, ch Channel) (string, error)
}
type ReceiptPublisher interface {
	Publish(ctx context.Context, r Receipt) error
}

type SMSSender struct {
	State    StateReader
	Attempts AttemptStore
	Contacts Contacts
	Render   Renderer
	Router   SMSRouter
	Results  ReceiptPublisher
	Now      func() time.Time
	Lease    time.Duration
}

// Handle возвращает nil только после публикации сохранённого результата либо
// для уже неактуальной команды. Busy и неизвестный исход требуют retry, не ACK.
func (s *SMSSender) Handle(ctx context.Context, cmd SendCommand) error {
	if cmd.Channel != SMS || cmd.NotificationID == "" || cmd.StepIdx < 0 ||
		cmd.AttemptID != AttemptID(cmd.NotificationID, cmd.StepIdx) {
		return fmt.Errorf("%w: invalid SMS command", ErrInvalid)
	}
	now := s.Now
	if now == nil {
		now = time.Now
	}
	lease := s.Lease
	if lease <= 0 {
		lease = 30 * time.Second
	}
	owner := rand.Text()
	claimedAt := now()
	r, err := s.Attempts.Claim(ctx, cmd.AttemptID, owner, claimedAt, lease)
	if err != nil {
		return err
	}
	if r != nil {
		// Receipt можно переиздать даже после смены шага: внешний send не повторяется.
		return s.Results.Publish(ctx, *r)
	}
	release := func() { _ = s.Attempts.Release(context.WithoutCancel(ctx), cmd.AttemptID, owner) }
	workCtx, cancelWork := context.WithTimeout(ctx, lease/2)
	defer cancelWork()
	active := func() (bool, error) {
		n, err := s.State.Current(workCtx, cmd.NotificationID)
		if err != nil {
			return false, err
		}
		return !n.State.Terminal() && n.StepIdx == cmd.StepIdx &&
			cmd.UserID == n.UserID && n.StepIdx >= 0 && n.StepIdx < len(n.Plan) &&
			n.Plan[n.StepIdx].Channel == SMS && now().Before(n.ExpiresAt) &&
			!cmd.ExpiresAt.IsZero() && now().Before(cmd.ExpiresAt), nil
	}
	ok, err := active()
	if err != nil || !ok {
		release()
		return err
	}
	phone, err := s.Contacts.Phone(workCtx, cmd.UserID)
	if err != nil {
		release()
		return err
	}
	text, err := s.Render.Render(workCtx, cmd.NotificationID, SMS)
	if err != nil {
		release()
		return err
	}
	// Повторная проверка сокращает окно гонки с ACK, но не делает HTTP-вызов
	// атомарным с PostgreSQL. Межканальные дубли остаются возможными.
	ok, err = active()
	if err != nil || !ok {
		release()
		return err
	}
	remaining := min(claimedAt.Add(lease).Sub(now()), cmd.ExpiresAt.Sub(now()))
	if remaining <= 0 || workCtx.Err() != nil {
		release()
		if workCtx.Err() != nil {
			return workCtx.Err()
		}
		return ErrLeaseLost
	}
	sendCtx, cancel := context.WithTimeout(workCtx, remaining/2)
	defer cancel()
	provider, err := s.Router.Send(sendCtx, SMSMessage{To: phone, Text: text, ClientMsgID: cmd.AttemptID})
	result := Receipt{NotificationID: cmd.NotificationID, StepIdx: cmd.StepIdx, Channel: SMS}
	switch {
	case errors.Is(err, ErrPermanent), errors.Is(err, ErrUnavailable):
		result.Status, result.Reason = ReceiptFailed, err.Error()
	case err != nil:
		// Аренду оставляем до истечения; следующая попытка сверит исход у провайдера.
		return err
	default:
		result.Status, result.Reason = ReceiptAccepted, provider
	}
	if err := s.Attempts.Complete(context.WithoutCancel(ctx), cmd.AttemptID, owner, result, now()); err != nil {
		return err
	}
	// Сбой Publish не теряет результат: повтор Claim вернёт его без новой SMS.
	return s.Results.Publish(ctx, result)
}
