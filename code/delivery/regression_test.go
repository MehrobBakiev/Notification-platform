package delivery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestExpiredAcceptDoesNotSend(t *testing.T) {
	o, s, c := setup(t)
	n := newNotification(c, fullPlan())
	n.ExpiresAt = c.now()
	if err := o.Accept(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if len(s.commands) != 0 || s.state(n.ID) != StateExpired {
		t.Fatal("expired notification started delivery")
	}
}
func TestTimerIsCappedAtTTL(t *testing.T) {
	o, s, c := setup(t)
	n := newNotification(c, fullPlan())
	n.ExpiresAt = c.now().Add(3 * time.Second)
	if err := o.Accept(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if got := s.Timers()[0].FireAt; !got.Equal(n.ExpiresAt) {
		t.Fatalf("timer=%v expiry=%v", got, n.ExpiresAt)
	}
	c.add(3 * time.Second)
	if _, err := o.FireDueTimers(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	assertChannels(t, s.commands, Push)
	if s.state(n.ID) != StateExpired {
		t.Fatal("expected EXPIRED at exact deadline")
	}
}
func TestRejectUnstartedOrMismatchedReceipt(t *testing.T) {
	for _, r := range []Receipt{
		{NotificationID: "n1", StepIdx: 99, Channel: SMS, Status: ReceiptDelivered},
		{NotificationID: "n1", StepIdx: 1, Channel: SMS, Status: ReceiptDelivered},
		{NotificationID: "n1", StepIdx: 0, Channel: SMS, Status: ReceiptDelivered},
		{NotificationID: "n1", StepIdx: -1, Channel: Push, Status: ReceiptDelivered},
	} {
		o, s, c := setup(t)
		if err := o.Accept(context.Background(), newNotification(c, fullPlan())); err != nil {
			t.Fatal(err)
		}
		if err := o.OnReceipt(context.Background(), r); !errors.Is(err, ErrInvalid) {
			t.Fatalf("receipt=%+v err=%v", r, err)
		}
		if s.state("n1") != StateSending {
			t.Fatal("invalid receipt changed state")
		}
	}
}
func TestReceiptReplayDoesNotCreateDuplicateEvents(t *testing.T) {
	o, s, c := setup(t)
	ctx := context.Background()
	if err := o.Accept(ctx, newNotification(c, fullPlan())); err != nil {
		t.Fatal(err)
	}
	r := Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptDelivered}
	if err := o.OnReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	count := len(s.events)
	if err := o.OnReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	if len(s.events) != count {
		t.Fatal("duplicate receipt inflated audit/duplicate metrics")
	}
}
func TestConflictingIdempotencyKey(t *testing.T) {
	o, _, c := setup(t)
	ctx := context.Background()
	n := newNotification(c, fullPlan())
	n.InputHash = "original"
	if err := o.Accept(ctx, n); err != nil {
		t.Fatal(err)
	}
	n.InputHash = "different"
	if err := o.Accept(ctx, n); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v", err)
	}
}
func TestConcurrentAcceptAndReceiptTimerRace(t *testing.T) {
	o, s, c := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- o.Accept(ctx, newNotification(c, fullPlan())) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertChannels(t, s.commands, Push)
	c.add(10 * time.Second)
	errCh := make(chan error, 2)
	go func() { _, err := o.FireDueTimers(ctx, 10); errCh <- err }()
	go func() {
		errCh <- o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptDelivered})
	}()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if s.state("n1") != StateDelivered || len(s.commands) > 2 || len(s.Timers()) != 0 {
		t.Fatal("race corrupted state")
	}
}
func TestTransactionRollback(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	err := s.InTx(ctx, func(tx Tx) error {
		_, err := tx.Insert(ctx, &Notification{ID: "n1"})
		if err != nil {
			return err
		}
		if err := tx.EnqueueCommand(ctx, SendCommand{NotificationID: "n1"}); err != nil {
			return err
		}
		return errors.New("rollback")
	})
	if err == nil || len(s.Notifications()) != 0 || len(s.Commands()) != 0 {
		t.Fatal("partial transaction committed")
	}
}
