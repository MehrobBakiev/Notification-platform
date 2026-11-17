package delivery

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeState struct{ n Notification }

func (f *fakeState) Current(context.Context, string) (*Notification, error) { n := f.n; return &n, nil }

type fakeRouter struct {
	sent []SMSMessage
	err  error
}

func (r *fakeRouter) Send(_ context.Context, m SMSMessage) (string, error) {
	r.sent = append(r.sent, m)
	return "aggregator-a", r.err
}

type fakeContacts struct{ fail bool }

func (f *fakeContacts) Phone(context.Context, int64) (string, error) {
	if f.fail {
		return "", errors.New("profile unavailable")
	}
	return "+79990000000", nil
}

type fakeRenderer struct{}

type renderFunc func(context.Context, string, Channel) (string, error)

func (f renderFunc) Render(ctx context.Context, id string, ch Channel) (string, error) {
	return f(ctx, id, ch)
}

func (fakeRenderer) Render(context.Context, string, Channel) (string, error) {
	return "Списание 1 500 ₽, карта *1234", nil
}

type fakeResults struct {
	got  []Receipt
	fail bool
}

func (f *fakeResults) Publish(_ context.Context, r Receipt) error {
	if f.fail {
		return errors.New("Kafka unavailable")
	}
	f.got = append(f.got, r)
	return nil
}
func newSender(st State, step int) (*SMSSender, *fakeRouter, *fakeResults, SendCommand, *clock) {
	c := newClock()
	r, res := &fakeRouter{}, &fakeResults{}
	plan := []Step{{Channel: Push, AckTimeout: time.Second}, {Channel: SMS, AckTimeout: time.Minute}}
	n := Notification{ID: "n1", UserID: 42, State: st, StepIdx: step, Plan: plan, ExpiresAt: c.now().Add(time.Hour)}
	s := &SMSSender{State: &fakeState{n}, Attempts: NewMemoryAttempts(), Contacts: &fakeContacts{},
		Render: fakeRenderer{}, Router: r, Results: res, Now: c.now}
	cmd := SendCommand{NotificationID: "n1", UserID: 42, StepIdx: 1, Channel: SMS, AttemptID: "n1:1", ExpiresAt: n.ExpiresAt}
	return s, r, res, cmd, c
}
func TestSMSSenderRedeliveredCommandSentOnce(t *testing.T) {
	s, router, res, cmd, _ := newSender(StateSending, 1)
	for i := 0; i < 3; i++ {
		if err := s.Handle(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}
	}
	if len(router.sent) != 1 || router.sent[0].ClientMsgID != "n1:1" {
		t.Fatalf("sent = %+v", router.sent)
	}
	// Результат публикуется at-least-once; оркестратор дедуплицирует receipts.
	if len(res.got) != 3 || res.got[0].Status != ReceiptAccepted {
		t.Fatalf("receipts = %+v", res.got)
	}
}
func TestSMSSenderSkipsStaleOrExpiredCommand(t *testing.T) {
	for _, kind := range []string{"delivered", "wrong_step", "expired"} {
		t.Run(kind, func(t *testing.T) {
			s, r, _, cmd, c := newSender(StateSending, 1)
			switch kind {
			case "delivered":
				s.State.(*fakeState).n.State = StateDelivered
			case "wrong_step":
				s.State.(*fakeState).n.StepIdx = 0
			case "expired":
				c.add(time.Hour)
			}
			if err := s.Handle(context.Background(), cmd); err != nil {
				t.Fatal(err)
			}
			if len(r.sent) != 0 {
				t.Fatal("SMS sent for stale command")
			}
		})
	}
}
func TestSMSSenderRetriesAfterContactError(t *testing.T) {
	s, r, _, cmd, _ := newSender(StateSending, 1)
	s.Contacts.(*fakeContacts).fail = true
	if err := s.Handle(context.Background(), cmd); err == nil {
		t.Fatal("expected contact error")
	}
	s.Contacts.(*fakeContacts).fail = false
	if err := s.Handle(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	if len(r.sent) != 1 {
		t.Fatal("retry was dropped")
	}
}
func TestSMSSenderRepublishesSavedResultAfterKafkaError(t *testing.T) {
	s, r, res, cmd, _ := newSender(StateSending, 1)
	res.fail = true
	if err := s.Handle(context.Background(), cmd); err == nil {
		t.Fatal("expected publish error")
	}
	res.fail = false
	if err := s.Handle(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	if len(r.sent) != 1 || len(res.got) != 1 {
		t.Fatalf("sends=%d receipts=%d", len(r.sent), len(res.got))
	}
}
func TestSMSSenderBusyRequiresRetry(t *testing.T) {
	s, r, _, cmd, c := newSender(StateSending, 1)
	_, err := s.Attempts.Claim(context.Background(), cmd.AttemptID, "other-worker", c.now(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Handle(context.Background(), cmd); !errors.Is(err, ErrAttemptBusy) {
		t.Fatalf("got %v", err)
	}
	if len(r.sent) != 0 {
		t.Fatal("sent with someone else's lease")
	}
	c.add(time.Second)
	if err = s.Handle(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
}
func TestSMSSenderUnknownOutcomeDoesNotFailChannel(t *testing.T) {
	s, router, res, cmd, _ := newSender(StateSending, 1)
	router.err = context.DeadlineExceeded
	if err := s.Handle(context.Background(), cmd); err == nil {
		t.Fatal("unknown outcome must retry")
	}
	if len(res.got) != 0 {
		t.Fatal("unknown outcome became FAILED")
	}
}
func TestAttemptLeaseOwnership(t *testing.T) {
	a, c, ctx := NewMemoryAttempts(), newClock(), context.Background()
	_, _ = a.Claim(ctx, "n:0", "old", c.now(), time.Second)
	c.add(time.Second)
	_, _ = a.Claim(ctx, "n:0", "new", c.now(), time.Minute)
	if err := a.Complete(ctx, "n:0", "old", Receipt{}, c.now()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("got %v", err)
	}
	if err := a.Release(ctx, "n:0", "old"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Claim(ctx, "n:0", "third", c.now(), time.Minute); !errors.Is(err, ErrAttemptBusy) {
		t.Fatalf("old owner removed new lease: %v", err)
	}
}

func TestSMSSenderDoesNotSubmitAfterPreparationOutlivesLeaseOrTTL(t *testing.T) {
	for _, elapsed := range []time.Duration{31 * time.Second, time.Hour} {
		t.Run(elapsed.String(), func(t *testing.T) {
			s, router, _, cmd, c := newSender(StateSending, 1)
			s.Render = renderFunc(func(context.Context, string, Channel) (string, error) { c.add(elapsed); return "demo", nil })
			err := s.Handle(context.Background(), cmd)
			if elapsed == 31*time.Second && !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("got %v", err)
			}
			if elapsed == time.Hour && err != nil {
				t.Fatal(err)
			}
			if len(router.sent) != 0 {
				t.Fatal("sent with expired lease or TTL")
			}
		})
	}
}
