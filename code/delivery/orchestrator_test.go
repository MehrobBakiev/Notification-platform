package delivery

import (
	"context"
	"testing"
	"time"
)

type memStore = MemoryStore

func newMemStore() *MemoryStore { return NewMemoryStore() }

// --- тестовое окружение ---

type clock struct{ t time.Time }

func (c *clock) now() time.Time           { return c.t }
func (c *clock) add(d time.Duration)      { c.t = c.t.Add(d) }
func newClock() *clock                    { return &clock{t: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)} }
func (s *memStore) state(id string) State { return s.notifs[id].State }

var testPolicy = Policy{PushAckTimeout: 10 * time.Second, SecurityPushAckTimeout: 5 * time.Second, SMSDeliveryTimeout: 60 * time.Second}

func setup(t *testing.T) (*Orchestrator, *memStore, *clock) {
	t.Helper()
	s, c := newMemStore(), newClock()
	return &Orchestrator{Store: s, Now: c.now, SendTimeout: 2 * time.Minute}, s, c
}

func newNotification(c *clock, plan []Step) *Notification {
	return &Notification{ID: "n1", UserID: 42, Category: Transactional, Plan: plan, ExpiresAt: c.now().Add(24 * time.Hour)}
}

func fullPlan() []Step {
	return BuildPlan(Transactional, Profile{PushReady: true, HasPhone: true, HasEmail: true}, testPolicy)
}

func channels(cmds []SendCommand) []Channel {
	var out []Channel
	for _, c := range cmds {
		out = append(out, c.Channel)
	}
	return out
}

func assertChannels(t *testing.T, got []SendCommand, want ...Channel) {
	t.Helper()
	g := channels(got)
	if len(g) != len(want) {
		t.Fatalf("commands = %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("commands = %v, want %v", g, want)
		}
	}
}

// --- тесты ---

func TestPushDeliveredNoFailover(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	if err := o.Accept(ctx, newNotification(c, fullPlan())); err != nil {
		t.Fatal(err)
	}
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptAccepted})
	c.add(2 * time.Second)
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptDelivered})
	c.add(time.Minute)
	if n, _ := o.FireDueTimers(ctx, 100); n != 0 {
		t.Fatalf("timers fired after delivery: %d", n)
	}
	assertChannels(t, s.commands, Push)
	if s.state("n1") != StateDelivered || s.notifs["n1"].DeliveredVia != Push {
		t.Fatalf("state = %s via %s", s.state("n1"), s.notifs["n1"].DeliveredVia)
	}
}

func TestDuplicateAcceptIsIgnored(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	for i := 0; i < 3; i++ { // отправитель повторил запрос с тем же ключом
		if err := o.Accept(ctx, newNotification(c, fullPlan())); err != nil {
			t.Fatal(err)
		}
	}
	assertChannels(t, s.commands, Push)
}

func TestFailoverToSMSOnAckTimeoutAndLateAck(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	_ = o.Accept(ctx, newNotification(c, fullPlan()))
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptAccepted})

	c.add(9 * time.Second)
	if n, _ := o.FireDueTimers(ctx, 100); n != 0 {
		t.Fatal("failover before push timeout")
	}
	c.add(1 * time.Second) // 10 с без ACK
	if n, _ := o.FireDueTimers(ctx, 100); n != 1 {
		t.Fatal("expected failover to SMS")
	}
	assertChannels(t, s.commands, Push, SMS)
	if s.commands[1].AttemptID != "n1:1" {
		t.Fatalf("attempt id = %s", s.commands[1].AttemptID)
	}

	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 1, Channel: SMS, Status: ReceiptDelivered})
	// Поздний ACK push: фиксируется событием, новых команд нет.
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptDelivered})
	c.add(5 * time.Minute)
	_, _ = o.FireDueTimers(ctx, 100)

	assertChannels(t, s.commands, Push, SMS)
	if s.state("n1") != StateDelivered || s.notifs["n1"].DeliveredVia != SMS {
		t.Fatalf("state = %s via %s", s.state("n1"), s.notifs["n1"].DeliveredVia)
	}
	if last := s.events[len(s.events)-1]; last.Type != "cross_channel_duplicate" {
		t.Fatalf("last event = %s, want cross_channel_duplicate", last.Type)
	}
}

func TestPushAckBeforeTimerCancelsSMS(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	_ = o.Accept(ctx, newNotification(c, fullPlan()))
	c.add(9900 * time.Millisecond)
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptDelivered})
	c.add(time.Second)
	_, _ = o.FireDueTimers(ctx, 100)
	assertChannels(t, s.commands, Push)
}

func TestPermanentPushErrorFailsOverImmediately(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	_ = o.Accept(ctx, newNotification(c, fullPlan()))
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Push, Status: ReceiptFailed, Reason: "UNREGISTERED"})
	assertChannels(t, s.commands, Push, SMS) // без ожидания 10 с
	// Устаревший таймер push не должен сдвинуть план ещё раз.
	c.add(10 * time.Second)
	if n, _ := o.FireDueTimers(ctx, 100); n != 0 {
		t.Fatalf("stale timer fired: %d", n)
	}
}

func TestAllChannelsExhausted(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	_ = o.Accept(ctx, newNotification(c, fullPlan()))
	c.add(10 * time.Second)
	_, _ = o.FireDueTimers(ctx, 100) // push → SMS
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 1, Channel: SMS, Status: ReceiptFailed, Reason: "UNDELIV"})
	assertChannels(t, s.commands, Push, SMS, Email)
	c.add(3 * time.Minute) // email-отправитель не ответил — страховочный таймер
	_, _ = o.FireDueTimers(ctx, 100)
	if s.state("n1") != StateFailed {
		t.Fatalf("state = %s, want FAILED", s.state("n1"))
	}
}

func TestEmailAcceptedIsSent(t *testing.T) {
	ctx := context.Background()
	o, s, c := setup(t)
	plan := BuildPlan(Transactional, Profile{HasEmail: true}, testPolicy)
	_ = o.Accept(ctx, newNotification(c, plan))
	_ = o.OnReceipt(ctx, Receipt{NotificationID: "n1", StepIdx: 0, Channel: Email, Status: ReceiptAccepted})
	if s.state("n1") != StateSent {
		t.Fatalf("state = %s, want SENT", s.state("n1"))
	}
}

func TestBuildPlan(t *testing.T) {
	cases := []struct {
		name string
		cat  Category
		p    Profile
		want []Channel
	}{
		{"push-first", Transactional, Profile{PushReady: true, HasPhone: true, HasEmail: true}, []Channel{Push, SMS, Email}},
		{"prefers SMS", Transactional, Profile{PreferredChannel: SMS, PushReady: true, HasPhone: true}, []Channel{SMS, Push}},
		{"no app", Transactional, Profile{HasPhone: true, HasEmail: true}, []Channel{SMS, Email}},
		{"push unreliable", Transactional, Profile{PushReady: true, PushUnreliable: true, HasPhone: true}, []Channel{SMS}},
	}
	for _, tc := range cases {
		var got []Channel
		for _, st := range BuildPlan(tc.cat, tc.p, testPolicy) {
			got = append(got, st.Channel)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s: plan = %v, want %v", tc.name, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: plan = %v, want %v", tc.name, got, tc.want)
			}
		}
	}
	sec := BuildPlan(Security, Profile{PushReady: true, HasPhone: true}, testPolicy)
	if sec[0].AckTimeout != 5*time.Second {
		t.Fatalf("security push timeout = %v", sec[0].AckTimeout)
	}
}
