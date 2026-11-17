// Package delivery — ядро подсистемы гарантированной доставки критичных
// уведомлений (вариант B RFC): план каналов и машина состояний оркестратора.
package delivery

import (
	"encoding/json"
	"time"
)

type Channel string

const (
	Push  Channel = "push"
	SMS   Channel = "sms"
	Email Channel = "email"
)

type Category string

const (
	Transactional Category = "TRANSACTIONAL"
	Security      Category = "SECURITY"
)

// Step — один шаг плана доставки.
type Step struct {
	Channel Channel `json:"channel"`
	// AckTimeout — сколько ждать подтверждения доставки (ACK приложения / DLR)
	// с момента постановки команды. 0 — подтверждения не бывает (email).
	AckTimeout time.Duration `json:"ack_timeout"`
}

// HTTP API показывает длительности в секундах, не в наносекундах time.Duration.
func (s Step) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Channel           Channel `json:"channel"`
		AckTimeoutSeconds float64 `json:"ack_timeout_seconds"`
	}{s.Channel, s.AckTimeout.Seconds()})
}

// Profile — данные клиента из кэша Preference & Device Service.
type Profile struct {
	PreferredChannel Channel `json:"preferred_channel"` // push (по умолчанию) или sms
	PushReady        bool    `json:"push_ready"`        // есть токен и поддержка ACK
	PushUnreliable   bool    `json:"push_unreliable"`
	HasPhone         bool    `json:"has_phone"`
	HasEmail         bool    `json:"has_email"`
}

// Policy — настраиваемые без релиза параметры (хранятся в конфигурации платформы).
type Policy struct {
	PushAckTimeout         time.Duration // 10 с для транзакционных
	SecurityPushAckTimeout time.Duration // 5 с для уведомлений безопасности
	SMSDeliveryTimeout     time.Duration // 60 с — ожидание DLR
}

// BuildPlan строит упорядоченный план каналов для критичного уведомления.
// Принцип: сначала бесплатный канал (push), платный SMS — только как резерв,
// если клиент сам не выбрал SMS или push у него заведомо не работает.
func BuildPlan(c Category, p Profile, pol Policy) []Step {
	pushTimeout := pol.PushAckTimeout
	if c == Security {
		pushTimeout = pol.SecurityPushAckTimeout
	}
	push := Step{Channel: Push, AckTimeout: pushTimeout}
	sms := Step{Channel: SMS, AckTimeout: pol.SMSDeliveryTimeout}

	pushOK := p.PushReady && !p.PushUnreliable
	var plan []Step
	switch {
	case p.PreferredChannel == SMS && p.HasPhone:
		plan = append(plan, sms)
		if pushOK {
			plan = append(plan, push)
		}
	default:
		if pushOK {
			plan = append(plan, push)
		}
		if p.HasPhone {
			plan = append(plan, sms)
		}
	}
	if p.HasEmail {
		plan = append(plan, Step{Channel: Email}) // последний резерв
	}
	return plan
}
