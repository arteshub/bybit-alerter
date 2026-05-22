package application

import (
	"context"
	"log/slog"
	"sync"

	"volume_pump_checker/domain/user"
	"volume_pump_checker/internal/exchange"
	"volume_pump_checker/internal/store"
)

// Sender delivers an alert to a specific Telegram chat.
type Sender interface {
	SendToUser(ctx context.Context, chatID int64, candle exchange.Candle, avg float64) error
}

type firedKey struct {
	chatID int64
	sym    exchange.Symbol
	date   string
}

// AlertService orchestrates per-user spike detection.
// Each user has their own VolumeMultiplier; alerts fire independently.
type AlertService struct {
	users  user.Repository
	store  *store.VolumeStore
	sender Sender

	mu    sync.Mutex
	fired map[firedKey]struct{}
}

func NewAlertService(users user.Repository, store *store.VolumeStore, sender Sender) *AlertService {
	return &AlertService{
		users:  users,
		store:  store,
		sender: sender,
		fired:  make(map[firedKey]struct{}),
	}
}

// Handle is called on every WebSocket kline update.
func (s *AlertService) Handle(ctx context.Context, candle exchange.Candle) {
	avg, ok := s.store.Get(candle.Symbol)
	if !ok {
		return
	}

	prev, hadPrev := s.store.SwapTurnover(candle.Symbol, candle.Turnover)
	if !hadPrev {
		// First update — record baseline only, no alerts.
		return
	}

	all, err := s.users.All(ctx)
	if err != nil {
		slog.Error("alert: fetch users", "error", err)
		return
	}

	for _, u := range all {
		threshold := avg * u.VolumeMultiplier

		// Only fire on the exact crossing moment: was below, now above.
		if prev >= threshold || candle.Turnover < threshold {
			continue
		}

		key := firedKey{chatID: u.ChatID, sym: candle.Symbol, date: candle.Date}
		s.mu.Lock()
		_, already := s.fired[key]
		if !already {
			s.fired[key] = struct{}{}
		}
		s.mu.Unlock()

		if already {
			continue
		}

		slog.Info("alert fired",
			"symbol", candle.Symbol,
			"turnover", candle.Turnover,
			"avg", avg,
			"ratio", candle.Turnover/avg,
			"chatID", u.ChatID,
			"multiplier", u.VolumeMultiplier,
		)

		if err := s.sender.SendToUser(ctx, u.ChatID, candle, avg); err != nil {
			slog.Error("alert: send failed", "chatID", u.ChatID, "error", err)
		}
	}
}
