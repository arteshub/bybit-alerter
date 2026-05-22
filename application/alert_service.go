package application

import (
	"context"
	"log/slog"
	"sync"

	"volume_pump_checker/domain/market"
	"volume_pump_checker/domain/user"
)

// Sender delivers an alert to a specific chat.
type Sender interface {
	SendToUser(ctx context.Context, chatID int64, candle market.Candle, avg float64, lookbackDays int) error
}

// VolumeCache is a read/write cache of per-symbol turnover data.
type VolumeCache interface {
	GetAvg(sym market.Symbol, days int) (float64, bool)
	SwapTurnover(sym market.Symbol, current float64) (prev float64, ok bool)
}

type firedKey struct {
	chatID int64
	sym    market.Symbol
	date   string
}

// AlertService orchestrates per-user spike detection.
type AlertService struct {
	users  user.Repository
	cache  VolumeCache
	sender Sender

	mu    sync.Mutex
	fired map[firedKey]struct{}
}

func NewAlertService(users user.Repository, cache VolumeCache, sender Sender) *AlertService {
	return &AlertService{
		users:  users,
		cache:  cache,
		sender: sender,
		fired:  make(map[firedKey]struct{}),
	}
}

// Handle is called on every WebSocket kline update.
func (s *AlertService) Handle(ctx context.Context, candle market.Candle) {
	prev, hadPrev := s.cache.SwapTurnover(candle.Symbol, candle.Turnover)
	if !hadPrev {
		return
	}

	all, err := s.users.All(ctx)
	if err != nil {
		slog.Error("alert: fetch users", "error", err)
		return
	}

	for _, u := range all {
		avg, ok := s.cache.GetAvg(candle.Symbol, u.LookbackDays)
		if !ok {
			continue
		}

		threshold := avg * u.VolumeMultiplier
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
			"lookbackDays", u.LookbackDays,
		)

		if err := s.sender.SendToUser(ctx, u.ChatID, candle, avg, u.LookbackDays); err != nil {
			slog.Error("alert: send failed", "chatID", u.ChatID, "error", err)
		}
	}
}
