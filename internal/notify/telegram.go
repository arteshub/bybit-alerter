package notify

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"volume_pump_checker/domain/user"
	"volume_pump_checker/internal/exchange"
)

type userPayload struct {
	chatID       int64
	candle       exchange.Candle
	avg          float64
	lookbackDays int
}

type TelegramNotifier struct {
	token       string
	defaultMult float64
	userRepo    user.Repository
	queue       chan userPayload
	offset      atomic.Int64
	client      *http.Client
}

func NewTelegramNotifier(token string, defaultMult float64, userRepo user.Repository) *TelegramNotifier {
	return &TelegramNotifier{
		token:       token,
		defaultMult: defaultMult,
		userRepo:    userRepo,
		queue:       make(chan userPayload, 1024),
		client:      &http.Client{Timeout: 15 * time.Second},
	}
}

func (t *TelegramNotifier) Start(ctx context.Context) {
	go t.worker(ctx)
	go t.pollUpdates(ctx)
}

func (t *TelegramNotifier) SendToUser(_ context.Context, chatID int64, candle exchange.Candle, avg float64, lookbackDays int) error {
	select {
	case t.queue <- userPayload{chatID: chatID, candle: candle, avg: avg, lookbackDays: lookbackDays}:
	default:
		// queue full — drop silently, log handled in caller
	}
	return nil
}

func (t *TelegramNotifier) worker(ctx context.Context) {
	for {
		select {
		case p := <-t.queue:
			_ = t.sendMessage(ctx, p.chatID, formatAlert(p), nil)
			time.Sleep(50 * time.Millisecond)
		case <-ctx.Done():
			for {
				select {
				case p := <-t.queue:
					_ = t.sendMessage(context.Background(), p.chatID, formatAlert(p), nil)
					time.Sleep(50 * time.Millisecond)
				default:
					return
				}
			}
		}
	}
}
