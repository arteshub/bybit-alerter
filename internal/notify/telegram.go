package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"volume_pump_checker/internal/exchange"
)

type payload struct {
	candle exchange.Candle
	avg    float64
}

// TelegramNotifier sends alerts to every subscriber who started the bot.
// Subscribers are added automatically when they send /start to the bot.
// An optional seed chat ID can be pre-registered at construction time.
type TelegramNotifier struct {
	token        string
	lookbackDays int
	subs         sync.Map    // map[int64]struct{}
	queue        chan payload // buffered, non-blocking writes
	offset       atomic.Int64
	client       *http.Client
}

func NewTelegramNotifier(token string, lookbackDays int) *TelegramNotifier {
	return &TelegramNotifier{
		token:        token,
		lookbackDays: lookbackDays,
		queue:        make(chan payload, 1024),
		client:       &http.Client{Timeout: 15 * time.Second},
	}
}

// Start launches the queue worker and the Telegram update poller.
func (t *TelegramNotifier) Start(ctx context.Context) {
	go t.worker(ctx)
	go t.pollUpdates(ctx)
}

// Send enqueues an alert non-blocking; drops the message if the queue is full.
func (t *TelegramNotifier) Send(_ context.Context, candle exchange.Candle, avg float64) error {
	select {
	case t.queue <- payload{candle: candle, avg: avg}:
	default:
		slog.Warn("telegram queue full, dropping alert", "symbol", candle.Symbol)
	}
	return nil
}

func (t *TelegramNotifier) worker(ctx context.Context) {
	for {
		select {
		case p := <-t.queue:
			t.broadcast(ctx, p)
			time.Sleep(1100 * time.Millisecond)
		case <-ctx.Done():
			// Drain remaining messages before exit.
			for {
				select {
				case p := <-t.queue:
					t.broadcast(context.Background(), p)
					time.Sleep(1100 * time.Millisecond)
				default:
					return
				}
			}
		}
	}
}

func (t *TelegramNotifier) broadcast(ctx context.Context, p payload) {
	text := t.formatMessage(p.candle, p.avg)
	t.subs.Range(func(key, _ any) bool {
		chatID := key.(int64)
		if err := t.sendMessage(ctx, chatID, text); err != nil {
			slog.Error("telegram send error", "error", err, "chatID", chatID)
		}
		time.Sleep(50 * time.Millisecond) // ~20 msgs/sec, well within Telegram limits
		return true
	})
}

// pollUpdates runs a long-poll loop and registers new subscribers on /start.
func (t *TelegramNotifier) pollUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.fetchUpdates(ctx)
	}
}

type tgUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  struct {
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		Text string `json:"text"`
	} `json:"message"`
}

func (t *TelegramNotifier) fetchUpdates(ctx context.Context) {
	offset := t.offset.Load()
	url := fmt.Sprintf(
		"https://api.telegram.org/bot%s/getUpdates?timeout=10&offset=%d&allowed_updates=%%5B%%22message%%22%%5D",
		t.token, offset,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("getUpdates error", "error", err)
			time.Sleep(5 * time.Second)
		}
		return
	}
	defer resp.Body.Close()

	var result struct {
		OK     bool        `json:"ok"`
		Result []tgUpdate  `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("getUpdates decode error", "error", err)
		return
	}

	for _, upd := range result.Result {
		if upd.Message.Text == "/start" {
			chatID := upd.Message.Chat.ID
			if _, loaded := t.subs.LoadOrStore(chatID, struct{}{}); !loaded {
				slog.Info("new subscriber", "chatID", chatID)
				welcome := "✅ <b>Bybit Volume Alerter</b>\n\nYou'll receive alerts when daily turnover spikes!"
				_ = t.sendMessage(ctx, chatID, welcome)
			}
		}
		t.offset.Store(upd.UpdateID + 1)
	}
}

func (t *TelegramNotifier) sendMessage(ctx context.Context, chatID int64, text string) error {
	body := map[string]any{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "HTML",
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token),
		bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram API %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (t *TelegramNotifier) formatMessage(c exchange.Candle, avg float64) string {
	tag := "intraday"
	if c.IsClosed {
		tag = "closed"
	}
	return fmt.Sprintf(
		"🚨 <b>%s</b>\nTurnover: %s USDT\n%dd avg: %s USDT\nRatio: x%.2f\n<i>%s</i>",
		c.Symbol,
		fmtVol(c.Turnover),
		t.lookbackDays,
		fmtVol(avg),
		c.Turnover/avg,
		tag,
	)
}

func fmtVol(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.2fK", v/1e3)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}
