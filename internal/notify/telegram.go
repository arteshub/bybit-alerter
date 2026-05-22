package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"volume_pump_checker/domain/user"
	"volume_pump_checker/internal/exchange"
)

type userPayload struct {
	chatID int64
	candle exchange.Candle
	avg    float64
}

// TelegramNotifier handles alert delivery and bot command processing.
// Users register via /start and configure their threshold via /multiplier.
type TelegramNotifier struct {
	token        string
	lookbackDays int
	defaultMult  float64
	userRepo     user.Repository
	queue        chan userPayload
	offset       atomic.Int64
	client       *http.Client
}

func NewTelegramNotifier(token string, lookbackDays int, defaultMult float64, userRepo user.Repository) *TelegramNotifier {
	return &TelegramNotifier{
		token:        token,
		lookbackDays: lookbackDays,
		defaultMult:  defaultMult,
		userRepo:     userRepo,
		queue:        make(chan userPayload, 1024),
		client:       &http.Client{Timeout: 15 * time.Second},
	}
}

func (t *TelegramNotifier) Start(ctx context.Context) {
	go t.worker(ctx)
	go t.pollUpdates(ctx)
}

// SendToUser enqueues an alert for a specific chat ID.
func (t *TelegramNotifier) SendToUser(_ context.Context, chatID int64, candle exchange.Candle, avg float64) error {
	select {
	case t.queue <- userPayload{chatID: chatID, candle: candle, avg: avg}:
	default:
		slog.Warn("telegram queue full, dropping alert", "symbol", candle.Symbol, "chatID", chatID)
	}
	return nil
}

func (t *TelegramNotifier) worker(ctx context.Context) {
	for {
		select {
		case p := <-t.queue:
			text := t.formatMessage(p.candle, p.avg)
			if err := t.sendMessage(ctx, p.chatID, text); err != nil {
				slog.Error("telegram send error", "chatID", p.chatID, "error", err)
			}
			time.Sleep(50 * time.Millisecond)
		case <-ctx.Done():
			for {
				select {
				case p := <-t.queue:
					text := t.formatMessage(p.candle, p.avg)
					_ = t.sendMessage(context.Background(), p.chatID, text)
					time.Sleep(50 * time.Millisecond)
				default:
					return
				}
			}
		}
	}
}

// --- Bot update polling ---

type tgUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  struct {
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		Text string `json:"text"`
	} `json:"message"`
}

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

func (t *TelegramNotifier) fetchUpdates(ctx context.Context) {
	url := fmt.Sprintf(
		"https://api.telegram.org/bot%s/getUpdates?timeout=10&offset=%d&allowed_updates=%%5B%%22message%%22%%5D",
		t.token, t.offset.Load(),
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
		t.handleUpdate(ctx, upd)
		t.offset.Store(upd.UpdateID + 1)
	}
}

func (t *TelegramNotifier) handleUpdate(ctx context.Context, upd tgUpdate) {
	chatID := upd.Message.Chat.ID
	text := strings.TrimSpace(upd.Message.Text)

	switch {
	case text == "/start":
		u := user.New(chatID, t.defaultMult)
		if err := t.userRepo.Upsert(ctx, u); err != nil {
			slog.Error("bot: upsert user", "error", err)
			return
		}
		slog.Info("new subscriber", "chatID", chatID, "multiplier", t.defaultMult)
		_ = t.sendMessage(ctx, chatID, fmt.Sprintf(
			"✅ <b>Bybit Volume Alerter</b>\n\nПодписан! Текущий порог: <b>x%.1f</b>\n\nКоманды:\n/multiplier 5.0 — изменить порог\n/settings — текущие настройки\n/stop — отписаться",
			t.defaultMult,
		))

	case strings.HasPrefix(text, "/multiplier"):
		parts := strings.Fields(text)
		if len(parts) != 2 {
			_ = t.sendMessage(ctx, chatID, "Использование: /multiplier 5.0")
			return
		}
		var mult float64
		if _, err := fmt.Sscanf(parts[1], "%f", &mult); err != nil || mult <= 0 {
			_ = t.sendMessage(ctx, chatID, "❌ Некорректное значение. Пример: /multiplier 5.0")
			return
		}
		u := user.New(chatID, mult)
		if err := t.userRepo.Upsert(ctx, u); err != nil {
			slog.Error("bot: upsert user multiplier", "error", err)
			return
		}
		slog.Info("user updated multiplier", "chatID", chatID, "multiplier", mult)
		_ = t.sendMessage(ctx, chatID, fmt.Sprintf("✅ Порог обновлён: <b>x%.1f</b>", mult))

	case text == "/settings":
		u, err := t.userRepo.Find(ctx, chatID)
		if err != nil {
			_ = t.sendMessage(ctx, chatID, "Ты не подписан. Напиши /start")
			return
		}
		_ = t.sendMessage(ctx, chatID, fmt.Sprintf(
			"⚙️ <b>Настройки</b>\nПорог: <b>x%.1f</b>\nЛукбек: <b>%d дней</b>",
			u.VolumeMultiplier, t.lookbackDays,
		))

	case text == "/stop":
		if err := t.userRepo.Delete(ctx, chatID); err != nil {
			slog.Error("bot: delete user", "error", err)
			return
		}
		slog.Info("user unsubscribed", "chatID", chatID)
		_ = t.sendMessage(ctx, chatID, "👋 Отписан. Напиши /start чтобы вернуться.")
	}
}

// --- HTTP helpers ---

func (t *TelegramNotifier) sendMessage(ctx context.Context, chatID int64, text string) error {
	body, _ := json.Marshal(map[string]any{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "HTML",
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token),
		bytes.NewReader(body))
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
	tag := "в моменте"
	if c.IsClosed {
		tag = "закрытая свеча"
	}
	return fmt.Sprintf(
		"🚨 <b>%s</b>\nОборот: %s USDT\nСреднее за %d дн.: %s USDT\nРатио: x%.2f\n<i>%s</i>",
		c.Symbol, fmtVol(c.Turnover), t.lookbackDays, fmtVol(avg), c.Turnover/avg, tag,
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
