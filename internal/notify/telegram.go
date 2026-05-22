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

var (
	multOptions = []float64{2, 3, 5, 7, 10, 15}
	daysOptions = []int{30, 60, 90}
)

type userPayload struct {
	chatID       int64
	candle       exchange.Candle
	avg          float64
	lookbackDays int
}

type inlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type inlineKeyboard struct {
	InlineKeyboard [][]inlineButton `json:"inline_keyboard"`
}

// TelegramNotifier handles alert delivery and bot command processing.
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
		slog.Warn("telegram queue full, dropping alert", "symbol", candle.Symbol, "chatID", chatID)
	}
	return nil
}

func (t *TelegramNotifier) worker(ctx context.Context) {
	for {
		select {
		case p := <-t.queue:
			if err := t.sendMessage(ctx, p.chatID, t.formatAlert(p), nil); err != nil {
				slog.Error("telegram send error", "chatID", p.chatID, "error", err)
			}
			time.Sleep(50 * time.Millisecond)
		case <-ctx.Done():
			for {
				select {
				case p := <-t.queue:
					_ = t.sendMessage(context.Background(), p.chatID, t.formatAlert(p), nil)
					time.Sleep(50 * time.Millisecond)
				default:
					return
				}
			}
		}
	}
}

// --- keyboards ---

func settingsMenu(u *user.User) (string, inlineKeyboard) {
	text := fmt.Sprintf(
		"⚙️ <b>Настройки</b>\n\nМножитель: <b>x%.0f</b>\nПериод: <b>%d дн.</b>",
		u.VolumeMultiplier, u.LookbackDays,
	)
	kb := inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "📊 Множитель", CallbackData: "mult"}, {Text: "📅 Период", CallbackData: "days"}},
		{{Text: "🚫 Отписаться", CallbackData: "stop"}},
	}}
	return text, kb
}

func multMenu(current float64) (string, inlineKeyboard) {
	var rows [][]inlineButton
	var row []inlineButton
	for i, m := range multOptions {
		label := fmt.Sprintf("x%.0f", m)
		if m == current {
			label = "✅ " + label
		}
		row = append(row, inlineButton{Text: label, CallbackData: fmt.Sprintf("mult:%d", int(m))})
		if len(row) == 3 || i == len(multOptions)-1 {
			rows = append(rows, row)
			row = nil
		}
	}
	rows = append(rows, []inlineButton{{Text: "◀️ Назад", CallbackData: "settings"}})
	return "📊 <b>Множитель объёма</b>\n\nАлерт срабатывает когда оборот превышает среднее в N раз:", inlineKeyboard{InlineKeyboard: rows}
}

func daysMenu(current int) (string, inlineKeyboard) {
	var row []inlineButton
	for _, d := range daysOptions {
		label := fmt.Sprintf("%d дн.", d)
		if d == current {
			label = "✅ " + label
		}
		row = append(row, inlineButton{Text: label, CallbackData: fmt.Sprintf("days:%d", d)})
	}
	kb := inlineKeyboard{InlineKeyboard: [][]inlineButton{
		row,
		{{Text: "◀️ Назад", CallbackData: "settings"}},
	}}
	return "📅 <b>Период усреднения</b>\n\nСреднее считается за выбранное количество дней:", kb
}

// --- update polling ---

type tgMessage struct {
	MessageID int64 `json:"message_id"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	Text string `json:"text"`
}

type tgCallbackQuery struct {
	ID   string `json:"id"`
	From struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Message tgMessage `json:"message"`
	Data    string    `json:"data"`
}

type tgUpdate struct {
	UpdateID      int64            `json:"update_id"`
	Message       *tgMessage       `json:"message"`
	CallbackQuery *tgCallbackQuery `json:"callback_query"`
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
		"https://api.telegram.org/bot%s/getUpdates?timeout=10&offset=%d&allowed_updates=%%5B%%22message%%22%%2C%%22callback_query%%22%%5D",
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
	if upd.CallbackQuery != nil {
		t.handleCallback(ctx, upd.CallbackQuery)
		return
	}
	if upd.Message == nil || strings.TrimSpace(upd.Message.Text) != "/start" {
		return
	}

	chatID := upd.Message.Chat.ID

	u, err := t.userRepo.Find(ctx, chatID)
	if err != nil {
		u = user.New(chatID, t.defaultMult, user.DefaultLookbackDays)
		if err := t.userRepo.Upsert(ctx, u); err != nil {
			slog.Error("bot: upsert user", "error", err)
			return
		}
		slog.Info("new subscriber", "chatID", chatID)
	}

	menuText, kb := settingsMenu(u)
	_ = t.sendMessage(ctx, chatID, "✅ <b>Bybit Volume Alerter</b>\n\n"+menuText, &kb)
}

func (t *TelegramNotifier) handleCallback(ctx context.Context, cb *tgCallbackQuery) {
	_ = t.answerCallback(ctx, cb.ID)

	chatID := cb.From.ID
	msgID := cb.Message.MessageID

	u, err := t.userRepo.Find(ctx, chatID)
	if err != nil {
		_ = t.editMessage(ctx, chatID, msgID, "Ты не подписан. Напиши /start", nil)
		return
	}

	switch {
	case cb.Data == "settings":
		text, kb := settingsMenu(u)
		_ = t.editMessage(ctx, chatID, msgID, text, &kb)

	case cb.Data == "mult":
		text, kb := multMenu(u.VolumeMultiplier)
		_ = t.editMessage(ctx, chatID, msgID, text, &kb)

	case cb.Data == "days":
		text, kb := daysMenu(u.LookbackDays)
		_ = t.editMessage(ctx, chatID, msgID, text, &kb)

	case strings.HasPrefix(cb.Data, "mult:"):
		var mult int
		if n, _ := fmt.Sscanf(cb.Data[5:], "%d", &mult); n != 1 || mult <= 0 {
			return
		}
		u.VolumeMultiplier = float64(mult)
		if err := t.userRepo.Upsert(ctx, u); err != nil {
			slog.Error("bot: upsert multiplier", "error", err)
			return
		}
		slog.Info("user updated multiplier", "chatID", chatID, "mult", mult)
		text, kb := settingsMenu(u)
		_ = t.editMessage(ctx, chatID, msgID, text, &kb)

	case strings.HasPrefix(cb.Data, "days:"):
		var days int
		if n, _ := fmt.Sscanf(cb.Data[5:], "%d", &days); n != 1 || days <= 0 {
			return
		}
		u.LookbackDays = days
		if err := t.userRepo.Upsert(ctx, u); err != nil {
			slog.Error("bot: upsert lookback", "error", err)
			return
		}
		slog.Info("user updated lookback", "chatID", chatID, "days", days)
		text, kb := settingsMenu(u)
		_ = t.editMessage(ctx, chatID, msgID, text, &kb)

	case cb.Data == "stop":
		if err := t.userRepo.Delete(ctx, chatID); err != nil {
			slog.Error("bot: delete user", "error", err)
			return
		}
		slog.Info("user unsubscribed", "chatID", chatID)
		_ = t.editMessage(ctx, chatID, msgID, "👋 Отписан. Напиши /start чтобы вернуться.", nil)
	}
}

// --- HTTP helpers ---

func (t *TelegramNotifier) sendMessage(ctx context.Context, chatID int64, text string, kb *inlineKeyboard) error {
	payload := map[string]any{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "HTML",
	}
	if kb != nil {
		payload["reply_markup"] = kb
	}
	return t.post(ctx, "sendMessage", payload)
}

func (t *TelegramNotifier) editMessage(ctx context.Context, chatID int64, msgID int64, text string, kb *inlineKeyboard) error {
	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": msgID,
		"text":       text,
		"parse_mode": "HTML",
	}
	if kb != nil {
		payload["reply_markup"] = kb
	}
	return t.post(ctx, "editMessageText", payload)
}

func (t *TelegramNotifier) answerCallback(ctx context.Context, id string) error {
	return t.post(ctx, "answerCallbackQuery", map[string]any{"callback_query_id": id})
}

func (t *TelegramNotifier) post(ctx context.Context, method string, payload any) error {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("https://api.telegram.org/bot%s/%s", t.token, method),
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
		return fmt.Errorf("telegram %s %d: %s", method, resp.StatusCode, string(b))
	}
	return nil
}

func (t *TelegramNotifier) formatAlert(p userPayload) string {
	tag := "в моменте"
	if p.candle.IsClosed {
		tag = "закрытая свеча"
	}
	return fmt.Sprintf(
		"🚨 <b>%s</b>\nОборот: %s USDT\nСреднее за %d дн.: %s USDT\nРатио: x%.2f\n<i>%s</i>",
		p.candle.Symbol, fmtVol(p.candle.Turnover), p.lookbackDays, fmtVol(p.avg), p.candle.Turnover/p.avg, tag,
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
