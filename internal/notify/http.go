package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

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

func formatAlert(p userPayload) string {
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
