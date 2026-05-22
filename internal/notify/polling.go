package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

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
