package notify

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"volume_pump_checker/domain/user"
)

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
