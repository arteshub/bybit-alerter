package notify

import (
	"context"

	"volume_pump_checker/internal/exchange"
)

// Sender delivers an alert for a specific chat ID.
type Sender interface {
	SendToUser(ctx context.Context, chatID int64, candle exchange.Candle, avg float64) error
}
