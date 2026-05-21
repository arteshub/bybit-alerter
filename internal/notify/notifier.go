package notify

import (
	"context"

	"volume_pump_checker/internal/exchange"
)

type Notifier interface {
	Send(ctx context.Context, candle exchange.Candle, avg float64) error
}
