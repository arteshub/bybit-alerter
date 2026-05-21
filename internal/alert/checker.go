package alert

import (
	"context"

	"volume_pump_checker/internal/exchange"
	"volume_pump_checker/internal/store"
)

type Checker struct {
	store      *store.VolumeStore
	multiplier float64
}

func NewChecker(store *store.VolumeStore, multiplier float64) *Checker {
	return &Checker{store: store, multiplier: multiplier}
}

// Check fires only when the candle turnover crosses the threshold from below —
// i.e. the previous update was under avg*multiplier and the current one is over.
// This eliminates startup floods and catches the exact moment a spike begins.
func (c *Checker) Check(_ context.Context, candle exchange.Candle) (bool, error) {
	avg, ok := c.store.Get(candle.Symbol)
	if !ok {
		return false, nil
	}

	threshold := avg * c.multiplier
	prev, hadPrev := c.store.SwapTurnover(candle.Symbol, candle.Turnover)

	// First update for this symbol — just record the baseline, don't alert.
	if !hadPrev {
		return false, nil
	}

	// Alert only on the crossing: was below, now above.
	if prev >= threshold || candle.Turnover < threshold {
		return false, nil
	}

	// Deduplicate: one alert per symbol per day.
	if !c.store.TryFire(candle.Symbol, candle.Date) {
		return false, nil
	}

	return true, nil
}
