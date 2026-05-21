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

// Check returns (true, nil) when candle.Turnover exceeds avg*multiplier AND
// this is the first alert for (symbol, date). Returns (false, nil) otherwise.
func (c *Checker) Check(_ context.Context, candle exchange.Candle) (bool, error) {
	avg, ok := c.store.Get(candle.Symbol)
	if !ok {
		return false, nil
	}

	if candle.Turnover < avg*c.multiplier {
		return false, nil
	}

	if !c.store.TryFire(candle.Symbol, candle.Date) {
		return false, nil
	}

	return true, nil
}
