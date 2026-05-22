package bybit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	sdk "github.com/hirokisan/bybit/v2"

	"volume_pump_checker/domain/market"
)

// SupportedPeriods are the lookback windows (days) available for avg turnover.
var SupportedPeriods = []int{30, 60, 90}

func (c *Client) FetchSymbols(ctx context.Context) ([]market.Symbol, error) {
	limit := 1000
	var (
		symbols []market.Symbol
		cursor  string
	)

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		param := sdk.V5GetInstrumentsInfoParam{
			Category: sdk.CategoryV5Linear,
			Limit:    &limit,
		}
		if cursor != "" {
			param.Cursor = &cursor
		}

		resp, err := c.rest.V5().Market().GetInstrumentsInfo(param)
		if err != nil {
			return nil, fmt.Errorf("get instruments info: %w", err)
		}

		li := resp.Result.LinearInverse
		if li == nil {
			break
		}

		for _, item := range li.List {
			if item.Status == sdk.InstrumentStatusTrading &&
				strings.HasSuffix(string(item.Symbol), "USDT") {
				symbols = append(symbols, market.Symbol(item.Symbol))
			}
		}

		if li.NextPageCursor == "" {
			break
		}
		cursor = li.NextPageCursor
	}

	return symbols, nil
}

// FetchMultiAvgTurnover fetches one batch of klines and returns average daily
// turnover for each period in SupportedPeriods.
func (c *Client) FetchMultiAvgTurnover(ctx context.Context, sym market.Symbol) (map[int]float64, error) {
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		if attempt > 0 {
			slog.Warn("retrying FetchMultiAvgTurnover", "symbol", sym, "error", lastErr, "attempt", attempt)
			select {
			case <-time.After(delays[attempt-1]):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		result, err := c.fetchMultiAvgOnce(sym)
		if err == nil {
			return result, nil
		}
		var noData errNoData
		if errors.As(err, &noData) {
			return nil, err
		}
		lastErr = err
	}

	return nil, fmt.Errorf("%s: after %d retries: %w", sym, len(delays), lastErr)
}

type errNoData struct{ sym market.Symbol }

func (e errNoData) Error() string { return string(e.sym) + ": no closed candle data available" }

func (c *Client) fetchMultiAvgOnce(sym market.Symbol) (map[int]float64, error) {
	maxDays := SupportedPeriods[len(SupportedPeriods)-1]
	limit := maxDays + 1

	resp, err := c.rest.V5().Market().GetKline(sdk.V5GetKlineParam{
		Category: sdk.CategoryV5Linear,
		Symbol:   sdk.SymbolV5(sym),
		Interval: sdk.IntervalD,
		Limit:    &limit,
	})
	if err != nil {
		return nil, err
	}

	list := resp.Result.List
	if len(list) < 2 {
		return nil, errNoData{sym}
	}

	// list[0] is the current unclosed candle; list is newest-first.
	closed := list[1:]

	result := make(map[int]float64, len(SupportedPeriods))
	for _, days := range SupportedPeriods {
		n := days
		if n > len(closed) {
			n = len(closed)
		}
		var sum float64
		for _, item := range closed[:n] {
			t, err := strconv.ParseFloat(item.Turnover, 64)
			if err != nil {
				return nil, fmt.Errorf("parse turnover %q: %w", item.Turnover, err)
			}
			sum += t
		}
		result[days] = sum / float64(n)
	}
	return result, nil
}
