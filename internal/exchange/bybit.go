package exchange

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hirokisan/bybit/v2"
)

const (
	// wsConnSymbols is the max symbols per WebSocket connection.
	wsConnSymbols = 200
	// wsSubBatch is the max topics per single subscribe message (Bybit limit).
	wsSubBatch = 10
)

type BybitClient struct {
	rest    *bybit.Client
	ws      *bybit.WebSocketClient
	delayMS int
	wg      sync.WaitGroup
}

func NewBybitClient(delayMS int) *BybitClient {
	return &BybitClient{
		rest:    bybit.NewClient(),
		ws:      bybit.NewWebsocketClient(),
		delayMS: delayMS,
	}
}

// FetchSymbols returns all active USDT-margined linear perpetuals.
func (b *BybitClient) FetchSymbols(ctx context.Context) ([]Symbol, error) {
	limit := 1000
	var (
		symbols []Symbol
		cursor  string
	)

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		param := bybit.V5GetInstrumentsInfoParam{
			Category: bybit.CategoryV5Linear,
			Limit:    &limit,
		}
		if cursor != "" {
			param.Cursor = &cursor
		}

		resp, err := b.rest.V5().Market().GetInstrumentsInfo(param)
		if err != nil {
			return nil, fmt.Errorf("get instruments info: %w", err)
		}

		li := resp.Result.LinearInverse
		if li == nil {
			break
		}

		for _, item := range li.List {
			if item.Status == bybit.InstrumentStatusTrading &&
				strings.HasSuffix(string(item.Symbol), "USDT") {
				symbols = append(symbols, Symbol(item.Symbol))
			}
		}

		if li.NextPageCursor == "" {
			break
		}
		cursor = li.NextPageCursor
	}

	return symbols, nil
}

// SupportedPeriods are the lookback periods (days) available to users.
var SupportedPeriods = []int{30, 60, 90}

// FetchMultiAvgTurnover fetches one batch of klines and returns average daily
// turnover for each period in SupportedPeriods. Retries up to 3 times on error.
func (b *BybitClient) FetchMultiAvgTurnover(ctx context.Context, sym Symbol) (map[int]float64, error) {
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

		result, err := b.fetchMultiAvgOnce(sym)
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

// errNoData is returned when there are no closed candles available (not retryable).
type errNoData struct{ sym Symbol }

func (e errNoData) Error() string { return string(e.sym) + ": no closed candle data available" }

func (b *BybitClient) fetchMultiAvgOnce(sym Symbol) (map[int]float64, error) {
	maxDays := SupportedPeriods[len(SupportedPeriods)-1]
	limit := maxDays + 1
	resp, err := b.rest.V5().Market().GetKline(bybit.V5GetKlineParam{
		Category: bybit.CategoryV5Linear,
		Symbol:   bybit.SymbolV5(sym),
		Interval: bybit.IntervalD,
		Limit:    &limit,
	})
	if err != nil {
		return nil, err
	}

	list := resp.Result.List
	if len(list) < 2 {
		return nil, errNoData{sym}
	}

	// list[0] is the current (unclosed) candle — discard. list is newest-first.
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

// Subscribe opens WebSocket connections for all symbols and calls handler on
// every kline update. Returns immediately; connections run in background
// goroutines tracked by an internal WaitGroup.
func (b *BybitClient) Subscribe(ctx context.Context, symbols []Symbol, handler func(Candle)) error {
	if err := b.openConnections(ctx, symbols, handler); err != nil {
		return err
	}
	slog.Info("subscriptions created", "count", len(symbols))
	return nil
}

// AddSubscriptions subscribes to additional symbols on new connections.
// Safe to call concurrently with running connections.
func (b *BybitClient) AddSubscriptions(ctx context.Context, symbols []Symbol, handler func(Candle)) error {
	if len(symbols) == 0 {
		return nil
	}
	if err := b.openConnections(ctx, symbols, handler); err != nil {
		return err
	}
	slog.Info("new subscriptions added", "count", len(symbols))
	return nil
}

// openConnections creates one WS connection per wsConnSymbols batch.
// Each connection auto-reconnects on failure until ctx is cancelled.
func (b *BybitClient) openConnections(ctx context.Context, symbols []Symbol, handler func(Candle)) error {
	for i := 0; i < len(symbols); i += wsConnSymbols {
		end := i + wsConnSymbols
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := make([]Symbol, end-i)
		copy(batch, symbols[i:end])

		b.wg.Add(1)
		go func(batch []Symbol) {
			defer b.wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				err := b.runConnection(ctx, batch, handler)
				if ctx.Err() != nil {
					return
				}
				slog.Warn("ws connection dropped, reconnecting in 5s", "error", err, "symbols", len(batch))
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}(batch)
	}
	return nil
}

func (b *BybitClient) runConnection(ctx context.Context, batch []Symbol, handler func(Candle)) error {
	svc, err := b.ws.V5().Public(bybit.CategoryV5Linear)
	if err != nil {
		return fmt.Errorf("create ws public service: %w", err)
	}

	klineHandler := func(resp bybit.V5WebsocketPublicKlineResponse) error {
		key := resp.Key()
		sym := Symbol(key.Symbol)
		for _, d := range resp.Data {
			turnover, err := strconv.ParseFloat(d.Turnover, 64)
			if err != nil {
				return fmt.Errorf("parse ws turnover %q: %w", d.Turnover, err)
			}
			handler(Candle{
				Symbol:   sym,
				Turnover: turnover,
				Date:     time.UnixMilli(d.Start).UTC().Format("2006-01-02"),
				IsClosed: d.Confirm,
			})
		}
		return nil
	}

	for j := 0; j < len(batch); j += wsSubBatch {
		jEnd := j + wsSubBatch
		if jEnd > len(batch) {
			jEnd = len(batch)
		}
		keys := make([]bybit.V5WebsocketPublicKlineParamKey, 0, jEnd-j)
		for _, sym := range batch[j:jEnd] {
			keys = append(keys, bybit.V5WebsocketPublicKlineParamKey{
				Interval: bybit.IntervalD,
				Symbol:   bybit.SymbolV5(sym),
			})
		}
		if _, err := svc.SubscribeKlines(keys, klineHandler); err != nil {
			return fmt.Errorf("subscribe klines: %w", err)
		}
	}

	errHandler := bybit.ErrHandler(func(isClosed bool, err error) {
		slog.Warn("ws error", "closed", isClosed, "error", err)
	})
	return svc.Start(ctx, errHandler)
}

// Start blocks until all WebSocket goroutines have exited.
func (b *BybitClient) Start(ctx context.Context, _ chan error) {
	b.wg.Wait()
}
