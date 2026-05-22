package bybit

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	sdk "github.com/hirokisan/bybit/v2"

	"volume_pump_checker/domain/market"
)

const (
	wsConnSymbols = 200
	wsSubBatch    = 10
)

func (c *Client) Subscribe(ctx context.Context, symbols []market.Symbol, handler func(market.Candle)) error {
	if err := c.openConnections(ctx, symbols, handler); err != nil {
		return err
	}
	slog.Info("subscriptions created", "count", len(symbols))
	return nil
}

func (c *Client) AddSubscriptions(ctx context.Context, symbols []market.Symbol, handler func(market.Candle)) error {
	if len(symbols) == 0 {
		return nil
	}
	if err := c.openConnections(ctx, symbols, handler); err != nil {
		return err
	}
	slog.Info("new subscriptions added", "count", len(symbols))
	return nil
}

// Wait blocks until all WebSocket goroutines have exited.
func (c *Client) Wait() {
	c.wg.Wait()
}

// openConnections creates one WS connection per wsConnSymbols batch.
// Each connection auto-reconnects on failure until ctx is cancelled.
func (c *Client) openConnections(ctx context.Context, symbols []market.Symbol, handler func(market.Candle)) error {
	for i := 0; i < len(symbols); i += wsConnSymbols {
		end := i + wsConnSymbols
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := make([]market.Symbol, end-i)
		copy(batch, symbols[i:end])

		c.wg.Add(1)
		go func(batch []market.Symbol) {
			defer c.wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				err := c.runConnection(ctx, batch, handler)
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

func (c *Client) runConnection(ctx context.Context, batch []market.Symbol, handler func(market.Candle)) error {
	svc, err := c.ws.V5().Public(sdk.CategoryV5Linear)
	if err != nil {
		return fmt.Errorf("create ws service: %w", err)
	}

	klineHandler := func(resp sdk.V5WebsocketPublicKlineResponse) error {
		key := resp.Key()
		sym := market.Symbol(key.Symbol)
		for _, d := range resp.Data {
			turnover, err := strconv.ParseFloat(d.Turnover, 64)
			if err != nil {
				return fmt.Errorf("parse ws turnover %q: %w", d.Turnover, err)
			}
			handler(market.Candle{
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
		keys := make([]sdk.V5WebsocketPublicKlineParamKey, 0, jEnd-j)
		for _, sym := range batch[j:jEnd] {
			keys = append(keys, sdk.V5WebsocketPublicKlineParamKey{
				Interval: sdk.IntervalD,
				Symbol:   sdk.SymbolV5(sym),
			})
		}
		if _, err := svc.SubscribeKlines(keys, klineHandler); err != nil {
			return fmt.Errorf("subscribe klines: %w", err)
		}
	}

	errHandler := sdk.ErrHandler(func(isClosed bool, err error) {
		slog.Warn("ws error", "closed", isClosed, "error", err)
	})
	return svc.Start(ctx, errHandler)
}
