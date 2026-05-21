package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"volume_pump_checker/internal/alert"
	"volume_pump_checker/internal/config"
	"volume_pump_checker/internal/exchange"
	"volume_pump_checker/internal/notify"
	"volume_pump_checker/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	var logHandler slog.Handler
	if cfg.Env == "production" {
		logHandler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		logHandler = slog.NewTextHandler(os.Stdout, nil)
	}
	slog.SetDefault(slog.New(logHandler))

	client := exchange.NewBybitClient(cfg.RESTDelayMS)
	vs := store.NewVolumeStore()
	checker := alert.NewChecker(vs, cfg.VolumeMultiplier)
	notifier := notify.NewTelegramNotifier(cfg.TGBotToken, cfg.LookbackDays)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	notifier.Start(ctx)

	slog.Info("fetching symbols")
	symbols, err := client.FetchSymbols(ctx)
	if err != nil {
		log.Fatalf("fetch symbols: %v", err)
	}
	slog.Info("symbols fetched", "count", len(symbols))

	loadStart := time.Now()
	loadAverages(ctx, client, vs, symbols, cfg)
	slog.Info("averages loaded", "count", len(symbols), "duration", time.Since(loadStart).Round(time.Second))

	candleHandler := func(candle exchange.Candle) {
		fired, err := checker.Check(ctx, candle)
		if err != nil {
			slog.Error("checker error", "symbol", candle.Symbol, "error", err)
			return
		}
		if !fired {
			return
		}
		avg, _ := vs.Get(candle.Symbol)
		slog.Info("alert fired",
			"symbol", candle.Symbol,
			"turnover", candle.Turnover,
			"avg", avg,
			"ratio", candle.Turnover/avg,
		)
		if err := notifier.Send(ctx, candle, avg); err != nil {
			slog.Error("notify send error", "error", err)
		}
	}

	// currentSymbols tracks the live set for daily diff.
	currentSymbols := symbols

	// Daily refresh: 00:05 UTC — diff symbols, update store + WS subscriptions.
	go func() {
		for {
			now := time.Now().UTC()
			next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 5, 0, 0, time.UTC)
			select {
			case <-time.After(time.Until(next)):
			case <-ctx.Done():
				return
			}

			start := time.Now()
			newSymbols, err := client.FetchSymbols(ctx)
			if err != nil {
				slog.Error("daily update: fetch symbols failed", "error", err)
				continue
			}

			added, removed := diffSymbols(currentSymbols, newSymbols)

			for _, sym := range removed {
				vs.Delete(sym)
			}

			loadAverages(ctx, client, vs, newSymbols, cfg)

			if len(added) > 0 {
				if err := client.AddSubscriptions(ctx, added, candleHandler); err != nil {
					slog.Error("daily update: add subscriptions failed", "error", err)
				}
			}

			slog.Info("daily update complete",
				"duration", time.Since(start).Round(time.Second),
				"added", len(added),
				"removed", len(removed),
			)
			currentSymbols = newSymbols
		}
	}()

	if err := client.Subscribe(ctx, symbols, candleHandler); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	slog.Info("service started")
	client.Start(ctx, nil)
	slog.Info("service stopped")
}

func loadAverages(ctx context.Context, client *exchange.BybitClient, vs *store.VolumeStore, symbols []exchange.Symbol, cfg *config.Config) {
	for _, sym := range symbols {
		if ctx.Err() != nil {
			return
		}
		avg, err := client.FetchAvgTurnover(ctx, sym, cfg.LookbackDays)
		if err != nil {
			slog.Warn("fetch avg failed, skipping", "symbol", sym, "error", err)
			continue
		}
		vs.Set(sym, avg)
		time.Sleep(time.Duration(cfg.RESTDelayMS) * time.Millisecond)
	}
}

func diffSymbols(old, new []exchange.Symbol) (added, removed []exchange.Symbol) {
	oldSet := make(map[exchange.Symbol]struct{}, len(old))
	for _, s := range old {
		oldSet[s] = struct{}{}
	}
	newSet := make(map[exchange.Symbol]struct{}, len(new))
	for _, s := range new {
		newSet[s] = struct{}{}
	}
	for _, s := range new {
		if _, ok := oldSet[s]; !ok {
			added = append(added, s)
		}
	}
	for _, s := range old {
		if _, ok := newSet[s]; !ok {
			removed = append(removed, s)
		}
	}
	return
}
