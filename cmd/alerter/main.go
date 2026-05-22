package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"volume_pump_checker/application"
	"volume_pump_checker/domain/market"
	"volume_pump_checker/infrastructure/bybit"
	pgrepo "volume_pump_checker/infrastructure/postgres"
	"volume_pump_checker/internal/config"
	"volume_pump_checker/internal/notify"
	"volume_pump_checker/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	setupLogger(cfg.Env)

	db, err := gorm.Open(gormpg.Open(cfg.DatabaseURL), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	if err := pgrepo.Migrate(db); err != nil {
		log.Fatalf("db migrate: %v", err)
	}

	userRepo := pgrepo.NewUserRepository(db)
	vs := store.NewVolumeStore()
	bybitClient := bybit.NewClient(cfg.RESTDelayMS)
	notifier := notify.NewTelegramNotifier(cfg.TGBotToken, cfg.VolumeMultiplier, userRepo)
	alertSvc := application.NewAlertService(userRepo, vs, notifier)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	notifier.Start(ctx)

	slog.Info("fetching symbols")
	symbols, err := bybitClient.FetchSymbols(ctx)
	if err != nil {
		log.Fatalf("fetch symbols: %v", err)
	}
	slog.Info("symbols fetched", "count", len(symbols))

	loadStart := time.Now()
	loadAverages(ctx, bybitClient, vs, symbols, cfg.RESTDelayMS)
	slog.Info("averages loaded", "count", len(symbols), "duration", time.Since(loadStart).Round(time.Second))

	candleHandler := func(candle market.Candle) {
		alertSvc.Handle(ctx, candle)
	}

	if err := bybitClient.Subscribe(ctx, symbols, candleHandler); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	go runDailyRefresh(ctx, bybitClient, vs, candleHandler, symbols, cfg.RESTDelayMS)

	slog.Info("service started")
	bybitClient.Wait()
	slog.Info("service stopped")
}

func setupLogger(env string) {
	var h slog.Handler
	if env == "production" {
		h = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		h = slog.NewTextHandler(os.Stdout, nil)
	}
	slog.SetDefault(slog.New(h))
}

func loadAverages(ctx context.Context, c *bybit.Client, vs *store.VolumeStore, symbols []market.Symbol, delayMS int) {
	total := len(symbols)
	for i, sym := range symbols {
		if ctx.Err() != nil {
			return
		}
		avgs, err := c.FetchMultiAvgTurnover(ctx, sym)
		if err != nil {
			slog.Warn("fetch avg failed, skipping", "symbol", sym, "error", err)
		} else {
			for days, avg := range avgs {
				vs.SetAvg(sym, days, avg)
			}
		}
		if (i+1)%50 == 0 || i+1 == total {
			slog.Info("loading averages", "progress", fmt.Sprintf("%d/%d", i+1, total))
		}
		time.Sleep(time.Duration(delayMS) * time.Millisecond)
	}
}

func runDailyRefresh(ctx context.Context, c *bybit.Client, vs *store.VolumeStore, handler func(market.Candle), current []market.Symbol, delayMS int) {
	for {
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 5, 0, 0, time.UTC)
		select {
		case <-time.After(time.Until(next)):
		case <-ctx.Done():
			return
		}

		start := time.Now()
		fresh, err := c.FetchSymbols(ctx)
		if err != nil {
			slog.Error("daily refresh: fetch symbols", "error", err)
			continue
		}

		added, removed := diffSymbols(current, fresh)
		for _, sym := range removed {
			vs.Delete(sym)
		}
		loadAverages(ctx, c, vs, fresh, delayMS)
		if len(added) > 0 {
			if err := c.AddSubscriptions(ctx, added, handler); err != nil {
				slog.Error("daily refresh: add subscriptions", "error", err)
			}
		}

		slog.Info("daily refresh complete",
			"duration", time.Since(start).Round(time.Second),
			"added", len(added),
			"removed", len(removed),
		)
		current = fresh
	}
}

func diffSymbols(old, new []market.Symbol) (added, removed []market.Symbol) {
	oldSet := make(map[market.Symbol]struct{}, len(old))
	for _, s := range old {
		oldSet[s] = struct{}{}
	}
	newSet := make(map[market.Symbol]struct{}, len(new))
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
