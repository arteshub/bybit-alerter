package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	TGBotToken       string
	DatabaseURL      string
	VolumeMultiplier float64
	RESTDelayMS      int
	Env              string
}

func Load() (*Config, error) {
	cfg := &Config{
		VolumeMultiplier: 3.0,
		RESTDelayMS:      50,
		Env:              os.Getenv("ENV"),
	}

	token := os.Getenv("TG_BOT_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("TG_BOT_TOKEN is required")
	}
	cfg.TGBotToken = token

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	cfg.DatabaseURL = dbURL

	if v := os.Getenv("VOLUME_MULTIPLIER"); v != "" {
		m, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("VOLUME_MULTIPLIER invalid: %w", err)
		}
		cfg.VolumeMultiplier = m
	}

	if v := os.Getenv("REST_DELAY_MS"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("REST_DELAY_MS invalid: %w", err)
		}
		cfg.RESTDelayMS = d
	}

	return cfg, nil
}
