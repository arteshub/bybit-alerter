package user

import "time"

const (
	DefaultMultiplier   = 3.0
	DefaultLookbackDays = 90
)

type User struct {
	ChatID           int64
	VolumeMultiplier float64
	LookbackDays     int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func New(chatID int64, multiplier float64, lookbackDays int) *User {
	return &User{
		ChatID:           chatID,
		VolumeMultiplier: multiplier,
		LookbackDays:     lookbackDays,
	}
}
