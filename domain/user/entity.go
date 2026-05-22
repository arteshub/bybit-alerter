package user

import "time"

type User struct {
	ChatID           int64
	VolumeMultiplier float64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func New(chatID int64, multiplier float64) *User {
	return &User{
		ChatID:           chatID,
		VolumeMultiplier: multiplier,
	}
}
