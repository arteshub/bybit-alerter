package postgres

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"volume_pump_checker/domain/user"
)

type userModel struct {
	ChatID           int64     `gorm:"primaryKey"`
	VolumeMultiplier float64   `gorm:"not null;default:3.0"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (userModel) TableName() string { return "users" }

func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&userModel{})
}

type UserRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Upsert(ctx context.Context, u *user.User) error {
	m := userModel{
		ChatID:           u.ChatID,
		VolumeMultiplier: u.VolumeMultiplier,
	}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chat_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"volume_multiplier", "updated_at"}),
		}).
		Create(&m).Error
}

func (r *UserRepository) All(ctx context.Context) ([]*user.User, error) {
	var models []userModel
	if err := r.db.WithContext(ctx).Find(&models).Error; err != nil {
		return nil, err
	}
	out := make([]*user.User, len(models))
	for i, m := range models {
		out[i] = toEntity(m)
	}
	return out, nil
}

func (r *UserRepository) Find(ctx context.Context, chatID int64) (*user.User, error) {
	var m userModel
	if err := r.db.WithContext(ctx).First(&m, chatID).Error; err != nil {
		return nil, err
	}
	return toEntity(m), nil
}

func (r *UserRepository) Delete(ctx context.Context, chatID int64) error {
	return r.db.WithContext(ctx).Delete(&userModel{}, chatID).Error
}

func toEntity(m userModel) *user.User {
	return &user.User{
		ChatID:           m.ChatID,
		VolumeMultiplier: m.VolumeMultiplier,
		CreatedAt:        m.CreatedAt,
		UpdatedAt:        m.UpdatedAt,
	}
}
