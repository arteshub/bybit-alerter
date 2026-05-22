package user

import "context"

type Repository interface {
	Upsert(ctx context.Context, u *User) error
	All(ctx context.Context) ([]*User, error)
	Find(ctx context.Context, chatID int64) (*User, error)
	Delete(ctx context.Context, chatID int64) error
}
