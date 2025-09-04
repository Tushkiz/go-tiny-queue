package queue

import "time"

const (
	StatusPending   = "pending"
	StatusInFlight  = "in_flight"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusDead      = "dead"
)

type Task struct {
	ID             string     `gorm:"primaryKey;type:char(36)"`
	Type           string     `gorm:"type:varchar(255);not null"`
	Payload        []byte     `gorm:"type:json;not null"`
	Status         string     `gorm:"type:varchar(16);not null;default:'pending';index"`
	Priority       int        `gorm:"type:int;not null;default:0;index"`
	Attempt        int        `gorm:"type:int;not null;default:0"`
	MaxAttempts    int        `gorm:"type:int;not null;default:5"`
	LastError      *string    `gorm:"type:text"`
	IdempotencyKey *string    `gorm:"type:varchar(128);uniqueIndex"`
	LeaseExpiresAt *time.Time `gorm:"type:datetime(6);index"`
	NextRunAt      time.Time  `gorm:"type:datetime(6);not null;index"`
	CreatedAt      time.Time  `gorm:"type:datetime(6);not null"`
	UpdatedAt      time.Time  `gorm:"type:datetime(6);not null"`
}

func (Task) TableName() string { return "tasks" }
