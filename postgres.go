package store

import (
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PgStore struct {
	db     *gorm.DB
	meta   PgMeta
	ch     chan struct{}
	ticker *time.Ticker
}

type Record struct {
	Key        string `gorm:"primaryKey;index:idx_key"`
	Value      string
	IsTTLBased bool      `gorm:"column:is_ttl_based;index:idx_ttl_expires,priority:2"`
	ExpiresAt  time.Time `gorm:"column:expires_at;index:idx_ttl_expires,priority:1"`
	Timestamp  time.Time `gorm:"column:ts;index:idx_timestamp"`
}

type PgMeta struct {
	DriverName       string `json:"driverName" yaml:"driverName"`
	ConnectionString string `json:"connectionString" yaml:"connectionString"`
	DatabaseName     string `json:"databaseName" yaml:"databaseName"`
	TableName        string `json:"tableName" yaml:"tableName"`
	SslMode          bool   `json:"sslMode" yaml:"sslMode"`
	UseCustomSchema  bool   `json:"useCustomSchema" yaml:"useCustomSchema"`
	SchemaName       string `json:"schemaName" yaml:"schemaName"`
	Timezone         string `json:"timezone" yaml:"timezone"`

	CronInterval int64 `json:"cronInterval" yaml:"cronInterval"`
}

func Cron(store PgStore, ch chan struct{}) {
	for {
		select {
		case <-store.ticker.C:
			return
		}
	}
}

func NewPostgresStore(meta PgMeta) (*Store, error) {
	db, err := gorm.Open(postgres.Open(meta.ConnectionString), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if meta.Timezone != "" {
		db = db.Set("gorm:time_zone", meta.Timezone)
	}

	if err := db.AutoMigrate(&Record{}); err != nil {
		return nil, err
	}

	store := &PgStore{
		db:   db,
		meta: meta,
		ch:   make(chan struct{}),
	}

	store.ticker = time.NewTicker(time.Duration(meta.CronInterval) * time.Second)
	return nil, nil
}
