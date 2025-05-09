package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PgStore struct {
	db     *gorm.DB
	meta   PgMeta
	pgCtx  context.Context
	cancel context.CancelFunc
	ticker *time.Ticker
}

type Record struct {
	Key        string    `gorm:"primaryKey;index:idx_key"`
	Value      string    `gorm:"not null"`
	IsTTLBased bool      `gorm:"column:is_ttl_based;index:idx_is_ttl_based;default:false;not null"`
	ExpiresAt  time.Time `gorm:"column:expires_at;index:idx_expires_at"`
	Timestamp  time.Time `gorm:"column:ts;index:idx_ts"`
}

type PgMeta struct {
	Host         string `json:"host" yaml:"host"`
	Port         string `json:"port" yaml:"port"`
	User         string `json:"user" yaml:"user"`
	Password     string `json:"password" yaml:"password"`
	DatabaseName string `json:"databaseName" yaml:"databaseName"`
	TableName    string `json:"tableName" yaml:"tableName"`
	SslMode      string `json:"sslMode" yaml:"sslMode"`
	Timezone     string `json:"timezone" yaml:"timezone"`
	CronInterval int64  `json:"cronInterval" yaml:"cronInterval"`
}

func NewPostgresStore(meta PgMeta) (Store, error) {
	baseConnectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=%s",
		meta.Host, meta.Port, meta.User, meta.Password, meta.SslMode)

	tdb, err := gorm.Open(postgres.Open(baseConnectionString), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := createDatabaseIfNotExists(tdb, meta.DatabaseName); err != nil {
		log.Printf("Error in creatingDatabaseIfNotExists: %v", err)
		return nil, err
	}

	sqlDB, err := tdb.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}
	sqlDB.Close()

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		meta.Host, meta.Port, meta.User, meta.Password, meta.DatabaseName, meta.SslMode)
	db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database %s: %w", meta.DatabaseName, err)
	}

	if meta.Timezone != "" {
		db = db.Set("gorm:time_zone", meta.Timezone)
	}

	if meta.TableName != "" {
		db = db.Table(meta.TableName)
	}

	if err := db.AutoMigrate(&Record{}); err != nil {
		return nil, err
	}

	pgCtx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(time.Duration(meta.CronInterval) * time.Second)
	store := &PgStore{
		db:     db,
		meta:   meta,
		pgCtx:  pgCtx,
		cancel: cancel,
		ticker: ticker,
	}

	go store.cleanupRoutine()
	return store, nil
}

func (s *PgStore) cleanupRoutine() {
	log.Printf("Starting TTL cleanup routine, interval: %d seconds", s.meta.CronInterval)

	for {
		select {
		case <-s.ticker.C:
			if err := s.cleanExpiredRecords(); err != nil {
				log.Printf("Error during TTL cleanup: %v", err)
			}
		case <-s.pgCtx.Done():
			log.Println("Stopping TTL cleanup routine")
			s.ticker.Stop()
			return
		}
	}
}

func (s *PgStore) cleanExpiredRecords() error {
	now := time.Now()
	result := s.db.Where("is_ttl_based = ? AND expires_at <= ?", true, now).Delete(&Record{})

	if result.Error != nil {
		return fmt.Errorf("failed to delete expired records: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		log.Printf("Cleaned up %d expired records", result.RowsAffected)
	}

	return nil
}

func createDatabaseIfNotExists(db *gorm.DB, dbName string) error {
	var exists bool
	result := db.Raw("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = ?)", dbName).Scan(&exists)
	if result.Error != nil {
		return result.Error
	}

	if !exists {
		createSQL := fmt.Sprintf("CREATE DATABASE %s", dbName)
		return db.Exec(createSQL).Error
	}

	return nil
}

func (s *PgStore) Get(ctx context.Context, key string) (response []byte, meta *TsMeta, err error) {
	var record Record
	result := s.db.Where("key = ?", key).First(&record)
	if result.Error != nil {
		if result.Error.Error() == "record not found" {
			return nil, nil, fmt.Errorf("key %s not found", key)
		}
		return nil, nil, fmt.Errorf("failed to get record: %w", result.Error)
	}

	meta = &TsMeta{
		CreatedAt: record.Timestamp.Format(time.RFC3339),
	}

	if record.IsTTLBased {
		meta.ExpiresAt = record.ExpiresAt.Format(time.RFC3339)
	}

	return []byte(record.Value), meta, nil
}

// func (s *PgStore) GetKeyValueByTimestamp(ctx context.Context, pattern, fromTimestamp, toTimestamp string) chan *GetKeyValueByTimestampC {
// 	ch := make(chan *GetKeyValueByTimestampC, 100)

// 	go func() {
// 		defer close(ch)

// 		var fromTime, toTime time.Time
// 		var err error

// 		if fromTimestamp != "" {
// 			fromTime, err = time.Parse(time.RFC3339, fromTimestamp)
// 			if err != nil {
// 				ch <- &GetKeyValueByTimestampC{
// 					Response: nil,
// 					Err:      fmt.Errorf("invalid from timestamp format: %w", err),
// 				}
// 				return
// 			}
// 		}

// 		if toTimestamp != "" {
// 			toTime, err = time.Parse(time.RFC3339, toTimestamp)
// 			if err != nil {
// 				ch <- &GetKeyValueByTimestampC{
// 					Response: nil,
// 					Err:      fmt.Errorf("invalid to timestamp format: %w", err),
// 				}
// 				return
// 			}
// 		} else {
// 			toTime = time.Now()
// 		}

// 		query := s.db.Where("key LIKE ?", strings.ReplaceAll(pattern, "*", "%"))

// 		if !fromTime.IsZero() {
// 			query = query.Where("ts >= ?", fromTime)
// 		}

// 		if !toTime.IsZero() {
// 			query = query.Where("ts <= ?", toTime)
// 		}

// 		rows, err := query.Model(&Record{}).Rows()
// 		if err != nil {
// 			ch <- &GetKeyValueByTimestampC{
// 				Response: nil,
// 				Err:      fmt.Errorf("failed to execute query: %w", err),
// 			}
// 			return
// 		}
// 		defer rows.Close()

// 		for rows.Next() {
// 			var record Record
// 			if err := s.db.ScanRows(rows, &record); err != nil {
// 				ch <- &GetKeyValueByTimestampC{
// 					Response: nil,
// 					Err:      fmt.Errorf("failed to scan row: %w", err),
// 				}
// 				continue
// 			}

// 			ch <- &GetKeyValueByTimestampC{
// 				Response: &KeyValuePair{
// 					Key:   []byte(record.Key),
// 					Value: []byte(record.Value),
// 				},
// 				Err: nil,
// 			}
// 		}
// 	}()

// 	return ch
// }

func (s *PgStore) GetByQuery(ctx context.Context, pattern, query, rest string) (*QueryPaginatedResponse, error) {
	var records []Record
	var totalRecords int64

	dbQuery := s.db.Where("key LIKE ?", strings.ReplaceAll(pattern, "*", "%"))

	if query != "" {
		var filters map[string]interface{}
		if err := json.Unmarshal([]byte(query), &filters); err != nil {
			return nil, fmt.Errorf("invalid query format: %w", err)
		}

		for k, v := range filters {
			dbQuery = dbQuery.Where(k, v)
		}
	}

	if err := dbQuery.Model(&Record{}).Count(&totalRecords).Error; err != nil {
		return nil, fmt.Errorf("failed to count records: %w", err)
	}

	if rest != "" {
		var pagination map[string]int
		if err := json.Unmarshal([]byte(rest), &pagination); err != nil {
			return nil, fmt.Errorf("invalid pagination format: %w", err)
		}

		if limit, ok := pagination["limit"]; ok {
			dbQuery = dbQuery.Limit(limit)
		}

		if offset, ok := pagination["offset"]; ok {
			dbQuery = dbQuery.Offset(offset)
		}
	}

	dbQuery = dbQuery.Order("ts DESC")

	if err := dbQuery.Find(&records).Error; err != nil {
		return nil, fmt.Errorf("failed to fetch records: %w", err)
	}

	queryResponse := make([]QueryResponse, len(records))
	for i, record := range records {
		queryResponse[i] = QueryResponse{
			Data:      []byte(record.Value),
			CreatedAt: record.Timestamp.Format(time.RFC3339),
		}

		if record.IsTTLBased {
			queryResponse[i].ExpiresAt = record.ExpiresAt.Format(time.RFC3339)
		}
	}

	return &QueryPaginatedResponse{
		QueryResponse: queryResponse,
		TotalRecords:  int(totalRecords),
	}, nil
}

func (s *PgStore) Set(ctx context.Context, key string, value []byte) error {
	record := Record{
		Key:        key,
		Value:      string(value),
		IsTTLBased: false,
		Timestamp:  time.Now(),
	}

	result := s.db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "is_ttl_based", "expires_at", "ts"}),
		},
	).Create(&record)

	if result.Error != nil {
		return fmt.Errorf("failed to set record: %w", result.Error)
	}

	return nil
}

func (s *PgStore) SetWithTTL(ctx context.Context, key string, value []byte, duration time.Duration) error {
	now := time.Now()
	record := Record{
		Key:        key,
		Value:      string(value),
		IsTTLBased: true,
		ExpiresAt:  now.Add(duration),
		Timestamp:  now,
	}

	result := s.db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "is_ttl_based", "expires_at", "ts"}),
		},
	).Create(&record)

	if result.Error != nil {
		return fmt.Errorf("failed to set record with TTL: %w", result.Error)
	}

	return nil
}

func (s *PgStore) Delete(ctx context.Context, key string) error {
	result := s.db.Where("key = ?", key).Delete(&Record{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete record: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("key %s not found", key)
	}

	return nil
}

func (s *PgStore) CloseConn() error {
	s.cancel()
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}

	return nil
}
