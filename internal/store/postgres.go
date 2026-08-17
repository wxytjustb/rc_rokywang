// Package store implements database persistence for notification_event,
// the single table that acts as event log, delivery task, simplified
// outbox, retry state and worker lease all at once (see DESIGN.md §3).
package store

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

//go:embed migrations_sql/0001_init.sql
var initSQL string

func validatePostgresDriver(driver string) error {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", "postgres", "postgresql", "pg":
		return nil
	default:
		return fmt.Errorf("unsupported database driver %q: only PostgreSQL is supported", driver)
	}
}

func resolveDialector(dsn, driver string) (gorm.Dialector, error) {
	if err := validatePostgresDriver(driver); err != nil {
		return nil, err
	}
	return postgres.Open(dsn), nil
}

// NewPool opens a database connection via GORM and applies connection pool
// sizing from the config values.
func NewPool(ctx context.Context, dsn string, maxConns, minConns int32, driver string) (*gorm.DB, error) {
	dialector, err := resolveDialector(dsn, driver)
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve sql database: %w", err)
	}
	if maxConns > 0 {
		sqlDB.SetMaxOpenConns(int(maxConns))
	}
	if minConns > 0 {
		sqlDB.SetMaxIdleConns(int(minConns))
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return db, nil
}

// Migrate applies the (idempotent) schema DDL. It is intentionally a plain
// "CREATE ... IF NOT EXISTS" script rather than a versioned migration chain:
// the schema is one table and is expected to change rarely, so a full
// migration framework would be more machinery than the problem warrants.
func Migrate(ctx context.Context, pool *gorm.DB) error {
	err := pool.WithContext(ctx).Exec(initSQL).Error
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
