package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const duplicateDatabaseSQLState = "42P04"

// EnsureDatabase creates the database named by dsn when it does not exist.
// PostgreSQL does not support CREATE DATABASE inside a transaction, so this
// connects to the always-present postgres maintenance database first.
func EnsureDatabase(ctx context.Context, dsn, driver string) error {
	if err := validatePostgresDriver(driver); err != nil {
		return err
	}
	return ensurePostgresDatabase(ctx, dsn)
}

func ensurePostgresDatabase(ctx context.Context, dsn string) error {
	adminConfig, targetDatabase, err := postgresAdminConfig(dsn)
	if err != nil {
		return err
	}

	conn, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL maintenance database: %w", err)
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()

	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
		targetDatabase,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check database %q: %w", targetDatabase, err)
	}
	if exists {
		return nil
	}

	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{targetDatabase}.Sanitize())
	if err == nil {
		return nil
	}

	// Server and worker may start concurrently. Treat the other process
	// winning the CREATE DATABASE race as success.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == duplicateDatabaseSQLState {
		return nil
	}
	return fmt.Errorf("create database %q: %w", targetDatabase, err)
}

func postgresAdminConfig(dsn string) (*pgx.ConnConfig, string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, "", fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	targetDatabase := strings.TrimSpace(cfg.Database)
	if targetDatabase == "" {
		return nil, "", fmt.Errorf("PostgreSQL DSN must include a database name")
	}
	cfg.Database = "postgres"
	return cfg, targetDatabase, nil
}
