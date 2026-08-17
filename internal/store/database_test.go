package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresAdminConfig(t *testing.T) {
	cfg, targetDatabase, err := postgresAdminConfig("postgres://app:secret@db:5432/notification_delivery?sslmode=disable")
	if err != nil {
		t.Fatalf("postgresAdminConfig() error = %v", err)
	}
	if targetDatabase != "notification_delivery" {
		t.Fatalf("target database = %q, want %q", targetDatabase, "notification_delivery")
	}
	if cfg.Database != "postgres" {
		t.Fatalf("admin database = %q, want %q", cfg.Database, "postgres")
	}
	if cfg.Host != "db" || cfg.Port != 5432 || cfg.User != "app" {
		t.Fatalf("connection identity changed: host=%q port=%d user=%q", cfg.Host, cfg.Port, cfg.User)
	}
}

func TestPostgresOnlyRejectsUnsupportedDrivers(t *testing.T) {
	for _, driver := range []string{"mysql", "mariadb", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			if _, err := resolveDialector("unused", driver); err == nil || !strings.Contains(err.Error(), "only PostgreSQL is supported") {
				t.Fatalf("resolveDialector() error = %v, want PostgreSQL-only error", err)
			}
			if err := EnsureDatabase(context.Background(), "unused", driver); err == nil || !strings.Contains(err.Error(), "only PostgreSQL is supported") {
				t.Fatalf("EnsureDatabase() error = %v, want PostgreSQL-only error", err)
			}
		})
	}
}

func TestEnsureDatabaseIntegration(t *testing.T) {
	baseDSN := os.Getenv("TEST_POSTGRES_DSN")
	if baseDSN == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	targetURL, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse TEST_POSTGRES_DSN: %v", err)
	}
	targetDatabase := fmt.Sprintf("rc_auto_create_test_%d", time.Now().UnixNano())
	targetURL.Path = "/" + targetDatabase
	targetDSN := targetURL.String()
	targetConfig, err := pgx.ParseConfig(targetDSN)
	if err != nil {
		t.Fatalf("parse target PostgreSQL DSN: %v", err)
	}

	adminConfig, _, err := postgresAdminConfig(targetDSN)
	if err != nil {
		t.Fatalf("build admin config: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, connectErr := pgx.ConnectConfig(ctx, adminConfig)
		if connectErr != nil {
			t.Errorf("cleanup connect: %v", connectErr)
			return
		}
		defer func() {
			_ = conn.Close(context.Background())
		}()
		if _, dropErr := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{targetDatabase}.Sanitize()+" WITH (FORCE)"); dropErr != nil {
			t.Errorf("cleanup database %q: %v", targetDatabase, dropErr)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := EnsureDatabase(ctx, targetDSN, "postgres"); err != nil {
		t.Fatalf("EnsureDatabase() first call error = %v", err)
	}
	if err := EnsureDatabase(ctx, targetDSN, "postgres"); err != nil {
		t.Fatalf("EnsureDatabase() idempotent call error = %v", err)
	}

	conn, err := pgx.ConnectConfig(ctx, targetConfig)
	if err != nil {
		t.Fatalf("connect to created database: %v", err)
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()
	var currentDatabase string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil {
		t.Fatalf("query created database: %v", err)
	}
	if currentDatabase != targetDatabase {
		t.Fatalf("current database = %q, want %q", currentDatabase, targetDatabase)
	}
}
