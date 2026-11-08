package database

import (
	"context"
	"fmt"

	"github.com/TicketsBot-cloud/database"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

var (
	Client *database.Database
	Pool   *pgxpool.Pool
)

func Connect(logger *zap.Logger, host, dbName, username, password string, threads int) error {
	pool, err := NewPool(context.Background(), host, dbName, username, password, threads)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	logger.Info("Connected to database")

	Pool = pool
	Client = database.NewDatabase(pool)

	return nil
}

// The pool size is applied to the parsed config rather than interpolated into the URI: an unset
// thread count would render as "pool_max_conns=0", which pgx rejects, taking the worker down.
func NewPool(ctx context.Context, host, dbName, username, password string, threads int) (*pgxpool.Pool, error) {
	uri := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, host, dbName)

	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	if threads > 0 {
		cfg.MaxConns = int32(threads)
	}

	return pgxpool.ConnectConfig(ctx, cfg)
}
