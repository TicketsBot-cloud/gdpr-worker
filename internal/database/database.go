package database

import (
	"context"
	"fmt"

	"github.com/TicketsBot-cloud/database"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

var Client *database.Database

func Connect(logger *zap.Logger, host, dbName, username, password string, threads int) error {
	uri := fmt.Sprintf("postgres://%s:%s@%s/%s?pool_max_conns=%d", username, password, host, dbName, threads)

	pool, err := pgxpool.Connect(context.Background(), uri)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	logger.Info("Connected to database")

	Client = database.NewDatabase(pool)

	return nil
}
