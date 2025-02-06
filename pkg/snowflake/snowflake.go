package snowflake

import (
	"context"
	"database/sql"
	"log"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	_ "github.com/snowflakedb/gosnowflake"
)

// Snowflake credentials (Load from config in production)
const (
	snowflakeDSN = "user:password@account/dbname/schema"
	externalFunc = "CALL my_database.public.load_arrow(?)"
)

type SnowflakeClient struct {
	db *sql.DB
}

func NewSnowflakeClient(ctx context.Context, snowflakeDSN string) (*SnowflakeClient, error) {
	db, err := sql.Open("snowflake", snowflakeDSN)
	if err != nil {
		return nil, err
	}
	return &SnowflakeClient{db: db}, nil
}

func (c *SnowflakeClient) Close() error {
	return c.db.Close()
}

// SendArrowToSnowflake sends an Arrow RecordBatch to Snowflake
func SendArrowToSnowflake(ctx context.Context, writer *ipc.Writer) error {
	db, err := sql.Open("snowflake", snowflakeDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// Convert Arrow batch to bytes
	arrowData := writer

	_, err = db.ExecContext(ctx, externalFunc, arrowData)
	if err != nil {
		return err
	}
	log.Println("Successfully loaded Arrow data into Snowflake")
	return nil
}
