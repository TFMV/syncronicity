package snowflake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.uber.org/zap"
)

// Client encapsulates all interactions with Snowflake.
type Client struct {
	DSN    string
	Logger *zap.Logger
}

// NewClient creates a new Snowflake client with the provided DSN and logger.
func NewClient(dsn string, logger *zap.Logger) *Client {
	return &Client{
		DSN:    dsn,
		Logger: logger,
	}
}

// LoadArrowIntoSnowflake connects to Snowflake and executes a COPY command
// to load data from the configured stage.
func (c *Client) LoadArrowIntoSnowflake(ctx context.Context) error {
	// Initialize the Snowflake ADBC driver.
	db, err := snowflake.NewDriver(memory.DefaultAllocator).NewDatabase(map[string]string{
		adbc.OptionKeyURI: c.DSN,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Snowflake database: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open Snowflake connection: %w", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create Snowflake statement: %w", err)
	}
	defer stmt.Close()

	// Set tuning options for parallelism.
	if err = stmt.SetOption("adbc.snowflake.statement.ingest_writer_concurrency", "4"); err != nil {
		return fmt.Errorf("failed to set writer concurrency: %w", err)
	}
	if err = stmt.SetOption("adbc.snowflake.statement.ingest_upload_concurrency", "8"); err != nil {
		return fmt.Errorf("failed to set upload concurrency: %w", err)
	}

	// Execute the COPY command to load data from the stage.
	if err = stmt.SetSqlQuery("COPY INTO foo FROM @SYNCHRONICITY_STAGE FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"); err != nil {
		return fmt.Errorf("failed to set COPY command: %w", err)
	}
	if _, err = stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("failed to execute COPY command: %w", err)
	}

	c.Logger.Info("Arrow record successfully loaded into Snowflake")
	return nil
}

// WriteArrowRecordToParquet writes the provided Arrow record to a Parquet file.
func (c *Client) WriteArrowRecordToParquet(ctx context.Context, record arrow.Record, outputFile string) error {
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return fmt.Errorf("failed to create directory for Parquet file: %w", err)
	}

	// Create (or overwrite) the output file.
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer file.Close()

	// Define Parquet writer properties.
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(64*1024*1024), // 64 MB batch size.
		parquet.WithVersion(parquet.V2_LATEST),
	)
	arrowWriterProps := pqarrow.NewArrowWriterProperties()

	writer, err := pqarrow.NewFileWriter(record.Schema(), file, writerProps, arrowWriterProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write the Arrow record.
	if err := writer.Write(record); err != nil {
		writer.Close() // best-effort cleanup
		return fmt.Errorf("failed to write Arrow record to Parquet: %w", err)
	}

	// Close the writer to flush data.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	c.Logger.Info("Successfully wrote Arrow record to Parquet", zap.String("outputFile", outputFile))
	return nil
}

// UploadParquetToStage uploads the specified Parquet file to a Snowflake stage.
func (c *Client) UploadParquetToStage(ctx context.Context, filePath, stagePath string) error {
	// Verify file exists before attempting upload
	if _, err := os.Stat(filePath); err != nil {
		return fmt.Errorf("parquet file not found at %s: %w", filePath, err)
	}

	// Convert to absolute path for Snowflake PUT command
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	db, err := snowflake.NewDriver(memory.DefaultAllocator).NewDatabase(map[string]string{
		adbc.OptionKeyURI: c.DSN,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Snowflake database: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open Snowflake connection: %w", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement for uploading: %w", err)
	}
	defer stmt.Close()

	// Construct and execute the PUT command.
	if !strings.HasPrefix(stagePath, "@") {
		stagePath = "@" + stagePath
	}
	stagePath = strings.ReplaceAll(stagePath, "@@", "@")

	query := fmt.Sprintf("PUT file://%s %s", absPath, stagePath)
	if err := stmt.SetSqlQuery(query); err != nil {
		return fmt.Errorf("failed to set PUT command: %w", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("failed to execute PUT command: %w", err)
	}

	c.Logger.Info("Parquet file successfully uploaded to Snowflake stage",
		zap.String("file", filePath), zap.String("stage", stagePath))
	return nil
}

// ArrowToParquetStage is a convenience method that writes an Arrow record to Parquet
// and then uploads the file to a specified Snowflake stage.
func (c *Client) ArrowToParquetStage(ctx context.Context, record arrow.Record, outputFile, stagePath string) error {
	if err := c.WriteArrowRecordToParquet(ctx, record, outputFile); err != nil {
		return err
	}
	if err := c.UploadParquetToStage(ctx, outputFile, stagePath); err != nil {
		return err
	}
	return nil
}
