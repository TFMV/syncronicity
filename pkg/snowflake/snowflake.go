package snowflake

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func LoadArrowIntoSnowflake(snowflakeDSN string) error {
	// Initialize Snowflake ADBC
	db, err := snowflake.NewDriver(memory.DefaultAllocator).NewDatabase(map[string]string{
		adbc.OptionKeyURI: snowflakeDSN,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Open connection
	conn, err := db.Open(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Execute ingestion using ADBC
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.SetOption("adbc.snowflake.statement.ingest_writer_concurrency", "4") // Tune parallel writes
	if err != nil {
		return err
	}

	err = stmt.SetOption("adbc.snowflake.statement.ingest_upload_concurrency", "8") // Tune parallel uploads
	if err != nil {
		return err
	}

	err = stmt.SetSqlQuery("COPY INTO foo FROM @SYNCHRONICITY_STAGE FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE")
	if err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	if err != nil {
		return err
	}

	log.Println("Arrow Record successfully loaded into Snowflake!")
	return nil
}

func ArrowToParquetStage(snowflakeDSN string, record arrow.Record, stagePath string) error {
	// Create a Parquet file
	parquetFile, err := os.Create("arrow_record.parquet")
	if err != nil {
		return fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer parquetFile.Close()

	// Define Parquet writer properties
	parquetWriterProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(64*1024*1024), // 64MB batch size
		parquet.WithVersion(parquet.V2_LATEST),
	)

	// Create a Parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), parquetFile, parquetWriterProps, pqarrow.NewArrowWriterProperties())
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer writer.Close()

	// Write the Arrow record to the Parquet file
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to Parquet: %w", err)
	}

	// Close the writer to flush data to disk
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	// Upload the Parquet file to the Snowflake stage
	if err := uploadToSnowflakeStage(snowflakeDSN, "/Users/thomasmcgeehan/syncronicity/syncronicity/cmd/arrow_record.parquet", stagePath); err != nil {
		return fmt.Errorf("failed to upload Parquet file to Snowflake stage: %w", err)
	}

	return nil
}

func uploadToSnowflakeStage(snowflakeDSN, filePath, stagePath string) error {
	// Initialize Snowflake ADBC
	db, err := snowflake.NewDriver(memory.DefaultAllocator).NewDatabase(map[string]string{
		adbc.OptionKeyURI: snowflakeDSN,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Open connection
	conn, err := db.Open(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create a statement to upload the file to the Snowflake stage
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Set the SQL query to upload the file
	query := fmt.Sprintf("PUT file://%s @%s", filePath, stagePath)
	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	// Execute the query
	_, err = stmt.ExecuteUpdate(context.Background())
	if err != nil {
		return err
	}

	fmt.Println("Parquet file successfully uploaded to Snowflake stage!")
	return nil
}
