package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docopt/docopt-go"
	"go.uber.org/zap"

	"github.com/TFMV/syncronicity/internal/config"
	"github.com/TFMV/syncronicity/pkg/bigquery" // Assume this package exists and is similarly designed.
	"github.com/TFMV/syncronicity/pkg/snowflake"
)

const usage = `Synchronicity: BigQuery to Snowflake Arrow Data Transfer

Usage:
  synchronicity [--project=<project>] [--dataset=<dataset>] [--table=<table>] [--service_account=<path>] [--snowflake_dsn=<dsn>] [--config=<config>]
  synchronicity -h | --help

Options:
  --project=<project>         GCP Project ID (overrides config)
  --dataset=<dataset>         BigQuery Dataset Name (overrides config)
  --table=<table>             BigQuery Table Name (overrides config)
  --service_account=<path>    Path to service account JSON file (overrides config)
  --snowflake_dsn=<dsn>       Snowflake DSN (overrides config)
  --config=<config>           Path to config.yaml
  -h --help                   Show this screen.
`

func main() {
	// Initialize structured logging.
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// Parse CLI arguments using docopt.
	args, err := docopt.ParseArgs(usage, os.Args[1:], "1.0")
	if err != nil {
		sugar.Fatalf("Error parsing arguments: %v", err)
	}

	// Extract CLI argument values.
	configPath, _ := args.String("--config")
	cliProject, _ := args.String("--project")
	cliDataset, _ := args.String("--dataset")
	cliTable, _ := args.String("--table")
	cliServiceAccount, _ := args.String("--service_account")
	cliSnowflakeDSN, _ := args.String("--snowflake_dsn")

	// Load configuration from file.
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		sugar.Fatalf("Failed to load configuration: %v", err)
	}

	// Merge CLI overrides with config file values.
	project := mergeConfig(cliProject, cfg.GetString("project_id"))
	dataset := mergeConfig(cliDataset, cfg.GetString("dataset"))
	table := mergeConfig(cliTable, cfg.GetString("table"))
	serviceAccount := mergeConfig(cliServiceAccount, cfg.GetString("service_account"))
	snowflakeDSN := mergeConfig(cliSnowflakeDSN, cfg.GetString("snowflake_dsn"))
	stagePath := cfg.GetString("snowflake_stage")

	// Set the service account environment variable if provided.
	if serviceAccount != "" {
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", strings.TrimSpace(serviceAccount)); err != nil {
			sugar.Fatalf("Failed to set GOOGLE_APPLICATION_CREDENTIALS: %v", err)
		}
		sugar.Infof("Service account set from: %s", serviceAccount)
	}

	logger.Info("Starting Synchronicity",
		zap.String("project", project),
		zap.String("dataset", dataset),
		zap.String("table", table),
		zap.String("snowflake_dsn", snowflakeDSN))

	// Create a context with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Initialize the BigQuery client.
	bqClient, err := bigquery.NewBigQueryReadClient(ctx)
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Create a reader for the specified BigQuery table.
	reader, err := bqClient.NewBigQueryReader(ctx, project, dataset, table, &bigquery.BigQueryReaderOptions{
		MaxStreamCount: 1,
	})
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()

	// Read the Arrow record from BigQuery.
	record, err := reader.Read()
	if err != nil {
		sugar.Fatalf("Error reading Arrow record from BigQuery: %v", err)
	}
	defer record.Release()
	logger.Info("Arrow record read from BigQuery", zap.Int("numRows", int(record.NumRows())))

	// Initialize the Snowflake client.
	sfClient := snowflake.NewClient(snowflakeDSN, logger)

	// Ensure data directory exists
	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		sugar.Fatalf("Failed to create data directory: %v", err)
	}

	// Update the parquet file path to use the data directory
	parquetFile := filepath.Join(dataDir, "arrow_record.parquet")

	// Write the Arrow record to a Parquet file and upload it to the Snowflake stage.
	if err := sfClient.ArrowToParquetStage(ctx, record, parquetFile, stagePath); err != nil {
		sugar.Fatalf("Error processing Arrow record for Snowflake stage: %v", err)
	}

	// Load the data into Snowflake using a COPY command.
	if err := sfClient.LoadArrowIntoSnowflake(ctx); err != nil {
		sugar.Fatalf("Error loading data into Snowflake: %v", err)
	}

	sugar.Infof("Data transfer complete!")
}

// mergeConfig returns the CLI value if provided; otherwise, it falls back to the config value.
func mergeConfig(cliValue, configValue string) string {
	if cliValue != "" {
		return cliValue
	}
	return configValue
}
