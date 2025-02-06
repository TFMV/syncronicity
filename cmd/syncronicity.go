package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docopt/docopt-go"
	"go.uber.org/zap"

	bigquery "github.com/TFMV/syncronicity/pkg/bigquery"
	snowflake "github.com/TFMV/syncronicity/pkg/snowflake"
)

const usage = `
synchronicity: BigQuery to Snowflake Arrow Data Transfer

Usage:
  synchronicity [--project=<project>] [--dataset=<dataset>] [--table=<table>] [--warehouse=<warehouse>] [--schema=<schema>] [--db=<db>]
  synchronicity -h | --help

Options:
  --project=<project>      GCP Project ID (overrides config.yaml)
  --dataset=<dataset>      BigQuery Dataset Name (overrides config.yaml)
  --service_account=<path> Path to service account JSON file (overrides config.yaml)
  --table=<table>          BigQuery Table Name (overrides config.yaml)
  --warehouse=<warehouse>  Snowflake Warehouse Name (overrides config.yaml)
  --schema=<schema>        Snowflake Schema Name (overrides config.yaml)
  --db=<db>                Snowflake Database Name (overrides config.yaml)
  --config=<config>        Path to config.yaml (overrides all other flags)
  --verbose                Enable verbose logging
  -h --help               Show this screen.
`

func main() {
	// Defer a function to catch panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "💥 Panic caught: %v\n", r)
			os.Exit(1)
		}
	}()

	fmt.Println("Initializing logger...") // Debugging statement

	// Initialize structured logging with zap
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	fmt.Println("Logger initialized successfully.") // Debugging statement

	// Parse CLI arguments
	args, err := docopt.ParseArgs(usage, os.Args[1:], "1.0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing arguments: %v\n", err)
		os.Exit(1)
	}

	// Override config values with CLI flags (if provided)
	project := getCLIOrConfig(args, "--project", "tfmv-371720")
	dataset := getCLIOrConfig(args, "--dataset", "tfmv")
	table := getCLIOrConfig(args, "--table", "foo")
	snowflakeDSN := getCLIOrConfig(args, "--snowflake_dsn", "tfmv:notapassword@YP29273.us-central1.gcp/tfmv/public")

	// Setup Service Account for BigQuery
	pathToServiceAccount := getCLIOrConfig(args, "--service_account", "/Users/thomasmcgeehan/syncronicity/syncronicity/sa.json")
	if pathToServiceAccount != "" {
		err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", strings.TrimSpace(pathToServiceAccount))
		if err != nil {
			sugar.Errorf("Failed to set GOOGLE_APPLICATION_CREDENTIALS: %v", err)
			os.Exit(1)
		}

		// Read and print the contents of sa.json
		contents, err := os.ReadFile(pathToServiceAccount)
		if err != nil {
			sugar.Errorf("Failed to read service account file: %v", err)
			os.Exit(1)
		}
		sugar.Infof("Service Account JSON: %s", string(contents))
	}

	zap.L().Info("Starting Synchronicity",
		zap.String("project", project),
		zap.String("dataset", dataset),
		zap.String("table", table),
		zap.String("snowflake_dsn", snowflakeDSN))

	// Context with timeout for the operation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	fmt.Println("Initializing BigQuery Reader...") // Debugging statement
	bqClient, err := bigquery.NewBigQueryReadClient(ctx)
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery client: %v", err)
	}

	fmt.Println("BigQuery Reader initialized successfully.") // Debugging statement

	reader, err := bqClient.NewBigQueryReader(ctx, project, dataset, table, &bigquery.BigQueryReaderOptions{
		MaxStreamCount: 1,
	})
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()

	fmt.Println("BigQuery Reader closed successfully.") // Debugging statement

	// Write Arrow RecordBatch to Parquet file
	record, err := reader.Read()

	fmt.Println("Arrow RecordBatch read successfully.") // Debugging statement
	if err != nil {
		sugar.Errorf("Error reading Arrow RecordBatch: %v", err)
	}
	defer record.Release()

	fmt.Println("Arrow RecordBatch released successfully.") // Debugging statement

	err = snowflake.ArrowToParquetStage(snowflakeDSN, record, "synchronicity_stage")
	if err != nil {
		sugar.Errorf("Error writing Arrow RecordBatch to Parquet file: %v", err)
	}

	fmt.Println("Arrow RecordBatch written to Parquet file successfully.") // Debugging statement

	// Send Arrow RecordBatch to Snowflake
	err = snowflake.LoadArrowIntoSnowflake(snowflakeDSN)
	if err != nil {
		sugar.Errorf("Error loading record into Snowflake: %v", err)
	}

	fmt.Println("Arrow RecordBatch loaded into Snowflake successfully.") // Debugging statement

	sugar.Infof("Data transfer complete!")
}

// getCLIOrConfig retrieves a value from CLI arguments or falls back to config.
func getCLIOrConfig(args map[string]interface{}, flag string, configValue string) string {
	if args[flag] != nil {
		return args[flag].(string)
	}
	return configValue
}
