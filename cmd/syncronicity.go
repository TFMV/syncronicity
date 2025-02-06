package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/docopt/docopt-go"
	"go.uber.org/zap"

	"github.com/TFMV/syncronicity/internal/config"
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
  --table=<table>          BigQuery Table Name (overrides config.yaml)
  --warehouse=<warehouse>  Snowflake Warehouse Name (overrides config.yaml)
  --schema=<schema>        Snowflake Schema Name (overrides config.yaml)
  --db=<db>                Snowflake Database Name (overrides config.yaml)
  -h --help               Show this screen.
`

func main() {
	// Parse CLI arguments
	args, err := docopt.ParseArgs(usage, os.Args[1:], "1.0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing arguments: %v\n", err)
		os.Exit(1)
	}

	// Initialize structured logging with zap
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Load config with hot-reloading support
	cfg, err := config.LoadConfigStruct()
	if err != nil {
		sugar.Fatalf("Failed to load configuration: %v", err)
	}

	// Override config values with CLI flags (if provided)
	project := getCLIOrConfig(args, "--project", cfg.ProjectID)
	dataset := getCLIOrConfig(args, "--dataset", cfg.Dataset)
	table := getCLIOrConfig(args, "--table", cfg.Table)
	snowflakeDSN := cfg.SnowflakeDSN // No CLI override for security

	sugar.Infof("Starting Synchronicity: BigQuery [%s.%s.%s] → Snowflake [%s]",
		project, dataset, table, snowflakeDSN)

	// Context with timeout for the operation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Step 1: Initialize BigQuery Reader
	bqClient, err := bigquery.NewBigQueryReadClient(ctx)
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery client: %v", err)
	}

	reader, err := bqClient.NewBigQueryReader(ctx, project, dataset, table, &bigquery.BigQueryReaderOptions{
		MaxStreamCount: 1,
	})
	if err != nil {
		sugar.Fatalf("Failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()

	// Step 2: Initialize Snowflake Loader
	sfClient, err := snowflake.NewSnowflakeClient(ctx, snowflakeDSN)
	if err != nil {
		sugar.Fatalf("Failed to connect to Snowflake: %v", err)
	}
	defer sfClient.Close()

	// Step 3: Transfer Data from BigQuery → Snowflake
	sugar.Info("Starting data transfer...")
	transferred := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break // No more data
			}
			sugar.Errorf("Error reading from BigQuery: %v", err)
			continue
		}

		writer := ipc.NewWriter(os.Stdout, nil)

		// Send Arrow RecordBatch to Snowflake
		err = snowflake.SendArrowToSnowflake(ctx, writer)
		if err != nil {
			sugar.Errorf("Error loading record into Snowflake: %v", err)
			continue
		}

		transferred += int(record.NumRows())
		record.Release() // Free memory after processing
	}

	sugar.Infof("Data transfer complete! Rows transferred: %d", transferred)
}

// getCLIOrConfig retrieves a value from CLI arguments or falls back to config.
func getCLIOrConfig(args map[string]interface{}, flag string, configValue string) string {
	if args[flag] != nil {
		return args[flag].(string)
	}
	return configValue
}
