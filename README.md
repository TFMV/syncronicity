# syncronicity

BigQuery to Snowflake Migration Tool

## Overview

syncronicity is a tool that migrates data from BigQuery to Snowflake using Arrow format.

## Features

✅ Apache Arrow-Powered – Optimized for columnar data transfer.
✅ BigQuery Storage API – Streams data efficiently to Parquet files.
✅ Snowflake COPY Command – Ingest parquet files into Snowflake.
✅ Hot-Reloading Config – No need to restart on config changes.
✅ CLI + Config Flexibility – Use config.yaml or override via CLI.

## Usage

Please note that you'll need 2FA enabled for your Snowflake account and will need to authorize twice in its current form.

```bash
syncronicity --config config.yaml --project tfmv-371720 --dataset tfmv --table foo --service_account path/to/service_account.json --snowflake_dsn tfmv:notapassword@YP29273.us-central1.gcp/tfmv/public
```

Or use the config file:

```yaml
project_id: "tfmv-371720"
dataset: "tfmv"
table: "foo"
snowflake_dsn: "tfmv:notapassword@YP29273.us-central1.gcp.snowflakecomputing.com/tfmv/public"
snowflake_stage: "SYNCHRONICITY_STAGE"
```

All fields are required.
