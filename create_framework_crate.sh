#!/usr/bin/env bash
# create_framework_crate.sh
# Script to scaffold a new Rust framework crate for orchestrating ETL metadata using a SQL database (Postgres or SQL Server)

set -e

CRATE_NAME="etl_framework"
DB="postgres" # Change to 'mssql' for SQL Server

# 1. Create the new crate
cargo new --lib $CRATE_NAME
cd $CRATE_NAME

# 2. Add dependencies for database access and orchestration
if [ "$DB" = "postgres" ]; then
  cargo add sqlx --features runtime-tokio,postgres
  cargo add tokio --features full
elif [ "$DB" = "mssql" ]; then
  cargo add sqlx --features runtime-tokio,mssql
  cargo add tokio --features full
fi

# 3. Add logging and configuration dependencies
cargo add log env_logger anyhow serde serde_json

# 4. Create a basic src/lib.rs template
cat > src/lib.rs <<'EOF'
//! ETL Framework: Orchestrates ETL metadata and execution using a SQL database.

use anyhow::Result;
use log::{info, error};
use sqlx::{Pool, Postgres}; // Change to Mssql for SQL Server
use tokio;

pub struct Framework {
    pool: Pool<Postgres>, // Change to Pool<Mssql> for SQL Server
}

impl Framework {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = Pool::connect(database_url).await?;
        info!("Connected to database");
        Ok(Self { pool })
    }

    /// Example: Fetch ETL metadata for extract layer
    pub async fn fetch_extract_metadata(&self) -> Result<()> {
        // TODO: Implement metadata fetch logic
        Ok(())
    }

    /// Example: Orchestrate extract execution
    pub async fn orchestrate_extract(&self) -> Result<()> {
        // TODO: Implement orchestration logic
        Ok(())
    }
}
EOF

# 5. Print next steps
cat <<EOM

Framework crate '$CRATE_NAME' created!
- Database: $DB
- Dependencies: sqlx, tokio, log, env_logger, anyhow, serde, serde_json

Next steps:
- Set your DATABASE_URL in a .env file or as an environment variable.
- Implement metadata and orchestration logic in src/lib.rs.
- Add Azure SQL or Postgres connection string as needed.
- Expand the framework to support other ETL layers.

EOM
