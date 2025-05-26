//pub mod config;: Makes the config module (with your DbConfig struct) available.
pub mod config;
//Imports: Brings in error handling (anyhow::Result), your config struct, and the necessary types from sqlx for SQLite connection pooling.
use anyhow::Result;
use config::DbConfig;
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, SqlitePool};
use std::str::FromStr;

//Purpose: Opens an async SQLite connection pool to the file at cfg.path.
pub async fn connect(cfg: &DbConfig) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(&cfg.path)? //Parses the path from your config into SQLite connection options.
        .create_if_missing(true); //database file is created if it doesn’t exist.
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?; //Creates a connection pool with up to 5 connections.
                 // Set PRAGMA foreign_keys = ON for every connection
    sqlx::query("PRAGMA foreign_keys = ON;") //to enforce foreign key constraints on every connection. Enabling this ensures that your database will respect foreign key relationships (e.g., prevent deleting a parent row if child rows exist), which is important for data integrity.
        .execute(&pool)
        .await?;
    Ok(pool) //Returns the pool or an error.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smoke_test_connect() {
        let json = r#"{ "path": ":memory:" }"#;
        let cfg: DbConfig = serde_json::from_str(json).unwrap();
        let pool = connect(&cfg).await.expect("Should connect to SQLite");
        // Optionally, check the pool is valid by running a simple query
        let row: (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.unwrap();
        assert_eq!(row.0, 1);
    }
}
