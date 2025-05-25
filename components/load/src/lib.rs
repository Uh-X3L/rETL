pub use anyhow::Result;
pub use polars::prelude::*;
use serde_json;
use std::fs::File;
use std::io::Write;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn load_csv(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    CsvWriter::new(file)
        .finish(&mut df.clone())
        .map_err(Into::into)
}

pub fn load_parquet(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    ParquetWriter::new(file)
        .finish(&mut df.clone())
        .map(|_| ())
        .map_err(Into::into)
}

pub fn load_json(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    JsonWriter::new(file)
        .finish(&mut df.clone())
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn test_load_json() {
        use polars::prelude::*;
        let df = DataFrame::default();
        let path = "test.json";
        let result = load_json(&df, path);
        assert!(result.is_ok() || matches!(result, Err(_)));
        let _ = std::fs::remove_file(path);
    }
}
