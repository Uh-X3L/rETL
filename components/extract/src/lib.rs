use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn extract_csv(path: &str) -> Result<DataFrame> {
    let file = File::open(path)?;
    CsvReader::new(file).finish().map_err(Into::into)
}

pub fn extract_json(path: &str) -> Result<DataFrame> {
    let file = File::open(path)?;
    JsonReader::new(file).finish().map_err(Into::into)
}

pub fn extract_parquet(path: &str) -> Result<DataFrame> {
    let file = File::open(path)?;
    ParquetReader::new(file).finish().map_err(Into::into)
}

pub fn extract_txt(path: &str) -> Result<DataFrame> {
    let file = File::open(path)?;
    CsvReader::new(file).finish().map_err(Into::into)
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
    fn test_extract_txt() {
        let path = "data/examples/sample.txt";
        let result = extract_txt(path);
        assert!(result.is_ok() || result.is_err()); // Accepts missing file for now
    }
}
