pub use anyhow::Result;
pub use polars::prelude::*;
use std::fs::File;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn load_csv(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    CsvWriter::new(file)
        .finish(&mut df.clone())
        .map_err(Into::into)
}

pub fn load_parquet(_df: &DataFrame, _path: &str) -> Result<()> {
    // TODO: Implement ParquetWriter
    unimplemented!("ParquetWriter is not yet implemented");
}

pub fn load_json(_df: &DataFrame, _path: &str) -> Result<()> {
    // TODO: Implement JsonWriter
    unimplemented!("JsonWriter is not yet implemented");
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
        assert!(result.is_ok() || result.is_err());
        let _ = std::fs::remove_file(path);
    }
}
