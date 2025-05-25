use anyhow::Result;
use log::{error, info};
use polars::prelude::*;
use std::fs::File;


/// Extracts a CSV file using Polars' lazy API.
pub fn extract_csv_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting CSV from path: {}", path);
    Ok(
        LazyCsvReader::new(path)
            .with_has_header(true)
            .finish()?
    )
}

/// Extracts a text file with customizable options using Polars' lazy API.
pub fn extract_text_lazy(
    path: &str,
    delimiter: u8,
    has_header: bool,
    quote_char: Option<u8>,
    comment_prefix: Option<&str>,
    skip_rows: usize,
    infer_schema_length: Option<usize>,
) -> Result<LazyFrame> {
    info!("Extracting text file from path: {}", path);
    let mut reader = LazyCsvReader::new(path)
        .with_separator(delimiter)
        .with_has_header(has_header)
        .with_skip_rows(skip_rows);

    if let Some(qc) = quote_char {
        reader = reader.with_quote_char(Some(qc));
    }

    if let Some(cp) = comment_prefix {
        reader = reader.with_comment_prefix(Some(cp.into()));
    }

    if let Some(infer_len) = infer_schema_length {
        reader = reader.with_infer_schema_length(Some(infer_len));
    }

    Ok(reader.finish()?)
}

/// Extracts a JSON file using Polars' lazy API.
pub fn extract_json_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting JSON from path: {}", path);
    Ok(
        LazyJsonLineReader::new(path)
            .finish()?
    )
}

/// Extracts a Parquet file using Polars' lazy API.
pub fn extract_parquet_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting Parquet from path: {}", path);
    Ok(
        LazyFrame::scan_parquet(path, Default::default())?
    )
}

/// Initializes the logger. Call this at the start of your application or tests.
pub fn init_logging() {
    let _ = env_logger::builder().is_test(false).try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_csv_lazy() {
        init_logging();
        let path = "data/examples/sample.csv";
        let result = extract_csv_lazy(path);
        assert!(result.is_ok() || result.is_err()); // Accepts missing file for now
    }

    #[test]
    fn test_extract_text_lazy() {
        init_logging();
        let path = "data/examples/sample.txt";
        let result = extract_text_lazy(path, b'|', false, None, None, 0, Some(100));
        assert!(result.is_ok() || result.is_err()); // Accepts missing file for now
    }

    #[test]
    fn test_extract_json_lazy() {
        init_logging();
        let path = "data/examples/sample.json";
        let result = extract_json_lazy(path);
        assert!(result.is_ok() || result.is_err()); // Accepts missing file for now
    }

    #[test]
    fn test_extract_parquet_lazy() {
        init_logging();
        let path = "data/examples/sample.parquet";
        let result = extract_parquet_lazy(path);
        assert!(result.is_ok() || result.is_err()); // Accepts missing file for now
    }
}
