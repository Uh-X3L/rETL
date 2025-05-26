use anyhow::Result;
use log::{error, info};
use polars::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, LazyJsonLineReader};

/// Extracts a CSV file using Polars' lazy API.
pub fn extract_csv_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting CSV from path: {}", path);
    LazyCsvReader::new(path)
        .with_has_header(true)
        .finish()
        .inspect(|_| {
            info!("Successfully loaded CSV file: {}", path);
        })
        .map_err(|e| {
            error!("Failed to load CSV file {}: {}", path, e);
            e.into()
        })
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

    reader
        .finish()
        .inspect(|_| {
            info!("Successfully loaded text file: {}", path);
        })
        .map_err(|e| {
            error!("Failed to load text file {}: {}", path, e);
            e.into()
        })
}

/// Extracts a JSON file using Polars' lazy API.
pub fn extract_json_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting JSON from path: {}", path);
    LazyJsonLineReader::new(path)
        .finish()
        .inspect(|_| {
            info!("Successfully loaded JSON file: {}", path);
        })
        .map_err(|e| {
            error!("Failed to load JSON file {}: {}", path, e);
            e.into()
        })
}

/// Extracts a Parquet file using Polars' lazy API.
pub fn extract_parquet_lazy(path: &str) -> Result<LazyFrame> {
    info!("Extracting Parquet from path: {}", path);
    LazyFrame::scan_parquet(path, Default::default())
        .inspect(|_| {
            info!("Successfully loaded Parquet file: {}", path);
        })
        .map_err(|e| {
            error!("Failed to load Parquet file {}: {}", path, e);
            e.into()
        })
}

/// Initializes the logger. Call this at the start of your application or tests.
pub fn init_logging() {
    use flexi_logger::{Logger, Duplicate, Age, Cleanup, Criterion, Naming, FileSpec};
    Logger::try_with_env()
        .unwrap()
        .log_to_file(FileSpec::default().directory("logs").basename("extract").suppress_timestamp())
        .duplicate_to_stdout(Duplicate::Info)
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .start()
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;
    use std::time::Duration;
    use std::sync::Once;

    static INIT: Once = Once::new();
    fn init_logging_once() {
        INIT.call_once(|| {
            let _ = std::panic::catch_unwind(|| init_logging());
        });
    }

    #[test]
    fn test_logging_creates_log_file() {
        let _ = fs::remove_dir_all("logs");
        init_logging_once();
        log::info!("Test log message for file logging");
        log::logger().flush();
        thread::sleep(Duration::from_millis(300));
        let log_dir = fs::read_dir("logs").expect("logs dir should exist");
        let mut found = false;
        for entry in log_dir {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().map(|e| e == "log").unwrap_or(false) {
                let contents = fs::read_to_string(&path).unwrap();
                if contents.contains("Test log message for file logging") {
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "Log file with expected message should be created");
    }

    #[test]
    fn test_extract_csv_lazy() {
        init_logging_once();
        let path = "data/examples/sample.csv";
        let result = extract_csv_lazy(path);
        assert!(
            result.is_ok(),
            "extract_csv_lazy failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_extract_text_lazy() {
        init_logging_once();
        let path = "data/examples/sample.json";
        let result = extract_text_lazy(path, b',', false, None, None, 0, None);
        assert!(
            result.is_ok(),
            "extract_text_lazy failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_extract_json_lazy() {
        init_logging_once();
        let path = "data/examples/sample.json";
        let result = extract_json_lazy(path);
        assert!(
            result.is_ok(),
            "extract_json_lazy failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_extract_parquet_lazy() {
        init_logging_once();
        let path = "data/examples/sample.parquet";
        let result = extract_parquet_lazy(path);
        assert!(
            result.is_ok(),
            "extract_parquet_lazy failed: {:?}",
            result.err()
        );
    }
}
