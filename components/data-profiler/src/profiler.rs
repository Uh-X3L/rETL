use polars::prelude::*;           // DataFrame, Series, ChunkedArray, DataType, CsvReader, JsonReader, ParquetReader
use polars::prelude::CsvReadOptions;
use polars::io::SerReader;       // brings `.finish()` into scope
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use anyhow::{Result, Context};
use polars::datatypes::DataType;
use std::io::Write; 
use serde::{Serialize, Deserialize};

/// Supported input file formats
#[derive(Debug, Clone)]
pub enum InputFormat {
    Csv,
    Json,
    Parquet,
    Txt,
    Unknown,
}

/// Infer format from file extension
pub fn infer_format(path: &Path) -> InputFormat {
    match path.extension().and_then(|s| s.to_str()) {
        Some("csv") => InputFormat::Csv,
        Some("json") => InputFormat::Json,
        Some("parquet") => InputFormat::Parquet,
        Some("txt") => InputFormat::Txt,
        _ => InputFormat::Unknown,
    }
}

pub fn read_dataframe(path: &Path, format: &InputFormat, delimiter: u8) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path.display()))?;

    match format {
        InputFormat::Csv | InputFormat::Txt => {
            let options = CsvReadOptions::default()
                .with_has_header(true)
                .map_parse_options(|opts| opts.with_separator(delimiter));
            let reader = options.into_reader_with_file_handle(file);
            reader
                .finish()
                .context("Failed to read CSV/TXT file")
        }
        InputFormat::Json => {
            JsonReader::new(file)
                .finish()
                .context("Failed to read JSON file")
        }
        InputFormat::Parquet => {
            ParquetReader::new(file)
                .finish()
                .context("Failed to read Parquet file")
        }
        InputFormat::Unknown => Err(anyhow::anyhow!("Unsupported file format")),
    }
}

/// Profiling information for a single column
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Profile {
    pub column: String,
    pub dtype: String,
    pub nulls: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MinMaxValue {
    Int(i64),
    Float(f64),
    Str(String),
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Detailed profiling information for a single column
pub struct ColumnProfileDetailed {
    pub column: String,
    pub dtype: String,
    pub nulls: usize,
    pub unique: Option<usize>,
    pub min: MinMaxValue,
    pub max: MinMaxValue,
    pub sample_values: Option<Vec<String>>,
}


/// Profiles a DataFrame
#[allow(dead_code)]
pub fn profile_df(df: DataFrame) -> Result<(usize, Vec<Profile>)> {
    let row_count = df.height();
    let profiles = df
        .get_columns()
        .par_iter()
        .map(|s| Profile {
            column: s.name().to_string(),
            dtype: format!("{:?}", s.dtype()),
            nulls: s.null_count(),
        })
        .collect();
    Ok((row_count, profiles))
}

/// Profiles a DataFrame with detailed information
pub fn profile_df_detailed(df: &DataFrame) -> Result<(usize, Vec<ColumnProfileDetailed>)> {
    let row_count = df.height();
    let profiles: Vec<ColumnProfileDetailed> = df.get_columns()
    .par_iter()
    .map(|col| {
        let s = col.as_series().expect("Expected a Series from Column"); // <-- THIS fixes type mismatch
        
        let dtype_obj = s.dtype();
        let dtype = format!("{:?}", dtype_obj);
        let nulls = s.null_count();
        let unique = s.n_unique().ok();

        let (min, max) = match dtype_obj {
            DataType::Int64 | DataType::Int32 => s.i64()
                .map(|ca| (
                    ca.min().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                    ca.max().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                ))
                .unwrap_or((MinMaxValue::None, MinMaxValue::None)),

            DataType::Float64 | DataType::Float32 => s.f64()
                .map(|ca| (
                    ca.min().map(MinMaxValue::Float).unwrap_or(MinMaxValue::None),
                    ca.max().map(MinMaxValue::Float).unwrap_or(MinMaxValue::None),
                ))
                .unwrap_or((MinMaxValue::None, MinMaxValue::None)),

            DataType::Boolean => s.bool()
                .map(|ca| (
                    ca.min().map(|v| MinMaxValue::Int(v as i64)).unwrap_or(MinMaxValue::None),
                    ca.max().map(|v| MinMaxValue::Int(v as i64)).unwrap_or(MinMaxValue::None),
                ))
                .unwrap_or((MinMaxValue::None, MinMaxValue::None)),

            DataType::Date => s.i32()
                .map(|ca| (
                    ca.min().map(|v| MinMaxValue::Int(v as i64)).unwrap_or(MinMaxValue::None),
                    ca.max().map(|v| MinMaxValue::Int(v as i64)).unwrap_or(MinMaxValue::None),
                ))
                .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
        
            DataType::Datetime(_, _) => s.i64()
                .map(|ca| (
                    ca.min().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                    ca.max().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                ))
                .unwrap_or((MinMaxValue::None, MinMaxValue::None)),

            DataType::String => (MinMaxValue::None, MinMaxValue::None),  // skip min/max for string

            _ => (MinMaxValue::None, MinMaxValue::None),
        };

        let sample_values = if matches!(dtype_obj, DataType::String) {
            Some(
                (0..s.len())
                    .filter_map(|idx| s.get(idx).ok())
                    .filter_map(|val| match val {
                        polars::prelude::AnyValue::String(v) => Some(v.to_string()),
                        _ => None,
                    })
                    .take(3)
                    .collect::<Vec<_>>()
            )
        } else {
            None
        };

        ColumnProfileDetailed {
            column: s.name().to_string(),
            dtype,
            nulls,
            unique,
            min,
            max,
            sample_values,
        }
    })
    .collect();

    Ok((row_count, profiles))
}


/// Profile any supported format file
#[allow(dead_code)]
pub fn profile_any(path: &Path, delimiter: u8) -> Result<(usize, Vec<Profile>)> {
    let format = infer_format(path);
    let df = read_dataframe(path, &format, delimiter)?;
    profile_df(df)
}

pub fn export_profile_to_json(profiles: &[ColumnProfileDetailed], path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(profiles)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_profile_any_formats() {
        let csv_path = Path::new("components/data-profiler/data/examples/sample.csv");
        let json_path = Path::new("components/data-profiler/data/examples/sample.json");
        let parquet_path = Path::new("components/data-profiler/data/examples/sample.parquet");

        let (csv_rows, _) = profile_any(csv_path, b',').expect("CSV profiling failed");
        let (json_rows, _) = profile_any(json_path, b',').expect("JSON profiling failed");
        let (parquet_rows, _) = profile_any(parquet_path, b',').expect("Parquet profiling failed");

        println!("CSV: {csv_rows} rows, JSON: {json_rows} rows, Parquet: {parquet_rows} rows");

        assert!(csv_rows > 0);
        assert!(json_rows > 0);
        assert!(parquet_rows > 0);
    }
}


#[test]
fn test_profile_detailed() {
    let path = Path::new("components/data-profiler/data/examples/sample.csv");
    let df = read_dataframe(path, &infer_format(path), b',').expect("failed to read");
    let (_, profiles) = profile_df_detailed(&df).expect("detailed profiling failed");

    assert!(!profiles.is_empty());
    println!("{:#?}", profiles[0]);
}