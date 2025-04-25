use anyhow::Result;
use polars::prelude::*;
use polars::io::SerReader;
use rayon::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::Write;

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

/// Read a file into a Polars DataFrame dynamically based on format
pub fn read_dataframe(path: &Path, format: &InputFormat, delimiter: u8) -> Result<DataFrame> {
    match format {
        InputFormat::Csv | InputFormat::Txt => {
            Ok(CsvReader::from_path(path)?
                .has_header(true)
                .with_delimiter(delimiter)
                .finish()?)
        }
        InputFormat::Json => {
            let file = std::fs::File::open(path)?;
            let reader = JsonReader::new(file);
            Ok(reader.finish()?)
        }
        InputFormat::Parquet => {
            Ok(ParquetReader::new(std::fs::File::open(path)?).finish()?)
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

/// Detailed profiling information for a single column
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnProfileDetailed {
    pub column: String,
    pub dtype: String,
    pub nulls: usize,
    pub unique: Option<usize>,
    pub min: Option<String>,
    pub max: Option<String>,
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

/// Performs detailed profiling on a DataFrame
pub fn profile_df_detailed(df: &DataFrame) -> Result<(usize, Vec<ColumnProfileDetailed>)> {
    let row_count = df.height();
    let profiles: Vec<ColumnProfileDetailed> = df
        .get_columns()
        .par_iter()
        .map(|s| {
            let dtype = format!("{:?}", s.dtype());
            let nulls = s.null_count();
            let unique = s.n_unique().ok();

            let (min, max) = match s.dtype() {
                DataType::Int64 => (
                    s.min::<i64>().map(|v| v.to_string()),
                    s.max::<i64>().map(|v| v.to_string()),
                ),
                DataType::Float64 => (
                    s.min::<f64>().map(|v| v.to_string()),
                    s.max::<f64>().map(|v| v.to_string()),
                ),
                DataType::Utf8 => (None, None),
                _ => (None, None),
            };

            let sample_values = s.unique().ok().map(|u| {
                u.head(Some(3))
                    .iter()
                    .filter_map(|v| v.to_string().into())
                    .collect::<Vec<_>>()
            });

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

/// Write detailed profile to JSON
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