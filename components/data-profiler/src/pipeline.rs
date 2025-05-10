use polars::prelude::*;
use anyhow::Result;

pub fn load_data(path: &str, format: &str) -> Result<DataFrame> {
    let file = std::fs::File::open(path)?;
    match format {
        "csv" => CsvReader::new(file).finish().map_err(Into::into),
        "json" => JsonReader::new(file).finish().map_err(Into::into),
        "parquet" => ParquetReader::new(file).finish().map_err(Into::into),
        _ => Err(anyhow::anyhow!("Unsupported format: {}", format)),
    }
}

pub fn save_data(df: &DataFrame, path: &str, format: &str) -> Result<()> {
    let file = std::fs::File::create(path)?;
    match format {
        "csv" => CsvWriter::new(file).finish(&mut df.clone()).map_err(Into::into),
        "parquet" => ParquetWriter::new(file).finish(&mut df.clone()).map(|_| ()).map_err(Into::into),
        _ => Err(anyhow::anyhow!("Unsupported output format: {}", format)),
    }
}

pub fn run_pipeline(
    file: &str,
    format: &str,
    output: Option<&str>,
    out_format: &str,
    filter_col: Option<&str>,
    filter_val: Option<&str>,
    drop_nulls: bool,
) -> Result<()> {
    let mut df = load_data(file, format)?;

    if drop_nulls {
        df = df.drop_nulls::<String>(None)?;
    }

    if let (Some(col), Some(val)) = (filter_col, filter_val) {
        let series = df.column(col)?;
        let mask = match series.dtype() {
            DataType::String => series.str()?.equal(val),
            DataType::Int64 => {
                let val_parsed = val.parse::<i64>().unwrap_or_default();
                series.i64()?.equal(val_parsed)
            },
            DataType::Float64 => {
                let val_parsed = val.parse::<f64>().unwrap_or_default();
                series.f64()?.equal(val_parsed)
            },
            _ => return Err(anyhow::anyhow!("Unsupported filter type")),
        };
        df = df.filter(&mask)?;
    }

    if let Some(out_path) = output {
        save_data(&df, out_path, out_format)?;
        println!("✅ Saved cleaned data to {out_path}");
    } else {
        println!("{:?}", df.head(Some(5)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_load_data_csv() {
        let path = "data/examples/sample.csv";
        let df = load_data(path, "csv").expect("Should load CSV");
        assert_eq!(df.shape().0, 3); // 3 rows
        assert!(df.get_column_names().iter().any(|c| c.as_str() == "name"));
    }

    #[test]
    fn test_load_data_json() {
        let path = "data/examples/sample.json";
        let df = load_data(path, "json").expect("Should load JSON");
        assert_eq!(df.shape().0, 3); // 3 rows
        assert!(df.get_column_names().iter().any(|c| c.as_str() == "name"));
    }

    #[test]
    fn test_save_data_csv() {
        let path = "data/examples/sample.csv";
        let df = load_data(path, "csv").unwrap();
        let out_path = "data/examples/test_out.csv";
        save_data(&df, out_path, "csv").unwrap();
        assert!(Path::new(out_path).exists());
        fs::remove_file(out_path).unwrap();
    }

    #[test]
    fn test_save_data_parquet() {
        let path = "data/examples/sample.csv";
        let df = load_data(path, "csv").unwrap();
        let out_path = "data/examples/test_out.parquet";
        save_data(&df, out_path, "parquet").unwrap();
        assert!(Path::new(out_path).exists());
        fs::remove_file(out_path).unwrap();
    }

    #[test]
    fn test_run_pipeline_drop_nulls() {
        let out_path = "data/examples/test_clean.csv";
        run_pipeline(
            "data/examples/sample.csv",
            "csv",
            Some(out_path),
            "csv",
            None,
            None,
            true,
        ).unwrap();
        let df = load_data(out_path, "csv").unwrap();
        let null_counts = df.null_count();
        let all_zero = null_counts
            .get_columns()
            .iter()
            .all(|s| s.as_series().unwrap().sum::<u32>().unwrap_or(0) == 0);
        assert!(all_zero);
        fs::remove_file(out_path).unwrap();
    }

    #[test]
    fn test_run_pipeline_filter() {
        let out_path = "data/examples/test_filter.csv";
        run_pipeline(
            "data/examples/sample.csv",
            "csv",
            Some(out_path),
            "csv",
            Some("name"),
            Some("Alice"),
            false,
        ).unwrap();
        let df = load_data(out_path, "csv").unwrap();
        assert_eq!(df.shape().0, 1);
        assert_eq!(df.column("name").unwrap().str().unwrap().get(0).unwrap(), "Alice");
        fs::remove_file(out_path).unwrap();
    }
}
