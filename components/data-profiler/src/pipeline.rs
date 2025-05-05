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
