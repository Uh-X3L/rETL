mod cli;

use clap::Parser;
use cli::Cli;
use polars::prelude::*;
use anyhow::Result;
use extract::*;
use conform::*;
use transform::*;
use load::{load_csv, load_parquet, load_json};

fn main() -> Result<()> {
    let args = Cli::parse();
    // Extract
    let mut df = match &format!("{:?}", args.input.format).to_lowercase()[..] {
        "csv" => extract_csv(args.input.file.to_str().unwrap())?,
        "json" => extract_json(args.input.file.to_str().unwrap())?,
        "parquet" => extract_parquet(args.input.file.to_str().unwrap())?,
        "txt" => extract_txt(args.input.file.to_str().unwrap())?,
        _ => return Err(anyhow::anyhow!("Unsupported input format: {:?}", args.input.format)),
    };

    // Conform (profiling, normalization, etc.)
    // (Profiling output and schema normalization options are not implemented in CLI)

    // Transform
    if args.transform.drop_nulls {
        df = df.drop_nulls::<String>(None)?;
    }
    if let (Some(col), Some(val)) = (args.transform.filter_col.as_deref(), args.transform.filter_val.as_deref()) {
        // If you want to use filter_by_value, implement or import it, otherwise comment out or use a simple filter
        // df = filter_by_value(df, col, val)?;
    }
    // Column selection and row limiting are not implemented in CLI

    // Load
    match &format!("{:?}", args.output.out_format).to_lowercase()[..] {
        "csv" => load_csv(&df, args.output.output.as_ref().map(|p| p.to_str().unwrap()).unwrap_or("output.csv"))?,
        "parquet" => load_parquet(&df, args.output.output.as_ref().map(|p| p.to_str().unwrap()).unwrap_or("output.parquet"))?,
        "json" => load_json(&df, args.output.output.as_ref().map(|p| p.to_str().unwrap()).unwrap_or("output.json"))?,
        _ => return Err(anyhow::anyhow!("Unsupported output format: {:?}", args.output.out_format)),
    }
    Ok(())
}
