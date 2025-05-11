use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "etl-cli", version, author, about = "A simple Rust ETL pipeline using Polars")]
pub struct Cli {
    #[clap(flatten)]
    pub input: InputArgs,

    #[clap(flatten)]
    pub transform: TransformArgs,

    #[clap(flatten)]
    pub output: OutputArgs,
}

#[derive(Parser, Debug)]
pub struct InputArgs {
    #[arg(short, long, help = "Path to the input file")]
    pub file: PathBuf,

    #[arg(long, default_value = "csv", value_enum, help = "Input file format")]
    pub format: FileFormat,
}

#[derive(Parser, Debug)]
pub struct TransformArgs {
    #[arg(long, help = "Drop null values from the dataset")]
    pub drop_nulls: bool,

    #[arg(long, help = "Column to filter on (optional)")]
    pub filter_col: Option<String>,

    #[arg(long, help = "Value to filter for (optional)")]
    pub filter_val: Option<String>,
}

#[derive(Parser, Debug)]
pub struct OutputArgs {
    #[arg(long, help = "Path to save the cleaned output (optional)")]
    pub output: Option<PathBuf>,

    #[arg(long, default_value = "csv", value_enum, help = "Output file format")]
    pub out_format: FileFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}
