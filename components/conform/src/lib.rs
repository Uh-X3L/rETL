use anyhow::Result;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;

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
pub struct ColumnProfileDetailed {
    pub column: String,
    pub dtype: String,
    pub nulls: usize,
    pub unique: Option<usize>,
    pub min: MinMaxValue,
    pub max: MinMaxValue,
    pub sample_values: Option<Vec<String>>,
}

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

pub fn profile_df_detailed(df: &DataFrame) -> Result<(usize, Vec<ColumnProfileDetailed>)> {
    let row_count = df.height();
    let profiles: Vec<ColumnProfileDetailed> = df
        .get_columns()
        .par_iter()
        .map(|col| {
            let s = col.as_series().expect("Expected a Series from Column");
            let dtype_obj = s.dtype();
            let dtype = format!("{:?}", dtype_obj);
            let nulls = s.null_count();
            let unique = s.n_unique().ok();
            let (min, max) = match dtype_obj {
                DataType::Int64 | DataType::Int32 => s
                    .i64()
                    .map(|ca| {
                        (
                            ca.min().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                            ca.max().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                        )
                    })
                    .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
                DataType::Float64 | DataType::Float32 => s
                    .f64()
                    .map(|ca| {
                        (
                            ca.min()
                                .map(MinMaxValue::Float)
                                .unwrap_or(MinMaxValue::None),
                            ca.max()
                                .map(MinMaxValue::Float)
                                .unwrap_or(MinMaxValue::None),
                        )
                    })
                    .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
                DataType::Boolean => s
                    .bool()
                    .map(|ca| {
                        (
                            ca.min()
                                .map(|v| MinMaxValue::Int(v as i64))
                                .unwrap_or(MinMaxValue::None),
                            ca.max()
                                .map(|v| MinMaxValue::Int(v as i64))
                                .unwrap_or(MinMaxValue::None),
                        )
                    })
                    .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
                DataType::Date => s
                    .i32()
                    .map(|ca| {
                        (
                            ca.min()
                                .map(|v| MinMaxValue::Int(v as i64))
                                .unwrap_or(MinMaxValue::None),
                            ca.max()
                                .map(|v| MinMaxValue::Int(v as i64))
                                .unwrap_or(MinMaxValue::None),
                        )
                    })
                    .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
                DataType::Datetime(_, _) => s
                    .i64()
                    .map(|ca| {
                        (
                            ca.min().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                            ca.max().map(MinMaxValue::Int).unwrap_or(MinMaxValue::None),
                        )
                    })
                    .unwrap_or((MinMaxValue::None, MinMaxValue::None)),
                DataType::String => (MinMaxValue::None, MinMaxValue::None),
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
                        .collect::<Vec<_>>(),
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

pub fn export_profile_to_json(profiles: &[ColumnProfileDetailed], path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(profiles)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}
