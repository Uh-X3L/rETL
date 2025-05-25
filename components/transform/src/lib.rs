use anyhow::Result;
use polars::prelude::*;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn select_columns(mut df: DataFrame, columns: &Vec<String>) -> Result<DataFrame> {
    let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
    df = df.select(&cols)?;
    Ok(df)
}

pub fn limit_rows(df: DataFrame, limit: usize) -> Result<DataFrame> {
    Ok(df.head(Some(limit)))
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
    fn test_select_columns_and_limit_rows() {
        use polars::prelude::*;
        let df = df! {"a" => &[1,2,3], "b" => &[4,5,6]}.unwrap();
        let cols = vec!["a".to_string()];
        let df2 = select_columns(df.clone(), &cols).unwrap();
        assert_eq!(df2.get_column_names(), vec!["a"]);
        let df3 = limit_rows(df, 2).unwrap();
        assert_eq!(df3.height(), 2);
    }
}
