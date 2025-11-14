use polars::prelude::*;
use polars::error::PolarsError;
use polars::frame::DataFrame;
use postgres::{Client, NoTls};
use std::collections::HashSet;

pub fn read_csv(path : String) -> DataFrame{
    let df = CsvReadOptions :: default()
        .try_into_reader_with_file_path(Some(path.into()))
        .unwrap()
        .finish()
        .unwrap();

    return df;

}

pub fn fetch_yellowbrick_data(){
    let host = "ybr-06";
    let user = "adhabale";
    let password = "$ymphony@20017553";
    let db_name = "dbo";
    let port = 5432;
    let query = "select * FROM ffsrouprd.pro_model_upcstoredate_cube a
        where a.promoweek_num  = 202543
        limit 1;";

    let connection_string = format!(
        "host={} user={} password={} dbname={} port={}",
        host,user,password,db_name,port
    );


    let mut client = Client::connect(&connection_string, NoTls).expect("Connection failed!");
    let result = client.query(query,&[]).unwrap();

    for row in &result {
        for (col_idx, column) in row.columns().iter().enumerate() {
            let col_name = column.name();

            // Try known types in Option<T> form
            if let Ok(Some(v)) = row.try_get::<usize, Option<String>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(Some(v)) = row.try_get::<usize, Option<i32>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(Some(v)) = row.try_get::<usize, Option<i64>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(Some(v)) = row.try_get::<usize, Option<f64>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(Some(v)) = row.try_get::<usize, Option<bool>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(Some(v)) = row.try_get::<usize, Option<chrono::NaiveDate>>(col_idx) {
                println!("{}: {}", col_name, v);
            } else if let Ok(None) = row.try_get::<usize, Option<String>>(col_idx) {
                println!("{}: Null", col_name);
            } else if let Ok(None) = row.try_get::<usize, Option<i32>>(col_idx) {
                println!("{}: Null", col_name);
            } else if let Ok(None) = row.try_get::<usize, Option<i64>>(col_idx) {
                println!("{}: Null", col_name);
            } else if let Ok(None) = row.try_get::<usize, Option<f64>>(col_idx) {
                println!("{}: Null", col_name);
            } else if let Ok(None) = row.try_get::<usize, Option<chrono::NaiveDate>>(col_idx) {
                println!("{}: Null", col_name);
            } else {
                println!("{}: <unprintable>", col_name);
            }
        }
    println!("-----------------------------");
    }
}

pub fn concatenate_columns(df: &DataFrame, columns: &[&str]) -> Result<String, PolarsError> {
    if columns.is_empty() {
        return Err(PolarsError::ComputeError("No columns specified".into()));
    }

    let series_vec: Vec<&Series> = columns.iter().map(|&col| df.column(col)).collect::<Result<_, _>>()?;

    // Ensure all series have the same length
    let len = series_vec[0].len();
    if !series_vec.iter().all(|s| s.len() == len) {
        return Err(PolarsError::ShapeMismatch("Columns have different lengths".into()));
    }
    let mut unique_values: HashSet<String> = HashSet::new();

    for i in 0..len {
        let concatenated = series_vec.iter()
            .map(|s| s.get(i).map(|v| v.to_string().trim_matches('"').to_string()).unwrap_or_default())
            .collect::<Vec<String>>()
            .join("|");

        unique_values.insert(concatenated);
    }

    let result = format!("({})", unique_values.into_iter()
        .map(|id| format!("'{}'", id))
        .collect::<Vec<_>>()
        .join(", "));

    Ok(result)
}