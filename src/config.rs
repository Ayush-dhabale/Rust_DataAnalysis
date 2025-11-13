use polars::prelude::*;
use polars::error::PolarsError;
use polars::frame::DataFrame;
use postgres::{Client, NoTls};

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