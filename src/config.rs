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
    // Print headers
    if let Some(first_row) = result.get(0) {
        let headers: Vec<&str> = first_row.columns().iter().map(|c| c.name()).collect();
        println!("{:?}", headers); // or format as you like
    }

    for row in &result {
        let mut row_values = Vec::new();
        for col_idx in 0..row.len() {
            let val: Result<String, _> = row.try_get(col_idx);
            match val {
                Ok(v) => row_values.push(v),
                Err(_) => row_values.push(String::from("<unprintable>")),
            }
        }
        println!("{:?}", row_values); // or format as you like
    }
    //println!("{:?}",temp);
}