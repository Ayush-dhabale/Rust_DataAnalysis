use polars :: prelude ::*;

fn main() {


    // Reading the CSV file
    let df = CsvReadOptions :: default()
    .try_into_reader_with_file_path(Some("customers.csv".into()))
    .unwrap()
    .finish()
    .unwrap();

    // //Display the df
    // println!("{}",df.head(Some(4)));

    // //Display the columns
    // println!("Column name: {:?}", df.get_column_names());

    //Select a particular column
    //println!("{:?}", df.column("Last Name"));

    // Select mutiple columns
    println!("{:?}", df.columns(["First Name", "Last Name"]))

}
