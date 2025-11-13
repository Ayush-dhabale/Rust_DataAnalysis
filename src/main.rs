use polars :: prelude ::*;
mod config;

fn main() {


    // // Reading the CSV file
    // let df = config :: read_csv(
    //     "customers.csv".to_string()
    // );


    // // //Display the df
    // println!("{}",df.head(Some(4)));

    // //Display the columns
    // println!("Column name: {:?}", df.get_column_names());

    //Select a particular column
    //println!("{:?}", df.column("Last Name"));

    // Select mutiple columns
    //println!("{:?}", df.columns(["First Name", "Last Name"]))

    // Select using select
    //println!("{:?}",df.select(["Email"]));

    //Fetching data from yellow bricks
    config :: fetch_yellowbrick_data();

}
