use std::fs::OpenOptions;
use kvdump::*;

fn main() {
    let file = OpenOptions::new().read(true).open("./test").unwrap();
    let (mut reader, config) = Reader::new(file).unwrap();
    println!("{:?}", config);
    while let Some(pair) = reader.next() {
        println!("{:?}", pair.unwrap());
    }
}
