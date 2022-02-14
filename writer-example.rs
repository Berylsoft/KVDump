use std::fs::OpenOptions;
use kvdump::*;

fn main() {
    let file = OpenOptions::new().write(true).create(true).open("./test").unwrap();
    let mut writer = Writer::new(file, Config { tag: "livekit-feed-raw".to_owned(), zone_len: 4, key_len: 8 }).unwrap();
    writer.push(
        Some(&u32::to_be_bytes(24393)),
        &u64::to_be_bytes(1644829883102),
        &[0x00, 0x01, 0x02],
    ).unwrap();
}
