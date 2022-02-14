use std::io::{self, Read, Write, Seek};

pub const BS_IDF: u32 = 0x42650000;

#[derive(Debug)]
pub struct Config {
    pub tag: String,
    pub zone_len: u32,
    pub key_len: u32,
}

pub struct Writer<F: Read + Write + Seek + Sized> {
    inner: F,
    zone_len: u32,
    key_len: u32,
}

fn check_tag(_tag: &str) -> bool {
    true
}

macro_rules! write_u32 {
    ($f:expr, $num:expr) => {
        $f.write(&u32::to_be_bytes($num))?;
    }
}

macro_rules! write_bytes {
    ($f:expr, $bytes:expr) => {
        $f.write($bytes)?;
    }
}

macro_rules! read_u32 {
    ($f:expr) => {{
        let mut buf = [0u8; 4];
        let result = $f.read(&mut buf).unwrap();
        if result == 4 {
            Some(u32::from_be_bytes(buf))
        } else {
            None
        }
    }};
}

macro_rules! read_bytes {
    ($f:expr, $len:expr) => {{
        let len_sz = $len.try_into().unwrap();
        let mut buf = vec![0u8; len_sz];
        let result = $f.read(&mut buf).unwrap();
        if result == len_sz {
            Some(buf.to_vec())
        } else {
            None
        }
    }};
}

impl<F: Read + Write + Seek + Sized> Writer<F> {
    pub fn new(mut inner: F, config: Config) -> io::Result<Writer<F>> {
        let Config { tag, zone_len, key_len } = config;

        assert_ne!(key_len, 0);
        assert!(check_tag(tag.as_str()));

        let tag_len: u32 = tag.len().try_into().unwrap();

        write_u32!(inner, BS_IDF);
        write_u32!(inner, zone_len);
        write_u32!(inner, key_len);
        write_u32!(inner, tag_len);
        write_bytes!(inner, tag.as_bytes());
        inner.flush()?;
        
        Ok(Writer { inner, zone_len, key_len })
    }

    pub fn len(&self) -> (u32, u32) {
        (self.zone_len, self.key_len)
    }

    pub fn push<Z: AsRef<[u8]>, K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, zone: Option<Z>, key: K, value: V) -> io::Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        let zone_len = zone.as_ref().map_or(0, |b| b.as_ref().len().try_into().unwrap());
        let key_len = key.len().try_into().unwrap();
        let value_len = value.len().try_into().unwrap();

        assert_eq!(self.zone_len, zone_len);
        assert_eq!(self.key_len, key_len);

        if self.zone_len != 0 {
            write_bytes!(self.inner, zone.unwrap().as_ref());
        }

        write_bytes!(self.inner, key);
        write_u32!(self.inner, value_len);
        write_bytes!(self.inner, value);

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub struct Reader<F: Read + Seek + Sized> {
    inner: F,
    zone_len: u32,
    key_len: u32,
}

type Pair = (Option<Vec<u8>>, Vec<u8>, Vec<u8>);

impl<F: Read + Seek + Sized> Reader<F> {
    pub fn new(mut inner: F) -> io::Result<(Reader<F>, Config)> {
        let idf = read_u32!(inner).unwrap();
        assert_eq!(idf, BS_IDF);

        let zone_len = read_u32!(inner).unwrap();
        let key_len = read_u32!(inner).unwrap();
        let tag_len = read_u32!(inner).unwrap();
        let tag = String::from_utf8(read_bytes!(inner, tag_len).unwrap()).unwrap();

        Ok((Reader { inner, zone_len, key_len }, Config { tag, zone_len, key_len }))
    }

    pub fn len(&self) -> (u32, u32) {
        (self.zone_len, self.key_len)
    }
}

impl<F: Read + Seek + Sized> Iterator for Reader<F> {
    type Item = io::Result<Pair>;

    fn next(&mut self) -> Option<Self::Item> {
        let zone = if self.zone_len != 0 {
            Some(read_bytes!(self.inner, self.zone_len)?)
        } else {
            None
        };
        let key = read_bytes!(self.inner, self.key_len)?;
        let value_len = read_u32!(self.inner)?;
        let value = read_bytes!(self.inner, value_len)?;
        Some(Ok((zone, key, value)))
    }
}
