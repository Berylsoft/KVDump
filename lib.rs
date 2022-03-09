pub const BS_IDENT: u32 = 0x42650000;

use std::io::{self, Read, Write, Seek};
use blake3::{Hasher, OUT_LEN as HASH_LEN};

pub type ReadResult<T> = io::Result<Result<T, Vec<u8>>>;

macro_rules! into {
    ($val:expr) => {
        $val.try_into().unwrap()
    };
}

// unstable feature: bool_to_option
fn then_some<T>(b: bool, t: T) -> Option<T> {
    if b { Some(t) } else { None }
}

macro_rules! read_num_impl {
    ($self:expr, $type:ty, $len:expr) => {{
        const LEN_NUM: usize = $len;
        let mut buf = [0u8; LEN_NUM];
        match $self.inner.read(&mut buf) {
            Ok(acc_len) => Ok({
                if acc_len == LEN_NUM {
                    Ok(<$type>::from_be_bytes(buf))
                } else {
                    Err(buf.to_vec())
                }
            }),
            Err(error) => Err(error),
        }
    }};
}

macro_rules! write_num_impl {
    ($self:expr, $type:ty, $val:expr) => {{
        $self.inner.write(<$type>::to_be_bytes($val).as_ref())?;
        Ok(())
    }};
}

const COL_KV: u8 = 0;
const COL_END: u8 = 1;

const SIZED_FLAG_SCOPE: u8 = 1 << 0;
const SIZED_FLAG_KEY: u8 = 1 << 1;
const SIZED_FLAG_VALUE: u8 = 1 << 2;

struct SizedFlags {
    pub scope: bool,
    pub key: bool,
    pub value: bool,
}

impl SizedFlags {
    pub fn encode(&self) -> u8 {
        let mut n = 0;
        if self.scope { n |= SIZED_FLAG_SCOPE }
        if self.key { n |= SIZED_FLAG_KEY }
        if self.value { n |= SIZED_FLAG_VALUE }
        n
    }

    pub fn decode(n: u8) -> SizedFlags {
        SizedFlags {
            scope: (n & SIZED_FLAG_SCOPE) != 0,
            key: (n & SIZED_FLAG_KEY) != 0,
            value: (n & SIZED_FLAG_VALUE) != 0,
        }
    }

    pub fn from_lengths(len: &Lengths) -> SizedFlags {
        SizedFlags {
            scope: len.scope.is_some(),
            key: len.key.is_some(),
            value: len.value.is_some(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct KV {
    pub scope: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Lengths {
    pub scope: Option<u32>,
    pub key: Option<u32>,
    pub value: Option<u32>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Config {
    pub ident: Vec<u8>,
    pub len: Lengths,
}

pub struct Writer<F: Read + Write + Seek + Sized> {
    inner: F,
    config: Config,
    hasher: Hasher,
}

impl<F: Read + Write + Seek + Sized> Writer<F> {
    #[inline]
    fn read_bytes(&mut self, len: usize) -> ReadResult<Vec<u8>> {
        let mut buf = vec![0u8; len];
        match self.inner.read(&mut buf) {
            Ok(acc_len) => Ok({
                if acc_len == len {
                    Ok(buf)
                } else {
                    Err(buf)
                }
            }),
            Err(error) => Err(error),
        }
    }

    #[inline]
    fn read_u32(&mut self) -> ReadResult<u32> {
        read_num_impl!(self, u32, 4)
    }

    #[inline]
    fn read_u8(&mut self) -> ReadResult<u8> {
        read_num_impl!(self, u8, 1)
    }

    #[inline]
    fn write_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) -> io::Result<()> {
        self.inner.write(bytes.as_ref())?;
        Ok(())
    }
    
    #[inline]
    fn write_u32(&mut self, val: u32) -> io::Result<()> {
        write_num_impl!(self, u32, val)
    }

    #[inline]
    fn write_u8(&mut self, val: u8) -> io::Result<()> {
        write_num_impl!(self, u8, val)
    }

    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    pub fn write_init(&mut self) -> io::Result<()> {
        self.write_u32(BS_IDENT)?;

        self.write_u32(into!(self.config.ident.len()))?;
        self.write_bytes(self.config.ident.clone())?;

        self.write_u8(SizedFlags::from_lengths(&self.config.len).encode())?;
        macro_rules! write_init_kvf_impl {
            ($x:ident) => {
                self.write_u32(self.config.len.$x.unwrap_or(0))?;
            }
        }
        write_init_kvf_impl!(scope);
        write_init_kvf_impl!(key);
        write_init_kvf_impl!(value);

        self.flush()?;
        Ok(())
    }

    pub fn read_init(&mut self) -> io::Result<Config> {
        assert_eq!(self.read_u32()?.unwrap(), BS_IDENT);

        let ident_len = into!(self.read_u32()?.unwrap());
        let ident = self.read_bytes(ident_len)?.unwrap();

        let sized_flags = SizedFlags::decode(self.read_u8()?.unwrap());
        macro_rules! read_init_kvf_impl {
            ($x:ident) => {
                let $x = then_some(sized_flags.$x, self.read_u32()?.unwrap());
            }
        }
        read_init_kvf_impl!(scope);
        read_init_kvf_impl!(key);
        read_init_kvf_impl!(value);
        let len = Lengths { scope, key, value };

        Ok(Config { ident, len })
    }

    pub fn write_kv(&mut self, kv: KV) -> io::Result<()> {
        self.write_u8(COL_KV)?;

        macro_rules! write_kvf_impl {
            ($x:ident) => {{
                let input_len = into!(kv.$x.len());
                match self.config.len.$x {
                    Some(len) => assert_eq!(len, input_len),
                    None => self.write_u32(input_len)?,
                }
                self.hasher.update(&kv.$x);
                self.write_bytes(kv.$x)?;
            }}
        }
        write_kvf_impl!(scope);
        write_kvf_impl!(key);
        write_kvf_impl!(value);

        Ok(())
    }

    pub fn read_kv(&mut self) -> io::Result<KV> {
        assert_eq!(self.read_u8()?.unwrap(), COL_KV);

        macro_rules! read_kvf_impl {
            ($x:ident) => {
                let len = into!(match self.config.len.$x {
                    Some(len) => len,
                    None => self.read_u32()?.unwrap(),
                });
                let $x = self.read_bytes(len)?.unwrap();
                self.hasher.update(&$x);
            };
        }
        read_kvf_impl!(scope);
        read_kvf_impl!(key);
        read_kvf_impl!(value);

        Ok(KV { scope, key, value })
    }

    pub fn write_end(mut self) -> io::Result<Vec<u8>> {
        self.write_u8(COL_END)?;
        
        let hash = self.hasher.finalize();
        self.write_bytes(hash.as_bytes())?;

        Ok(hash.as_bytes().to_vec())
    }

    pub fn read_end(&mut self) -> io::Result<Vec<u8>> {
        assert_eq!(self.read_u8()?.unwrap(), COL_END);
        
        let read_hash = self.read_bytes(HASH_LEN)?.unwrap();
        let calc_hash = self.hasher.finalize();
        assert_eq!(read_hash.as_slice(), calc_hash.as_bytes());

        Ok(read_hash)
    }

    pub fn new(inner: F, config: Config) -> io::Result<Writer<F>> {
        let hasher = Hasher::new();
        let mut _self = Writer { inner, config, hasher };
        // _self.write_init()?;
        Ok(_self)
    }
}
