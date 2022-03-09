pub const BS_IDENT: u32 = 0x42650000;

use std::io::{self, Read, Write, Seek};
use blake3::{Hasher, OUT_LEN as HASH_LEN};

macro_rules! into {
    ($val:expr) => {
        $val.try_into().unwrap()
    };
}

// unstable feature: bool_to_option
fn then_some<T>(b: bool, t: T) -> Option<T> {
    if b { Some(t) } else { None }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    VersionNotMatch { existing: u32 },
    ConfigNotMatch { existing: Config, current: Config },
    HashNotMatch { read_hash: Vec<u8>, calc_hash: Vec<u8> },
    InputLengthNotMatch { config_len: u32, input_len: u32, which: String },
    UnexpectedEnd { buffer: Vec<u8>, expected_len: usize, actual_len: usize },
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

macro_rules! read_num_impl {
    ($self:expr, $type:ty, $len:expr) => {{
        const LEN_NUM: usize = $len;
        let mut buf = [0u8; LEN_NUM];
        let actual_len = $self.read(&mut buf)?;
        if actual_len == LEN_NUM {
            Ok(<$type>::from_be_bytes(buf))
        } else {
            Err(Error::UnexpectedEnd {
                buffer: buf.to_vec(),
                expected_len: LEN_NUM,
                actual_len,
            })
        }
    }};
}

trait ReadHelper: Read {
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>>;
    fn read_u32(&mut self) -> Result<u32>;
    fn read_u8(&mut self) -> Result<u8>;
}

impl<R: Read> ReadHelper for R {
    #[inline]
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let actual_len = self.read(&mut buf)?;
        if actual_len == len {
            Ok(buf)
        } else {
            Err(Error::UnexpectedEnd {
                buffer: buf.to_vec(),
                expected_len: len,
                actual_len,
            })
        }
    }

    #[inline]
    fn read_u32(&mut self) -> Result<u32> {
        read_num_impl!(self, u32, 4)
    }

    #[inline]
    fn read_u8(&mut self) -> Result<u8> {
        read_num_impl!(self, u8, 1)
    }
}

macro_rules! write_num_impl {
    ($self:expr, $type:ty, $val:expr) => {{
        $self.write(<$type>::to_be_bytes($val).as_ref())?;
        Ok(())
    }};
}

trait WriteHelper: Write {
    fn write_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) -> io::Result<()>;
    fn write_u32(&mut self, val: u32) -> io::Result<()>;
    fn write_u8(&mut self, val: u8) -> io::Result<()>;
}

impl<W: Write> WriteHelper for W {
    #[inline]
    fn write_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) -> io::Result<()> {
        self.write(bytes.as_ref())?;
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
}

const COL_KV: u8 = 0;
const COL_END: u8 = 1;

const SIZED_FLAG_SCOPE: u8 = 1 << 0;
const SIZED_FLAG_KEY: u8 = 1 << 1;
const SIZED_FLAG_VALUE: u8 = 1 << 2;

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

impl Lengths {
    fn flag(&self) -> u8 {
        let mut flag = 0;
        if self.scope.is_some() { flag |= SIZED_FLAG_SCOPE }
        if self.key.is_some() { flag |= SIZED_FLAG_KEY }
        if self.value.is_some() { flag |= SIZED_FLAG_VALUE }
        flag
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Config {
    pub ident: Vec<u8>,
    pub len: Lengths,
}

pub struct Reader<F: Read + Seek + Sized> {
    inner: F,
    config: Config,
    hasher: Hasher,
}

impl<F: Read + Seek + Sized> Reader<F> {
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn read_init(inner: &mut F) -> Result<Config> {
        let version = inner.read_u32()?;
        if version != BS_IDENT {
            return Err(Error::VersionNotMatch { existing: version });
        }

        let ident_len = into!(inner.read_u32()?);
        let ident = inner.read_bytes(ident_len)?;

        let sized_flags = inner.read_u8()?;
        macro_rules! read_init_kvf_impl {
            ($x:ident, $flag:expr) => {
                let $x = then_some((sized_flags & $flag) != 0, inner.read_u32()?);
            };
        }
        read_init_kvf_impl!(scope, SIZED_FLAG_SCOPE);
        read_init_kvf_impl!(key, SIZED_FLAG_KEY);
        read_init_kvf_impl!(value, SIZED_FLAG_VALUE);
        let len = Lengths { scope, key, value };

        Ok(Config { ident, len })
    }

    pub fn read_kv(&mut self) -> Result<KV> {
        assert_eq!(self.inner.read_u8()?, COL_KV);

        macro_rules! read_kvf_impl {
            ($x:ident) => {
                let len = into!(match self.config.len.$x {
                    Some(len) => len,
                    None => self.inner.read_u32()?,
                });
                let $x = self.inner.read_bytes(len)?;
                self.hasher.update(&$x);
            };
        }
        read_kvf_impl!(scope);
        read_kvf_impl!(key);
        read_kvf_impl!(value);

        Ok(KV { scope, key, value })
    }

    pub fn read_end(&mut self) -> Result<Vec<u8>> {
        assert_eq!(self.inner.read_u8()?, COL_END);

        let read_hash = self.inner.read_bytes(HASH_LEN)?;
        let calc_hash = self.hasher.finalize();
        if read_hash.as_slice() != calc_hash.as_bytes() {
            return Err(Error::HashNotMatch {
                read_hash,
                calc_hash: calc_hash.as_bytes().to_vec(),
            });
        }
        self.hasher.reset();

        Ok(read_hash)
    }

    pub fn init(mut inner: F) -> Result<Reader<F>> {
        let config = Reader::read_init(&mut inner)?;
        let hasher = Hasher::new();
        Ok(Reader { inner, config, hasher })
    }
}

pub struct Writer<F: Read + Write + Seek + Sized> {
    inner: F,
    config: Config,
    hasher: Hasher,
}

impl<F: Read + Write + Seek + Sized> Writer<F> {
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn write_init(&mut self) -> Result<()> {
        self.inner.write_u32(BS_IDENT)?;

        self.inner.write_u32(into!(self.config.ident.len()))?;
        self.inner.write_bytes(self.config.ident.clone())?;

        self.inner.write_u8(self.config.len.flag())?;
        macro_rules! write_init_kvf_impl {
            ($x:ident) => {
                self.inner.write_u32(self.config.len.$x.unwrap_or(0))?;
            };
        }
        write_init_kvf_impl!(scope);
        write_init_kvf_impl!(key);
        write_init_kvf_impl!(value);

        self.inner.flush()?;
        Ok(())
    }

    pub fn write_kv(&mut self, kv: KV) -> Result<()> {
        self.inner.write_u8(COL_KV)?;

        macro_rules! write_kvf_impl {
            ($x:ident) => {{
                let input_len = into!(kv.$x.len());
                match self.config.len.$x {
                    Some(config_len) => {
                        if config_len != input_len {
                            return Err(Error::InputLengthNotMatch {
                                config_len,
                                input_len,
                                which: stringify!($x).to_owned(),
                            });
                        }
                    },
                    None => self.inner.write_u32(input_len)?,
                }
                self.hasher.update(&kv.$x);
                self.inner.write_bytes(kv.$x)?;
            }};
        }
        write_kvf_impl!(scope);
        write_kvf_impl!(key);
        write_kvf_impl!(value);

        // TODO may too frequent
        self.inner.flush()?;
        Ok(())
    }

    pub fn write_end(mut self) -> Result<Vec<u8>> {
        self.inner.write_u8(COL_END)?;

        let hash = self.hasher.finalize();
        self.inner.write_bytes(hash.as_bytes())?;

        Ok(hash.as_bytes().to_vec())
    }

    pub fn init(inner: F, config: Config) -> Result<Writer<F>> {
        let hasher = Hasher::new();
        let mut _self = Writer { inner, config, hasher };
        // _self.write_init()?;
        Ok(_self)
    }
}
