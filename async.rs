use std::{fs::OpenOptions, path::Path};
use request_channel::{Req, ReqPayload, unbounded::{channel, ReqTx}};
pub use kvdump::*;

#[derive(Debug, Clone)]
pub enum Request {
    KV(KV),
    Hash,
}

impl Req for Request {
    type Res = Result<()>;
}

pub struct Db {
    tx: ReqTx<Request>,
}

pub struct Scope {
    scope: Box<[u8]>,
    tx: ReqTx<Request>,
}

impl Db {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<Db> {
        let (tx, mut rx) = channel::<Request>();
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = Writer::init(file, config)?;
        tokio::spawn(async move {
            while let Ok(ReqPayload { req, res_tx }) = rx.recv().await {
                res_tx.send(match req {
                    Request::KV(kv) => writer.write_kv(kv),
                    Request::Hash => writer.write_hash().map(|_| ()),
                }).expect("FATAL: Channel closed when sending a response");
            }
        });
        Ok(Db { tx })
    }

    pub fn open_scope<S: AsRef<[u8]>>(&self, scope: S) -> Scope {
        Scope {
            scope: Box::from(scope.as_ref()),
            tx: self.tx.clone(),
        }
    }

    pub async fn write_hash(&self) -> Result<()> {
        self.tx.send_recv(Request::Hash).await.unwrap_or(Err(Error::AsyncFileClosed))
    }
}

impl Scope {
    pub fn name(&self) -> Box<[u8]> {
        self.scope.clone()
    }

    pub async fn write_kv<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.tx.send_recv(Request::KV(KV {
            scope: self.scope.clone(),
            key: Box::from(key.as_ref()),
            value: Box::from(value.as_ref()),
        })).await.unwrap_or(Err(Error::AsyncFileClosed))
    }
}
