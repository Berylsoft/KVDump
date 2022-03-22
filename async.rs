use std::{fs::OpenOptions, path::Path};
pub use kvdump::*;

#[derive(Debug, Clone)]
pub enum Request {
    KV(KV),
    Hash,
}

type Tx = bmrng::unbounded::UnboundedRequestSender<Request, Result<()>>;

pub struct Db {
    tx: Tx,
}

pub struct Scope {
    scope: Box<[u8]>,
    tx: Tx,
}

impl Db {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<Db> {
        let (tx, mut rx) = bmrng::unbounded_channel::<Request, Result<()>>();
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = Writer::init(file, config)?;
        tokio::spawn(async move {
            while let Ok((request, responder)) = rx.recv().await {
                responder.respond(match request {
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
        self.tx.send_receive(Request::Hash).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed))
    }
}

impl Scope {
    pub fn name(&self) -> Box<[u8]> {
        self.scope.clone()
    }

    pub async fn write_kv<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.tx.send_receive(Request::KV(KV {
            scope: self.scope.clone(),
            key: Box::from(key.as_ref()),
            value: Box::from(value.as_ref()),
        })).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed))
    }
}
