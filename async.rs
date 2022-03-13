use std::{fs::OpenOptions, path::Path};
pub use kvdump::*;

type Tx = bmrng::unbounded::UnboundedRequestSender<Request, Result<Response>>;

pub struct AsyncFileWriter {
    tx: Tx,
}

pub struct AsyncFileScopeWriter {
    scope: Box<[u8]>,
    tx: Tx,
}

impl AsyncFileWriter {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<AsyncFileWriter> {
        let (tx, mut rx) = bmrng::unbounded_channel::<Request, Result<Response>>();
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = Writer::init(file, config)?;
        tokio::spawn(async move {
            while let Ok((request, responder)) = rx.recv().await {
                let drop = match request {
                    Request::End => true,
                    _ => false,
                };
                responder.respond(writer.write(request)).expect("FATAL: Channel closed when sending a response");
                if drop {
                    break;
                }
            }
        });
        Ok(AsyncFileWriter { tx })
    }

    pub fn new_scope<S: AsRef<[u8]>>(&self, scope: S) -> AsyncFileScopeWriter {
        AsyncFileScopeWriter {
            scope: Box::from(scope.as_ref()),
            tx: self.tx.clone(),
        }
    }

    pub async fn write_hash(&self) -> Result<Hash> {
        let req = Request::Hash;
        let resp = self.tx.send_receive(req).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed))?;
        Ok(match resp {
            Response::Hash(hash) => hash,
            _ => unreachable!(),
        })
    }
}

impl AsyncFileScopeWriter {
    pub async fn write_kv<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        let req = Request::KV(KV {
            scope: self.scope.clone(),
            key: Box::from(key.as_ref()),
            value: Box::from(value.as_ref()),
        });
        let resp = self.tx.send_receive(req).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed))?;
        Ok(match resp {
            Response::KV => (),
            _ => unreachable!(),
        })
    }
}
