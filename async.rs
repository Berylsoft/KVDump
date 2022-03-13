use std::{fs::OpenOptions, path::Path};
pub use kvdump::*;

type Tx = bmrng::unbounded::UnboundedRequestSender<Request, Result<Response>>;

pub struct AsyncFileWriter {
    tx: Tx,
}

pub struct AsyncFileScopeWriter {
    scope: Vec<u8>,
    tx: Tx,
}

impl AsyncFileWriter {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<AsyncFileWriter> {
        let (tx, mut rx) = bmrng::unbounded_channel::<Request, Result<Response>>();
        let file = OpenOptions::new().write(true).create(true).open(path)?;
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

    pub fn new_scope(&self, scope: Vec<u8>) -> AsyncFileScopeWriter {
        AsyncFileScopeWriter { scope, tx: self.tx.clone() }
    }

    pub async fn write_hash(self) -> Result<Hash> {
        let req = Request::Hash;
        let resp = self.tx.send_receive(req).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed));
        resp.map(|res| match res {
            Response::Hash(hash) => hash,
            _ => unreachable!(),
        })
    }

    pub async fn close(self) -> Result<()> {
        let req = Request::End;
        let resp = self.tx.send_receive(req).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed));
        resp.map(|res| match res {
            Response::End => (),
            _ => unreachable!(),
        })
    }
}

impl AsyncFileScopeWriter {
    pub async fn write_kv(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let req = Request::KV(KV { scope: self.scope.clone(), key, value });
        let resp = self.tx.send_receive(req).await.unwrap_or_else(|_| Err(Error::AsyncFileClosed));
        resp.map(|res| match res {
            Response::KV => (),
            _ => unreachable!(),
        })
    }
}
