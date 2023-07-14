use std::{path::{Path, PathBuf}, fs::{OpenOptions, File}};

pub use kvdump;
use kvdump::*;

#[path = "tokio.rs"]
mod async_basics;
use async_basics::*;

#[derive(Debug, Clone)]
pub enum Request {
    KV(KV),
    Hash,
    Sync,
    Close,
}

pub type ReqPayload = (Request, OneTx<Result<()>>);

#[derive(Clone)]
pub struct Handle {
    // access inner is generally safe
    pub inner: ReqTx<ReqPayload>,
}

impl Handle {
    pub async fn request(&self, req: Request) -> std::result::Result<Result<()>, Option<Request>> {
        let (res_tx, res_rx) = one_channel::<Result<()>>();
        self.inner.send((req, res_tx)).map_err(|payload| Some(payload.0.0))?;
        res_rx.await.map_err(|_| None)
    }

    pub async fn wait_close(self) -> Result<()> {
        self.request(Request::Close).await.unwrap_or(Err(Error::AsyncFileClosed))
    }
}

struct Context {
    writer: Writer<File>,
    non_synced: u16,
    sync_interval: u16,
}

impl Context {
    fn init(path: &Path, config: Config, sync_interval: u16) -> Result<Context> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Context { writer: Writer::init(file, config)?, non_synced: 0, sync_interval })
    }

    fn exec(&mut self, req: Request) -> Result<()> {
        match req {
            Request::KV(kv) => {
                self.writer.write_kv(kv)?;
                self.non_synced += 1;
                if self.non_synced >= self.sync_interval {
                    self.writer.datasync()?;
                    self.non_synced = 0;
                }
            },
            Request::Hash => {
                let _ = self.writer.write_hash()?;
            },
            Request::Sync => {
                self.writer.datasync()?;
            },
            Request::Close => {
                self.writer.close_file()?;
            }
        }
        Ok(())
    }
}

pub async fn open(path: PathBuf, config: Config, sync_interval: u16) -> Result<Handle> {
    let (req_tx, mut req_rx) = req_channel::<ReqPayload>();
    let req_tx = Handle { inner: req_tx };
    let mut ctx = unblock(move || {
        Context::init(&path, config, sync_interval)
    }).await??;
    spawn_blocking(move || {
        if let Some((req, res_tx)) = req_rx.blocking_recv() {
            res_tx.send(ctx.exec(req)).expect("FATAL: all request sender dropped");
        } else {
            ctx.exec(Request::Close).expect("FATAL: Error occurred during closing");
        }
    });
    Ok(req_tx)
}
