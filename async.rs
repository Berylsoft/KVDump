use std::{fs::OpenOptions, path::Path};
use tokio::{spawn, sync::mpsc};
pub use kvdump::*;

type Tx = mpsc::UnboundedSender<KV>;

pub struct AsyncFileWriter {
    tx: Tx,
}

pub struct AsyncFileScopeWriter {
    scope: Vec<u8>,
    tx: Tx,
}

impl AsyncFileWriter {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<AsyncFileWriter> {
        let (tx, mut rx) = mpsc::unbounded_channel::<KV>();
        let file = OpenOptions::new().write(true).create(true).open(path)?;
        let mut writer = Writer::init(file, config)?;
        spawn(async move {
            while let Some(kv) = rx.recv().await {
                // TODO spread error to upstream
                writer.write_kv(kv).unwrap();
            }
        });
        Ok(AsyncFileWriter { tx })
    }

    pub fn new_scope(&self, scope: Vec<u8>) -> AsyncFileScopeWriter {
        AsyncFileScopeWriter { scope, tx: self.tx.clone() }
    }

    // TODO impl close()
}

impl AsyncFileScopeWriter {
    pub fn write_kv(&self, key: Vec<u8>, value: Vec<u8>) {
        self.tx.send(KV { scope: self.scope.clone(), key, value }).unwrap();
    }
}
