use std::{fs::OpenOptions, path::Path};
use tokio::{spawn, sync::mpsc};
pub use kvdump::*;

#[derive(Debug)]
enum Message {
    KV(KV),
    Close,
}

type Tx = mpsc::UnboundedSender<Message>;

pub struct AsyncFileWriter {
    tx: Tx,
}

pub struct AsyncFileScopeWriter {
    scope: Vec<u8>,
    tx: Tx,
}

impl AsyncFileWriter {
    pub fn init<P: AsRef<Path>>(path: P, config: Config) -> Result<AsyncFileWriter> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let file = OpenOptions::new().write(true).create(true).open(path)?;
        let mut writer = Writer::init(file, config)?;
        spawn(async move {
            while let Some(message) = rx.recv().await {
                // TODO spread error to upstream
                match message {
                    Message::KV(kv) => {
                        writer.write_kv(kv).unwrap();
                    },
                    Message::Close => {
                        writer.write_hash().unwrap();
                        break;
                    }
                }
            }
        });
        Ok(AsyncFileWriter { tx })
    }

    pub fn new_scope(&self, scope: Vec<u8>) -> AsyncFileScopeWriter {
        AsyncFileScopeWriter { scope, tx: self.tx.clone() }
    }

    pub fn close(self) {
        self.tx.send(Message::Close).unwrap();
    }
}

impl AsyncFileScopeWriter {
    pub fn write_kv(&self, key: Vec<u8>, value: Vec<u8>) {
        self.tx.send(Message::KV(KV { scope: self.scope.clone(), key, value })).unwrap();
    }
}
