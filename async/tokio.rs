pub use tokio::sync::{
    oneshot::{channel as one_channel, Sender as OneTx},
    mpsc::{unbounded_channel as req_channel, UnboundedSender as ReqTx, UnboundedReceiver as ReqRx},
};

#[inline]
pub fn spawn_blocking<F, R>(func: F)
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let _ = tokio::runtime::Handle::current().spawn_blocking(func);
}

#[inline]
pub async fn unblock<F, R>(func: F) -> std::io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::runtime::Handle::current().spawn_blocking(func).await.map_err(|err| std::io::Error::new(
        std::io::ErrorKind::Other,
        err,
    ))
}

#[inline(always)]
pub fn recv_req<T: Send>(rx: &mut ReqRx<T>) -> Option<T> {
    rx.blocking_recv()
}

#[inline(always)]
#[allow(unused_mut)]
pub fn send_res<T: Send>(mut tx: OneTx<T>, v: T) -> Result<(), T> {
    tx.send(v)
}
