pub use async_oneshot::{oneshot as one_channel, Sender as OneTx};
pub use async_channel::{unbounded as req_channel, Sender as ReqTx, Receiver as ReqRx};

#[inline]
pub fn spawn_blocking<F, R>(func: F)
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    blocking::unblock(func).detach()
}

#[inline]
pub async fn unblock<F, R>(func: F) -> std::io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    Ok(blocking::unblock(func).await)
}

#[inline(always)]
pub fn recv_req<T: Send>(rx: &mut ReqRx<T>) -> Option<T> {
    rx.recv_blocking().ok()
}

#[inline(always)]
pub fn send_res<T: Send>(mut tx: OneTx<T>, v: T) -> Result<(), async_oneshot::Closed> {
    tx.send(v)
}
