use crate::sender::Requests;
use crate::{LEADING_BUFFER_SPACE, MAXIMUM_DATA, Request};
use grammers_crypto::DequeBuffer;
use grammers_mtproto::mtp::Mtp;
use grammers_mtproto::transport::Transport;
use parking_lot::RwLock;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct NetworkWriter<T: Transport> {
    inner: Arc<NetworkWriterInner<T>>,
}
pub struct NetworkWriterInner<T: Transport> {
    transport: T,
    m: Arc<RwLock<Box<dyn Mtp>>>,
    requests: Requests,
    write_tail: AtomicUsize,
    write_buffer: DequeBuffer<u8>,
    request_rx: Mutex<UnboundedReceiver<Request>>,
    connection_rx: Mutex<Receiver<Arc<OwnedWriteHalf>>>,
}

impl<T: Transport> NetworkWriter<T> {
    pub fn new(
        t: T,
        m: Arc<RwLock<Box<dyn Mtp>>>,
        requests: Requests,
        request_rx: UnboundedReceiver<Request>,
        connection_rx: Receiver<Arc<OwnedWriteHalf>>,
    ) -> Self {
        Self {
            inner: Arc::new(NetworkWriterInner {
                transport: t,
                m,
                requests,
                write_buffer: DequeBuffer::with_capacity(MAXIMUM_DATA, LEADING_BUFFER_SPACE),
                write_tail: AtomicUsize::new(0),
                request_rx: Mutex::new(request_rx),
                connection_rx: Mutex::new(connection_rx),
            }),
        }
    }
    pub fn spawn(&self) -> JoinHandle<()> {
        let slf = self.clone();
        tokio::spawn(async move {
            loop {
                let Some(req) = slf.request_rx.lock().await.recv().await else {
                    break;
                };
                slf.requests.lock().push(req);
            }
        })
    }
}

impl<T: Transport> Deref for NetworkWriter<T> {
    type Target = NetworkWriterInner<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
