use crate::errors::WriteError;
use crate::sender::Requests;
use crate::{
    LEADING_BUFFER_SPACE, MAXIMUM_DATA, MsgIdPair, NO_PING_DISCONNECT, PING_DELAY, ReadError,
    Request, RequestState,
};
use grammers_crypto::DequeBuffer;
use grammers_mtproto::mtp::Mtp;
use grammers_mtproto::transport::Transport;
use grammers_tl_types as tl;
use grammers_tl_types::Serializable;
use log::{debug, error, info, trace};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub struct NetworkWriter {
    transport: Arc<Box<dyn Transport>>,
    m: Arc<RwLock<Box<dyn Mtp>>>,
    requests: Requests,
    write_head: usize,
    write_buffer: DequeBuffer<u8>,
    connection_rx: Receiver<Arc<OwnedWriteHalf>>,
    writer: OwnedWriteHalf,
    last_ping: Instant,
    request_rx: UnboundedReceiver<Request>,
    request_tx: UnboundedSender<Request>,
}

impl NetworkWriter {
    pub fn new(
        t: Arc<Box<dyn Transport>>,
        m: Arc<RwLock<Box<dyn Mtp>>>,
        requests: Requests,
        connection_rx: Receiver<Arc<OwnedWriteHalf>>,
        writer: OwnedWriteHalf,
        request_rx: UnboundedReceiver<Request>,
        request_tx: UnboundedSender<Request>,
    ) -> Self {
        Self {
            request_tx,
            request_rx,
            transport: t,
            m,
            requests,
            writer,
            write_head: 0,
            write_buffer: DequeBuffer::with_capacity(MAXIMUM_DATA, LEADING_BUFFER_SPACE),
            connection_rx,
            last_ping: Instant::now(),
        }
    }
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_micros(100)).await;
                if self.last_ping.elapsed() > PING_DELAY {
                    self.send_ping().await;
                }
                match self.request_rx.try_recv() {
                    Ok(req) => {
                        self.requests.lock().await.push(req);
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            if !self
                                .requests
                                .lock()
                                .await
                                .iter()
                                .any(|e| matches!(e.state, RequestState::NotSerialized))
                            {
                                continue;
                            }
                        }
                        TryRecvError::Disconnected => {
                            break;
                        }
                    },
                }

                _ = self
                    .try_write()
                    .await
                    .inspect_err(|e| error!("try_write failed:{e:?}"));
            }
            info!("nww closed");
        })
    }

    async fn send_ping(&mut self) {
        pub(crate) fn generate_random_id() -> i64 {
            static LAST_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

            if LAST_ID.load(Ordering::SeqCst) == 0 {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .expect("system time is before epoch")
                    .as_nanos() as i64;

                LAST_ID
                    .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst)
                    .unwrap();
            }

            LAST_ID.fetch_add(1, Ordering::SeqCst)
        }
        let ping_id = generate_random_id();
        let (tx, rx) = oneshot::channel();
        let body = tl::functions::PingDelayDisconnect {
            ping_id,
            disconnect_delay: NO_PING_DISCONNECT,
        }
        .to_bytes();
        self.request_tx
            .send(Request {
                body,
                state: RequestState::NotSerialized,
                result: tx,
            })
            .unwrap();
        tokio::spawn(async move {
            _=rx.await.unwrap();
            info!("received ping response ");
        });
        info!("sent ping {ping_id}");
        self.last_ping = Instant::now();
    }

    async fn try_write(&mut self) -> Result<(), WriteError> {
        if !self.write_buffer.is_empty() {
            return Ok(());
        }

        let r = self.requests.clone();
        let mut requests = r.lock().await;

        // TODO add a test to make sure we only ever send the same request once
        let mut mtp = self.m.write().await;
        for request in requests
            .iter_mut()
            .filter(|r| matches!(r.state, RequestState::NotSerialized))
        {
            // TODO make mtp itself use BytesMut to avoid copies
            if let Some(msg_id) = mtp.push(&mut self.write_buffer, &request.body) {
                assert!(request.body.len() >= 4);
                let req_id = u32::from_le_bytes([
                    request.body[0],
                    request.body[1],
                    request.body[2],
                    request.body[3],
                ]);
                debug!(
                    "serialized request {req_id:x} ({}) with {msg_id:?}",
                    tl::name_for_id(req_id)
                );
                // Note how only NotSerialized become Serialized.
                // Nasty bugs that take ~2h to find occur otherwise!
                // (e.g. infinite loops leading to transport flood.)
                request.state = RequestState::Serialized(MsgIdPair::new(msg_id));
            } else {
                break;
            }
        }

        if let Some(container_msg_id) = mtp.finalize(&mut self.write_buffer) {
            for request in requests.iter_mut() {
                if let RequestState::Serialized(ref mut pair) = request.state {
                    pair.container_msg_id = container_msg_id;
                }
            }
            self.transport.pack(&mut self.write_buffer)
        }
        drop(mtp);

        let n = self.write_network().await?;

        if n == 0 {
            return Ok(());
        }

        self.on_net_write(n, &mut requests).await;
        drop(requests);
        Ok(())
    }

    /// Handle `n` more written bytes being ready to process by the transport.
    async fn on_net_write(&mut self, n: usize, requests: &mut [Request]) {
        self.write_head += n;

        trace!(
            "written {n} bytes to the network ({}/{})",
            self.write_head,
            self.write_buffer.len()
        );

        assert!(self.write_head <= self.write_buffer.len());
        if self.write_head != self.write_buffer.len() {
            info!("on_net_write head != buffer.len");
            return;
        }

        self.write_head = 0;
        self.write_buffer.clear();

        for req in requests {
            if let RequestState::Serialized(ref pair) = req.state {
                debug!("sent request with {:?}", pair);
                req.state = RequestState::Sent(pair.clone());
            }
        }
    }

    async fn write_network(&mut self) -> Result<usize, WriteError> {
        let write_len = self.write_buffer.len() - self.write_head;
        loop {
            trace!("sending up to {write_len} bytes via network");

            let res = self
                .writer
                .write(&self.write_buffer[self.write_head..])
                .await
                .map_err(|e| ReadError::Io(e));

            match res {
                Ok(n) => return Ok(n),
                Err(e) => {
                    error!("write_network failed: {e}, waiting for new connection");
                    let Ok(Ok(recv)) =
                        tokio::time::timeout(Duration::from_secs(2), self.connection_rx.recv())
                            .await
                    else {
                        return Err(WriteError::BadWriter);
                    };
                    self.writer =
                        Arc::try_unwrap(recv).map_err(|_| WriteError::WriterNotReleased)?;
                }
            }
        }
    }
}
