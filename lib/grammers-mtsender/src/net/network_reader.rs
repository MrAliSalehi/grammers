use crate::net::NetStream;
use crate::process_mtp::process_mtp_buffer;
use crate::sender::Requests;
use crate::{
    InvocationError, MAXIMUM_DATA, ReadError, ReconnectionPolicy, RequestState, ServerAddr,
};
use grammers_mtproto::mtp::Mtp;
use grammers_mtproto::transport;
use grammers_mtproto::transport::Transport;
use grammers_tl_types as tl;
use grammers_tl_types::enums::Updates;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use std::io;
use std::io::Error;
use std::ops::{ControlFlow, Deref};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Clone)]
pub struct NetworkReader<T: Transport> {
    inner: Arc<NetworkReaderInner<T>>,
}

pub struct NetworkReaderInner<T: Transport> {
    requests: Requests,
    m: Arc<RwLock<Box<dyn Mtp>>>,
    transport: T,
    read_tail: AtomicUsize,
    read_buffer: Mutex<Vec<u8>>,
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
    update_tx: Sender<Vec<Updates>>,
    addr: ServerAddr,
    rp: &'static dyn ReconnectionPolicy,
    connection_tx: Sender<Arc<OwnedWriteHalf>>,
    reader: Mutex<OwnedReadHalf>,
}

impl<T: Transport> NetworkReader<T> {
    pub fn spawn_new(
        t: T,
        m: Arc<RwLock<Box<dyn Mtp>>>,
        requests: Requests,
        reader: OwnedReadHalf,
        update_tx: Sender<Vec<Updates>>,
        addr: ServerAddr,
        rp: &'static dyn ReconnectionPolicy,
        connection_tx: Sender<Arc<OwnedWriteHalf>>,
    ) -> Self {
        let read_tail = AtomicUsize::new(0);
        let read_buffer = Mutex::new(vec![0; MAXIMUM_DATA]);

        let slf = Self {
            inner: Arc::new(NetworkReaderInner {
                reader: Mutex::new(reader),
                read_tail,
                read_buffer,
                requests,
                m,
                rp,
                addr,
                update_tx,
                connection_tx,
                transport: t,
                handle: parking_lot::Mutex::new(None),
            }),
        };
        let slf_cl = slf.clone();
        let read_handle = tokio::spawn(async move {
            loop {
                _ = slf_cl
                    .step_network()
                    .await
                    .inspect_err(|e| error!("network_reader failed: {e}"));
            }
        });

        *slf.handle.lock() = Some(read_handle);

        todo!()
    }

    async fn step_network(&self) -> Result<(), ReadError> {
        let mut read_buffer = self.read_buffer.lock().await;
        let read_tail = self.read_tail.load(Ordering::Relaxed);

        let n = self
            .reader
            .lock()
            .await
            .read(&mut read_buffer[read_tail..])
            .await?;

        let updates = match self.on_net_read(&mut read_buffer, n) {
            Ok(u) => u,
            Err(e) => self.on_error(e).await?,
        };

        //todo handle the case where update_rx is closed.
        self.update_tx.send(updates).unwrap();
        Ok(())
    }

    /// Handle `n` more read bytes being ready to process by the transport.
    ///
    /// This won't cause `ReadError::Io`, but yet another enum would be overkill.
    fn on_net_read(
        &self,
        read_buffer: &mut Vec<u8>,
        n: usize,
    ) -> Result<Vec<tl::enums::Updates>, ReadError> {
        if n == 0 {
            return Err(ReadError::Io(Error::new(
                io::ErrorKind::ConnectionReset,
                "read 0 bytes",
            )));
        }

        // *read_tail += n;
        //fetch_add returns the old value, later we need the new value so +n is added again
        let read_tail = self.read_tail.fetch_add(n, Ordering::Relaxed) + n;

        trace!("read {} bytes from the network", n);
        trace!("trying to unpack buffer of {} bytes...", read_tail);

        // TODO the buffer might have multiple transport packets, what should happen with the
        // updates successfully read if subsequent packets fail to be deserialized properly?
        let mut updates = Vec::new();
        let mut next_offset = 0;
        while next_offset != read_tail {
            match self
                .transport
                .unpack(&mut read_buffer[next_offset..read_tail])
            {
                Ok(offset) => {
                    debug!("deserializing valid transport packet...");
                    let result = self.m.write().deserialize(
                        &read_buffer[next_offset..][offset.data_start..offset.data_end],
                    )?;

                    process_mtp_buffer(result, &mut updates, self.requests.clone());
                    next_offset += offset.next_offset;
                }
                Err(transport::Error::MissingBytes) => break,
                Err(err) => return Err(err.into()),
            }
        }

        read_buffer.copy_within(next_offset..read_tail, 0);
        //*read_tail -= next_offset;
        self.read_tail.fetch_sub(next_offset, Ordering::Relaxed);

        Ok(updates)
    }

    /// Handle errors that occurred while performing I/O.
    async fn on_error(&self, error: ReadError) -> Result<Vec<Updates>, ReadError> {
        info!("handling error: {error}");
        self.transport.reset();
        self.m.write().reset();
        let mut read_buffer = self.read_buffer.lock().await;
        info!(
            "resetting sender state from read_buffer {}/{}",
            self.read_tail.load(Ordering::Relaxed),
            read_buffer.len(),
        );
        self.read_tail.store(0, Ordering::Relaxed);
        read_buffer.fill(0);
        drop(read_buffer);

        let error = match error {
            ReadError::Io(_) if matches!(self.rp.should_retry(0), ControlFlow::Continue(_)) => {
                match self.try_connect().await {
                    Ok(_) => {
                        // Reconnect success means everything can be retried.
                        self.requests
                            .lock()
                            .iter_mut()
                            .for_each(|r| r.state = RequestState::NotSerialized);

                        // We'll return a TooLong update to signal to the client
                        // that it needs to call getDifference and query the server
                        // for new updates again.
                        return Ok(vec![tl::enums::Updates::TooLong]);
                    }
                    Err(e) => ReadError::from(e),
                }
            }
            e => e,
        };

        let mut requests = self.requests.lock();
        warn!(
            "marking all {} request(s) as failed: {}",
            requests.len(),
            &error
        );

        requests
            .drain(..)
            .for_each(|r| drop(r.result.send(Err(InvocationError::from(error.clone())))));

        Err(error)
    }

    async fn try_connect(&self) -> Result<(), Error> {
        let mut attempts = 0;
        loop {
            match NetStream::connect(&self.addr).await.map(|e| e.into_split()) {
                Ok((read, write)) => {
                    info!(
                        "auto-reconnect success after {} failed attempt(s)",
                        attempts
                    );
                    *self.reader.lock().await = read;
                    //send the writer half of stream to the writer task
                    self.connection_tx.send(Arc::new(write)).unwrap();
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    warn!("auto-reconnect failed {} time(s): {}", attempts, e);
                    sleep(Duration::from_secs(1)).await;

                    match self.rp.should_retry(attempts) {
                        ControlFlow::Break(_) => {
                            log::error!(
                                "attempted more than {} times for reconnection and failed",
                                attempts
                            );
                            return Err(e);
                        }
                        ControlFlow::Continue(duration) => {
                            sleep(duration).await;
                        }
                    }
                }
            }
        }
    }
}

impl<T: Transport> Deref for NetworkReader<T> {
    type Target = NetworkReaderInner<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
