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
use std::io;
use std::io::Error;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::RwLock;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub struct NetworkReader {
    requests: Requests,
    m: Arc<RwLock<Box<dyn Mtp>>>,
    transport: Arc<Box<dyn Transport>>,
    read_tail: usize,
    read_buffer: Vec<u8>,
    update_tx: Sender<Vec<Updates>>,
    addr: ServerAddr,
    rp: &'static dyn ReconnectionPolicy,
    connection_tx: Sender<Arc<OwnedWriteHalf>>,
    reader: OwnedReadHalf,
}

impl NetworkReader {
    pub fn new(
        t: Arc<Box<dyn Transport>>,
        m: Arc<RwLock<Box<dyn Mtp>>>,
        requests: Requests,
        reader: OwnedReadHalf,
        update_tx: Sender<Vec<Updates>>,
        addr: ServerAddr,
        rp: &'static dyn ReconnectionPolicy,
        connection_tx: Sender<Arc<OwnedWriteHalf>>,
    ) -> Self {
        Self {
            reader,
            read_tail: 0,
            read_buffer: vec![0; MAXIMUM_DATA],
            requests,
            m,
            rp,
            addr,
            update_tx,
            connection_tx,
            transport: t,
        }
    }

    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                _ = self
                    .step_network()
                    .await
                    .inspect_err(|e| error!("network_reader failed: {e}"));
            }
        })
    }

    async fn step_network(&mut self) -> Result<(), ReadError> {
        let n = self
            .reader
            .read(&mut self.read_buffer[self.read_tail..])
            .await?;

        let updates = match self.on_net_read(n).await {
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
    async fn on_net_read(&mut self, n: usize) -> Result<Vec<tl::enums::Updates>, ReadError> {
        if n == 0 {
            return Err(ReadError::Io(Error::new(
                io::ErrorKind::ConnectionReset,
                "read 0 bytes",
            )));
        }

        self.read_tail += n;

        trace!("read {n} bytes from the network");
        trace!("trying to unpack buffer of {} bytes...", self.read_tail);

        // TODO the buffer might have multiple transport packets, what should happen with the
        // updates successfully read if subsequent packets fail to be deserialized properly?
        let mut updates = Vec::new();
        let mut next_offset = 0;
        let mut mtp = self.m.write().await;
        while next_offset != self.read_tail {
            match self
                .transport
                .unpack(&mut self.read_buffer[next_offset..self.read_tail])
            {
                Ok(offset) => {
                    debug!("deserializing valid transport packet...");
                    let result = mtp.deserialize(
                        &self.read_buffer[next_offset..][offset.data_start..offset.data_end],
                    )?;

                    process_mtp_buffer(result, &mut updates, self.requests.clone()).await;
                    next_offset += offset.next_offset;
                }
                Err(transport::Error::MissingBytes) => break,
                Err(err) => return Err(err.into()),
            }
        }
        drop(mtp);

        self.read_buffer.copy_within(next_offset..self.read_tail, 0);
        self.read_tail -= next_offset;

        Ok(updates)
    }

    /// Handle errors that occurred while performing I/O.
    async fn on_error(&mut self, error: ReadError) -> Result<Vec<Updates>, ReadError> {
        info!("handling error: {error}");
        self.transport.reset();
        self.m.write().await.reset();
        info!(
            "resetting sender state from read_buffer {}/{}",
            self.read_tail,
            self.read_buffer.len(),
        );
        self.read_tail = 0;
        self.read_buffer.fill(0);

        let error = match error {
            ReadError::Io(_) if matches!(self.rp.should_retry(0), ControlFlow::Continue(_)) => {
                match self.try_connect().await {
                    Ok(_) => {
                        // Reconnect success means everything can be retried.
                        self.requests
                            .lock()
                            .await
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

        let mut requests = self.requests.lock().await;
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

    async fn try_connect(&mut self) -> Result<(), Error> {
        let mut attempts = 0;
        loop {
            match NetStream::connect(&self.addr).await.map(|e| e.into_split()) {
                Ok((read, write)) => {
                    info!(
                        "auto-reconnect success after {} failed attempt(s)",
                        attempts
                    );
                    self.reader = read;
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
