use crate::net::network_reader::NetworkReader;
use crate::net::network_writer::NetworkWriter;
pub use crate::{
    LEADING_BUFFER_SPACE, MAXIMUM_DATA, MsgIdPair, NO_PING_DISCONNECT, PING_DELAY, Request,
    RequestState,
    errors::{AuthorizationError, InvocationError, ReadError, RpcError},
    net::{NetStream, ServerAddr},
    reconnection::*,
    utils::{sleep, sleep_until},
};
use grammers_mtproto::{
    authentication,
    mtp::{self, Mtp},
    transport::Transport,
};
use grammers_tl_types::{self as tl, RemoteCall};
use log::{debug, info, trace};
use std::{io::Error, sync::Arc};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub type Requests = Arc<Mutex<Vec<Request>>>;

/// Manages enqueuing requests, matching them to their response, and IO.
pub struct Sender {
    mtp: Arc<RwLock<Box<dyn Mtp>>>,
    update_rx: Arc<Mutex<broadcast::Receiver<Vec<tl::enums::Updates>>>>,
    handles: Arc<Vec<JoinHandle<()>>>,
    request_tx: UnboundedSender<Request>,
}

impl Sender {
    async fn connect<T: Transport, M: Mtp>(
        transport: T,
        mtp: M,
        addr: ServerAddr,
        rp: &'static dyn ReconnectionPolicy,
    ) -> Result<Self, Error> {
        let stream = NetStream::connect(&addr).await?;
        let (reader, write) = stream.into_split();
        let (request_tx, request_rx) = mpsc::unbounded_channel::<Request>();

        let (update_tx, update_rx) = broadcast::channel::<Vec<tl::enums::Updates>>(5);

        //re-connection
        //the writer task should check the connection_rx in case of write failure to see if a new socket writer is available
        let (connection_tx, connection_rx) = broadcast::channel::<Arc<OwnedWriteHalf>>(1);

        let m: Arc<RwLock<Box<dyn Mtp>>> = Arc::new(RwLock::new(Box::new(mtp)));

        let requests = Arc::new(Mutex::new(Vec::<Request>::new()));

        let transport: Arc<Box<dyn Transport>> = Arc::new(Box::new(transport));

        let read_handle = NetworkReader::new(
            transport.clone(),
            m.clone(),
            requests.clone(),
            reader,
            update_tx,
            addr,
            rp,
            connection_tx,
        )
        .spawn();

        let write_handle = NetworkWriter::new(
            transport,
            m.clone(),
            requests,
            connection_rx,
            write,
            request_rx,
        )
        .spawn();

        Ok(Self {
            update_rx: Arc::new(Mutex::new(update_rx)),
            request_tx,
            handles: Arc::new(vec![write_handle, read_handle]),
            mtp: m.clone(),
        })
    }

    pub async fn invoke<R: RemoteCall>(&self, request: &R) -> Result<Vec<u8>, InvocationError> {
        let rx = self.enqueue_body(request.to_bytes());
        self.step_until_receive(rx).await
    }

    /// Like `invoke` but raw data.
    async fn send(&self, body: Vec<u8>) -> Result<Vec<u8>, InvocationError> {
        let rx = self.enqueue_body(body);
        self.step_until_receive(rx).await
    }

    fn enqueue_body(&self, body: Vec<u8>) -> oneshot::Receiver<Result<Vec<u8>, InvocationError>> {
        assert!(body.len() >= 4);
        let req_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        debug!(
            "enqueueing request {} to be serialized",
            tl::name_for_id(req_id)
        );

        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(Request {
                body,
                state: RequestState::NotSerialized,
                result: tx,
            })
            .unwrap();
        rx
    }

    /// Enqueue a Remote Procedure Call to be sent in future calls to `step`.
    pub fn enqueue<R: RemoteCall>(
        &self,
        request: &R,
    ) -> oneshot::Receiver<Result<Vec<u8>, InvocationError>> {
        // TODO we probably want a bound here (to not enqueue more than N at once)
        let body = request.to_bytes();
        self.enqueue_body(body)
    }

    async fn step_until_receive(
        &self,
        rx: oneshot::Receiver<Result<Vec<u8>, InvocationError>>,
    ) -> Result<Vec<u8>, InvocationError> {
        info!("waiting for result of request");
        match rx.await {
            Ok(ok) => ok,
            _ => {
                panic!("request channel dropped before receiving a result")
            }
        }
    }

    pub async fn next_updates(&self) -> Result<Vec<tl::enums::Updates>, ReadError> {
        self.update_rx
            .lock()
            .await
            .recv()
            .await
            .map_err(|_| ReadError::RxClosed)
    }

    pub async fn auth_key(&self) -> [u8; 256] {
        self.mtp.read().await.auth_key()
    }

    /*
      /// Handle a ping timeout, meaning we need to enqueue a new ping request.
      fn on_ping_timeout(&mut self) {
          let ping_id = generate_random_id();
          debug!("enqueueing keepalive ping {}", ping_id);
          drop(
              self.enqueue_body(
                  tl::functions::PingDelayDisconnect {
                      ping_id,
                      disconnect_delay: NO_PING_DISCONNECT,
                  }
                  .to_bytes(),
              ),
          );
          self.next_ping = Instant::now() + PING_DELAY;
      }

    */
}

pub async fn connect<T: Transport>(
    transport: T,
    addr: ServerAddr,
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<Sender, AuthorizationError> {
    let sender = Sender::connect(transport, mtp::Plain::new(), addr, rc_policy).await?;
    generate_auth_key::<T>(sender).await
}

pub async fn connect_with_auth<T: Transport>(
    transport: T,
    addr: ServerAddr,
    auth_key: [u8; 256],
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<Sender, Error> {
    Sender::connect(
        transport,
        mtp::Encrypted::build().finish(auth_key),
        addr,
        rc_policy,
    )
    .await
}

async fn generate_auth_key<T: Transport>(sender: Sender) -> Result<Sender, AuthorizationError> {
    info!("generating new authorization key...");
    let (request, data) = authentication::step1()?;
    debug!("gen auth key: sending step 1");
    let response = sender.send(request).await?;
    debug!("gen auth key: starting step 2");
    let (request, data) = authentication::step2(data, &response)?;
    debug!("gen auth key: sending step 2");
    let response = sender.send(request).await?;
    debug!("gen auth key: starting step 3");
    let (request, data) = authentication::step3(data, &response)?;
    debug!("gen auth key: sending step 3");
    let response = sender.send(request).await?;
    debug!("gen auth key: completing generation");
    let authentication::Finished {
        auth_key,
        time_offset,
        first_salt,
    } = authentication::create_key(data, &response)?;
    info!("authorization key generated successfully");

    *sender.mtp.write().await = Box::new(
        mtp::Encrypted::build()
            .time_offset(time_offset)
            .first_salt(first_salt)
            .finish(auth_key),
    );

    Ok(sender)
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.handles.iter().for_each(|h| {
            h.abort();
            trace!("aborting rw task {}", h.id());
        });
    }
}
