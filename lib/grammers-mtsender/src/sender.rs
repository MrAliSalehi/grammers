use crate::net::network_reader::NetworkReader;
use crate::process_mtp::process_mtp_buffer;
pub use crate::{
    LEADING_BUFFER_SPACE, MAXIMUM_DATA, MsgIdPair, NO_PING_DISCONNECT, PING_DELAY, Request,
    RequestState,
    enqueuer::Enqueuer,
    errors::{AuthorizationError, InvocationError, ReadError, RpcError},
    net::{NetStream, ServerAddr},
    reconnection::*,
    utils::{sleep, sleep_until},
};
use grammers_crypto::DequeBuffer;
use grammers_mtproto::{
    authentication,
    mtp::{self, Mtp},
    transport::{self, Transport},
};
use grammers_tl_types::{self as tl, RemoteCall};
use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, RwLock};
use std::ops::ControlFlow;
use std::{io, io::Error, sync::Arc};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, oneshot, oneshot::error::TryRecvError},
};

pub type Requests = Arc<Mutex<Vec<Request>>>;

/// Manages enqueuing requests, matching them to their response, and IO.
pub struct Sender {
    mtp: Arc<RwLock<Box<dyn Mtp>>>,
    requests: Vec<Request>,
    request_rx: mpsc::UnboundedReceiver<Request>,
    write_buffer: DequeBuffer<u8>,
    write_head: usize,
    update_rx: broadcast::Receiver<Vec<tl::enums::Updates>>,
}

impl Sender {
    async fn connect<T: Transport, M: Mtp>(
        transport: T,
        mtp: M,
        addr: ServerAddr,
        rp: &'static dyn ReconnectionPolicy,
    ) -> Result<(Self, Enqueuer), Error> {
        let stream = NetStream::connect(&addr).await?;
        let (reader, write) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel::<Request>();

        let (update_tx, update_rx) = broadcast::channel::<Vec<tl::enums::Updates>>(5);

        //re-connection
        //the writer task should check the connection_rx in case of write failure to see if a new socket writer is available
        let (connection_tx, connection_rx) = broadcast::channel::<Arc<OwnedWriteHalf>>(1);

        let m: Arc<RwLock<Box<dyn Mtp>>> = Arc::new(RwLock::new(Box::new(mtp)));

        let requests = Arc::new(Mutex::new(Vec::<Request>::new()));

        let nw_reader = NetworkReader::spawn_new(
            transport.clone(),
            m.clone(),
            requests.clone(),
            reader,
            update_tx,
            addr,
            rp,
            connection_tx,
        );

        let slf = Self {
            update_rx,
            mtp: m.clone(),
            requests: vec![],
            request_rx: rx,
            write_buffer: DequeBuffer::with_capacity(MAXIMUM_DATA, LEADING_BUFFER_SPACE),
            write_head: 0,
        };
        Ok((slf, Enqueuer(tx)))
    }

    pub async fn invoke<R: RemoteCall>(&mut self, request: &R) -> Result<Vec<u8>, InvocationError> {
        let rx = self.enqueue_body(request.to_bytes());
        self.step_until_receive(rx).await
    }

    /// Like `invoke` but raw data.
    async fn send(&mut self, body: Vec<u8>) -> Result<Vec<u8>, InvocationError> {
        let rx = self.enqueue_body(body);
        self.step_until_receive(rx).await
    }

    fn enqueue_body(
        &mut self,
        body: Vec<u8>,
    ) -> oneshot::Receiver<Result<Vec<u8>, InvocationError>> {
        assert!(body.len() >= 4);
        let req_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        debug!(
            "enqueueing request {} to be serialized",
            tl::name_for_id(req_id)
        );

        let (tx, rx) = oneshot::channel();
        self.requests.push(Request {
            body,
            state: RequestState::NotSerialized,
            result: tx,
        });
        rx
    }

    async fn step_until_receive(
        &mut self,
        mut rx: oneshot::Receiver<Result<Vec<u8>, InvocationError>>,
    ) -> Result<Vec<u8>, InvocationError> {
        loop {
            match rx.try_recv() {
                Ok(x) => break x,
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Closed) => {
                    panic!("request channel dropped before receiving a result")
                }
            }
        }
    }

    pub async fn next_updates(&mut self) -> Result<Vec<tl::enums::Updates>, ReadError> {
        self.update_rx.recv().await.map_err(|_| ReadError::RxClosed)
    }

    pub fn auth_key(&self) -> [u8; 256] {
        self.mtp.read().auth_key()
    }

    /*    /// Step network events, writing and reading at the same time.
      ///
      /// Updates received during this step, if any, are returned.
      pub async fn step(&mut self) -> Result<Vec<tl::enums::Updates>, ReadError> {
          enum Sel {
              Sleep,
              Request(Option<Request>),
              Read(io::Result<usize>),
              Write(io::Result<usize>),
          }

          self.try_fill_write();
          let write_len = self.write_buffer.len() - self.write_head;
          trace!(
              "reading bytes and sending up to {} bytes via network",
              write_len
          );

          let (mut reader, mut writer) = self.stream.split();
          let sel = {
              let sleep = pin!(async { sleep_until(self.next_ping).await });
              let recv_req = pin!(async { self.request_rx.recv().await });
              let recv_data =
                  pin!(async { reader.read(&mut self.read_buffer[self.read_tail..]).await });
              let send_data = pin!(async {
                  if self.write_buffer.is_empty() {
                      pending().await
                  } else {
                      writer.write(&self.write_buffer[self.write_head..]).await
                  }
              });

              match select(select(sleep, recv_req), select(recv_data, send_data)).await {
                  Either::Left((Either::Left(_), _)) => Sel::Sleep,
                  Either::Left((Either::Right((request, _)), _)) => Sel::Request(request),
                  Either::Right((Either::Left((n, _)), _)) => Sel::Read(n),
                  Either::Right((Either::Right((n, _)), _)) => Sel::Write(n),
              }
          };

          let res = match sel {
              Sel::Request(request) => {
                  self.requests.push(request.unwrap());
                  Ok(Vec::new())
              }
              Sel::Read(n) => n
                  .map_err(ReadError::Io)
                  .and_then(|n| /*self.on_net_read(n)*/ todo!()),
              Sel::Write(n) => n.map_err(ReadError::Io).map(|n| {
                  self.on_net_write(n);
                  Vec::new()
              }),
              Sel::Sleep => {
                  self.on_ping_timeout();
                  Ok(Vec::new())
              }
          };

          match res {
              Ok(ok) => Ok(ok),
              Err(err) => self.on_error(err).await,
          }
      }

      #[allow(unused_variables)]


      /// Setup the write buffer for the transport, unless a write is already pending.
      fn try_fill_write(&mut self) {
          if !self.write_buffer.is_empty() {
              return;
          }

          // TODO add a test to make sure we only ever send the same request once
          for request in self
              .requests
              .iter_mut()
              .filter(|r| matches!(r.state, RequestState::NotSerialized))
          {
              // TODO make mtp itself use BytesMut to avoid copies
              if let Some(msg_id) = self.mtp.push(&mut self.write_buffer, &request.body) {
                  assert!(request.body.len() >= 4);
                  let req_id = u32::from_le_bytes([
                      request.body[0],
                      request.body[1],
                      request.body[2],
                      request.body[3],
                  ]);
                  debug!(
                      "serialized request {:x} ({}) with {:?}",
                      req_id,
                      tl::name_for_id(req_id),
                      msg_id
                  );
                  // Note how only NotSerialized become Serialized.
                  // Nasty bugs that take ~2h to find occur otherwise!
                  // (e.g. infinite loops leading to transport flood.)
                  request.state = RequestState::Serialized(MsgIdPair::new(msg_id));
              } else {
                  break;
              }
          }

          if let Some(container_msg_id) = self.mtp.finalize(&mut self.write_buffer) {
              for request in self.requests.iter_mut() {
                  match request.state {
                      RequestState::Serialized(ref mut pair) => {
                          pair.container_msg_id = container_msg_id;
                      }
                      RequestState::NotSerialized | RequestState::Sent(..) => {}
                  }
              }
              self.transport.pack(&mut self.write_buffer)
          }
      }

      /// Handle `n` more written bytes being ready to process by the transport.
      fn on_net_write(&mut self, n: usize) {
          self.write_head += n;
          trace!(
              "written {} bytes to the network ({}/{})",
              n,
              self.write_head,
              self.write_buffer.len()
          );
          assert!(self.write_head <= self.write_buffer.len());
          if self.write_head != self.write_buffer.len() {
              return;
          }

          self.write_buffer.clear();
          self.write_head = 0;
          for req in self.requests.iter_mut() {
              match &req.state {
                  RequestState::NotSerialized | RequestState::Sent(_) => {}
                  RequestState::Serialized(pair) => {
                      debug!("sent request with {:?}", pair);
                      req.state = RequestState::Sent(pair.clone());
                  }
              }
          }
      }

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

//todo on_error for writer task
/// Handle errors that occured while performing I/O.
/*async fn on_error<T: Transport>(
    t: T,
    m: Arc<Mutex<RwLock<Box<dyn Mtp>>>>,
    requests: Requests,
    error: ReadError,
) -> Result<Vec<tl::enums::Updates>, ReadError> {
    info!("handling error: {error}");
    t.reset();
    m.lock().write().reset();
    info!(
        "resetting sender state from read_buffer {}/{}, write_buffer {}/{}",
        self.read_tail,
        self.read_buffer.len(),
        self.write_head,
        self.write_buffer.len(),
    );
    self.read_tail = 0;
    self.read_buffer.fill(0);
    self.write_head = 0;
    self.write_buffer.clear();

    let error = match error {
        ReadError::Io(_)
            if matches!(
                self.reconnection_policy.should_retry(0),
                ControlFlow::Continue(_)
            ) =>
        {
            match self.try_connect().await {
                Ok(_) => {
                    // Reconnect success means everything can be retried.
                    requests.lock()
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

    //fixme lock just for len?
    warn!(
        "marking all {} request(s) as failed: {}",
        requests.lock().len(),
        &error
    );

    requests.lock()
        .drain(..)
        .for_each(|r| drop(r.result.send(Err(InvocationError::from(error.clone())))));

    Err(error)
}*/

pub async fn connect<T: Transport>(
    transport: T,
    addr: ServerAddr,
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<(Sender, Enqueuer), AuthorizationError> {
    let (sender, enqueuer) = Sender::connect(transport, mtp::Plain::new(), addr, rc_policy).await?;
    generate_auth_key::<T>(sender, enqueuer).await
}

pub async fn connect_with_auth<T: Transport>(
    transport: T,
    addr: ServerAddr,
    auth_key: [u8; 256],
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<(Sender, Enqueuer), Error> {
    Sender::connect(
        transport,
        mtp::Encrypted::build().finish(auth_key),
        addr,
        rc_policy,
    )
    .await
}

async fn generate_auth_key<T: Transport>(
    mut sender: Sender,
    enqueuer: Enqueuer,
) -> Result<(Sender, Enqueuer), AuthorizationError> {
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

    Ok((
        Sender {
            mtp: Arc::new(RwLock::new(Box::new(
                mtp::Encrypted::build()
                    .time_offset(time_offset)
                    .first_salt(first_salt)
                    .finish(auth_key),
            ))),
            update_rx: sender.update_rx,
            requests: sender.requests,
            request_rx: sender.request_rx,
            write_buffer: sender.write_buffer,
            write_head: sender.write_head,
        },
        enqueuer,
    ))
}
