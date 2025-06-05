use crate::generate_random_id;
pub use crate::{
    LEADING_BUFFER_SPACE, MAXIMUM_DATA, MsgIdPair, NO_PING_DISCONNECT, PING_DELAY, Request,
    RequestState,
    enqueuer::Enqueuer,
    errors::{AuthorizationError, InvocationError, ReadError, RpcError},
    net::{NetStream, ServerAddr},
    reconnection::*,
    utils::{sleep, sleep_until},
};
use futures_util::future::{Either, pending, select};
use grammers_crypto::DequeBuffer;
use grammers_mtproto::transport::Intermediate;
use grammers_mtproto::{
    MsgId, authentication,
    mtp::{
        self, BadMessage, Deserialization, DeserializationFailure, Mtp, RpcResult, RpcResultError,
    },
    transport::{self, Transport},
};
use grammers_tl_types::{self as tl, Deserializable, RemoteCall};
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use std::ops::Deref;
use std::sync::Arc;
use std::{io, io::Error, ops::ControlFlow, pin::pin, time::Duration};
use tl::Serializable;
use tokio::task::JoinHandle;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, oneshot::error::TryRecvError},
};
use web_time::Instant;

/// Manages enqueuing requests, matching them to their response, and IO.
pub struct Sender<T: Transport, M: Mtp> {
    stream: NetStream,
    transport: T,
    mtp: M,
    addr: ServerAddr,
    requests: Vec<Request>,
    request_rx: mpsc::UnboundedReceiver<Request>,
    next_ping: Instant,
    reconnection_policy: &'static dyn ReconnectionPolicy,

    // Transport-level buffers and positions
    read_buffer: Vec<u8>,
    read_tail: usize,
    write_buffer: DequeBuffer<u8>,
    write_head: usize,
    handles: Vec<JoinHandle<()>>,
}

impl<T: Transport + Send + Sync + 'static, M: Mtp + Send + Sync + 'static> Sender<T, M> {
    async fn connect(
        transport: T,
        mtp: M,
        addr: ServerAddr,
        reconnection_policy: &'static dyn ReconnectionPolicy,
    ) -> Result<(Self, Enqueuer), Error> {
        let stream = NetStream::connect(&addr).await?;
        let (mut reader, write) = stream.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Request>();

        /*let slf = Self {
            stream,
            transport,
            mtp,
            addr,
            requests: vec![],
            request_rx: rx,
            next_ping: Instant::now() + PING_DELAY,
            reconnection_policy,
            handles: vec![],
            read_buffer: vec![0; MAXIMUM_DATA],
            read_tail: 0,
            write_buffer: DequeBuffer::with_capacity(MAXIMUM_DATA, LEADING_BUFFER_SPACE),
            write_head: 0,
        };*/

        let t = transport.clone();
        let m = Arc::new(RwLock::new(mtp));

        let read_handle = tokio::spawn(async move {
            let transport = t.clone();
            let mtp = m.clone();
            let mut read_tail = 0;
            let mut read_buffer = vec![0; MAXIMUM_DATA];
            let Ok(n) = reader
                .read(&mut read_buffer[read_tail..])
                .await
                .inspect_err(|e| error!("read error: {e}"))
            else {
                return;
            };
            //what to do with the updates
            let result = on_net_read(&mut read_tail, &mut read_buffer, &transport, mtp.clone(), n);
        });

        /*Ok((slf, Enqueuer(tx)))*/
        todo!()
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
            self.step().await?;
            match rx.try_recv() {
                Ok(x) => break x,
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Closed) => {
                    panic!("request channel dropped before receiving a result")
                }
            }
        }
    }

    /// Step network events, writing and reading at the same time.
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
            Sel::Read(n) => n.map_err(ReadError::Io).and_then(|n| /*self.on_net_read(n)*/ todo!()),
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
    async fn try_connect(&mut self) -> Result<(), Error> {
        let mut attempts = 0;
        loop {
            match NetStream::connect(&self.addr).await {
                Ok(result) => {
                    info!(
                        "auto-reconnect success after {} failed attempt(s)",
                        attempts
                    );
                    self.stream = result;
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    warn!("auto-reconnect failed {} time(s): {}", attempts, e);
                    sleep(Duration::from_secs(1)).await;

                    match self.reconnection_policy.should_retry(attempts) {
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

    /// Handle errors that occured while performing I/O.
    async fn on_error(&mut self, error: ReadError) -> Result<Vec<tl::enums::Updates>, ReadError> {
        info!("handling error: {error}");
        self.transport.reset();
        self.mtp.reset();
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
                        self.requests
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

        warn!(
            "marking all {} request(s) as failed: {}",
            self.requests.len(),
            &error
        );

        self.requests
            .drain(..)
            .for_each(|r| drop(r.result.send(Err(InvocationError::from(error.clone())))));

        Err(error)
    }

    /// Process the result of deserializing an MTP buffer.
    fn process_mtp_buffer(
        &mut self,
        results: Vec<Deserialization>,
        updates: &mut Vec<tl::enums::Updates>,
    ) {
        for result in results {
            match result {
                Deserialization::Update(update) => self.process_update(updates, update),
                Deserialization::RpcResult(result) => self.process_result(result),
                Deserialization::RpcError(error) => self.process_error(error),
                Deserialization::BadMessage(bad_msg) => self.process_bad_message(bad_msg),
                Deserialization::Failure(failure) => self.process_deserialize_error(failure),
            }
        }
    }

    fn process_update(&mut self, updates: &mut Vec<tl::enums::Updates>, update: Vec<u8>) {
        let update = match tl::enums::Updates::from_bytes(&update) {
            Ok(u) => Some(u),
            Err(e) => {
                // Annoyingly enough, `messages.affectedMessages` also has `pts`.
                // Mostly received when deleting messages, so pretend that's the
                // update that actually occured.
                match tl::enums::messages::AffectedMessages::from_bytes(&update) {
                    Ok(tl::enums::messages::AffectedMessages::Messages(
                        tl::types::messages::AffectedMessages { pts, pts_count },
                    )) => Some(
                        tl::types::UpdateShort {
                            update: tl::types::UpdateDeleteMessages {
                                messages: Vec::new(),
                                pts,
                                pts_count,
                            }
                            .into(),
                            date: 0,
                        }
                        .into(),
                    ),
                    Err(_) => match tl::types::messages::InvitedUsers::from_bytes(&update) {
                        Ok(u) => Some(u.updates),
                        Err(_) => {
                            warn!(
                                "telegram sent updates that failed to be deserialized: {}",
                                e
                            );
                            None
                        }
                    },
                }
            }
        };

        if let Some(update) = update {
            updates.push(update);
        }
    }

    fn process_result(&mut self, result: RpcResult) {
        if let Some(req) = self.pop_request(result.msg_id) {
            let x = result.body;
            assert!(x.len() >= 4);
            let res_id = u32::from_le_bytes([x[0], x[1], x[2], x[3]]);
            debug!(
                "got result {:x} ({}) for request {:?}",
                res_id,
                tl::name_for_id(res_id),
                result.msg_id
            );
            drop(req.result.send(Ok(x)));
        } else {
            info!(
                "got rpc result {:?} but no such request is saved",
                result.msg_id
            );
        }
    }

    fn process_error(&mut self, error: RpcResultError) {
        if let Some(req) = self.pop_request(error.msg_id) {
            debug!("got rpc error {:?}", error.error);
            let x = req.body.as_slice();
            drop(
                req.result.send(Err(InvocationError::Rpc(
                    RpcError::from(error.error)
                        .with_caused_by(u32::from_le_bytes([x[0], x[1], x[2], x[3]])),
                ))),
            );
        } else {
            info!(
                "got rpc error {:?} but no such request is saved",
                error.msg_id
            );
        }
    }

    fn process_bad_message(&mut self, bad_msg: BadMessage) {
        for i in (0..self.requests.len()).rev() {
            match &self.requests[i].state {
                RequestState::Serialized(pair)
                    if pair.msg_id == bad_msg.msg_id || pair.container_msg_id == bad_msg.msg_id =>
                {
                    panic!(
                        "bad msg for unsent request {:?}: {}",
                        bad_msg.msg_id,
                        bad_msg.description()
                    );
                }
                RequestState::Sent(pair)
                    if pair.msg_id == bad_msg.msg_id || pair.container_msg_id == bad_msg.msg_id =>
                {
                    // TODO add a test to make sure we resend the request
                    if bad_msg.retryable() {
                        info!(
                            "{}; re-sending request {:?}",
                            bad_msg.description(),
                            pair.msg_id
                        );

                        // TODO check if actually retryable first!
                        self.requests[i].state = RequestState::NotSerialized;
                    } else {
                        if bad_msg.fatal() {
                            error!(
                                "{}; canont retry request {:?}",
                                bad_msg.description(),
                                pair.msg_id
                            );
                        } else {
                            warn!(
                                "{}; canont retry request {:?}",
                                bad_msg.description(),
                                pair.msg_id
                            );
                        }
                        let req = self.requests.swap_remove(i);
                        drop(req.result.send(Err(InvocationError::Dropped)));
                    }
                }
                _ => {}
            }
        }
    }

    fn process_deserialize_error(&mut self, failure: DeserializationFailure) {
        if let Some(req) = self.pop_request(failure.msg_id) {
            debug!("got deserialization failure {:?}", failure.error);
            drop(
                req.result
                    .send(Err(InvocationError::Read(failure.error.into()))),
            );
        } else {
            info!(
                "got deserialization failure {:?} but no such request is saved",
                failure.error
            );
        }
    }

    fn pop_request(&mut self, msg_id: MsgId) -> Option<Request> {
        for i in 0..self.requests.len() {
            match &self.requests[i].state {
                RequestState::Serialized(pair) if pair.msg_id == msg_id => {
                    panic!("got response {msg_id:?} for unsent request {pair:?}");
                }
                RequestState::Sent(pair) if pair.msg_id == msg_id => {
                    return Some(self.requests.swap_remove(i));
                }
                _ => {}
            }
        }

        None
    }
}

/// Handle `n` more read bytes being ready to process by the transport.
///
/// This won't cause `ReadError::Io`, but yet another enum would be overkill.
fn on_net_read<T: Transport, M: Mtp>(
    read_tail: &mut usize,
    read_buffer: &mut Vec<u8>,
    t: &T,
    m: Arc<RwLock<M>>,
    n: usize,
) -> Result<Vec<tl::enums::Updates>, ReadError> {
    if n == 0 {
        return Err(ReadError::Io(Error::new(
            io::ErrorKind::ConnectionReset,
            "read 0 bytes",
        )));
    }

    *read_tail += n;
    trace!("read {} bytes from the network", n);
    trace!("trying to unpack buffer of {} bytes...", read_tail);

    // TODO the buffer might have multiple transport packets, what should happen with the
    // updates successfully read if subsequent packets fail to be deserialized properly?
    let mut updates = Vec::new();
    let mut next_offset = 0;
    while next_offset != *read_tail {
        match t.unpack(&mut read_buffer[next_offset..*read_tail]) {
            Ok(offset) => {
                debug!("deserializing valid transport packet...");
                let result = m
                    .write()
                    .deserialize(&read_buffer[next_offset..][offset.data_start..offset.data_end])?;

                //todo fix the process
                //Sender::process_mtp_buffer(result, &mut updates);
                next_offset += offset.next_offset;
            }
            Err(transport::Error::MissingBytes) => break,
            Err(err) => return Err(err.into()),
        }
    }

    read_buffer.copy_within(next_offset..*read_tail, 0);
    *read_tail -= next_offset;

    Ok(updates)
}

impl<T: Transport> Sender<T, mtp::Encrypted> {
    pub fn auth_key(&self) -> [u8; 256] {
        self.mtp.auth_key()
    }
}

pub async fn connect<T: Transport + Send + Sync + 'static>(
    transport: T,
    addr: ServerAddr,
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<(Sender<T, mtp::Encrypted>, Enqueuer), AuthorizationError> {
    let (sender, enqueuer) = Sender::connect(transport, mtp::Plain::new(), addr, rc_policy).await?;
    generate_auth_key(sender, enqueuer).await
}

pub async fn connect_with_auth<T: Transport + Send + Sync + 'static>(
    transport: T,
    addr: ServerAddr,
    auth_key: [u8; 256],
    rc_policy: &'static dyn ReconnectionPolicy,
) -> Result<(Sender<T, mtp::Encrypted>, Enqueuer), Error> {
    Sender::connect(
        transport,
        mtp::Encrypted::build().finish(auth_key),
        addr,
        rc_policy,
    )
    .await
}

pub async fn generate_auth_key<T: Transport + Send + Sync + 'static>(
    mut sender: Sender<T, mtp::Plain>,
    enqueuer: Enqueuer,
) -> Result<(Sender<T, mtp::Encrypted>, Enqueuer), AuthorizationError> {
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
            mtp: mtp::Encrypted::build()
                .time_offset(time_offset)
                .first_salt(first_salt)
                .finish(auth_key),
            next_ping: Instant::now() + PING_DELAY,
            stream: sender.stream,
            transport: sender.transport,
            requests: sender.requests,
            request_rx: sender.request_rx,
            read_buffer: sender.read_buffer,
            read_tail: sender.read_tail,
            write_buffer: sender.write_buffer,
            write_head: sender.write_head,
            addr: sender.addr,
            reconnection_policy: sender.reconnection_policy,
            handles: sender.handles,
        },
        enqueuer,
    ))
}
