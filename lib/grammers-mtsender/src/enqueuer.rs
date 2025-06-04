use crate::{InvocationError, Request, RequestState};
use grammers_tl_types::{self as tl, RemoteCall};
use log::debug;
use tokio::sync::{mpsc, oneshot};

pub struct Enqueuer(pub(crate) mpsc::UnboundedSender<Request>);

impl Enqueuer {
    /// Enqueue a Remote Procedure Call to be sent in future calls to `step`.
    pub fn enqueue<R: RemoteCall>(
        &self,
        request: &R,
    ) -> oneshot::Receiver<Result<Vec<u8>, InvocationError>> {
        // TODO we probably want a bound here (to not enqueue more than N at once)
        let body = request.to_bytes();
        assert!(body.len() >= 4);
        let req_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        debug!(
            "enqueueing request {} to be serialized",
            tl::name_for_id(req_id)
        );

        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.0.send(Request {
            body,
            state: RequestState::NotSerialized,
            result: tx,
        }) {
            err.0.result.send(Err(InvocationError::Dropped)).unwrap();
        }
        rx
    }
}
