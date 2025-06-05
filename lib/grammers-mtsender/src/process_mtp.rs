use crate::sender::Requests;
use crate::{InvocationError, Request, RequestState, RpcError};
use grammers_mtproto::MsgId;
use grammers_mtproto::mtp::{
    BadMessage, Deserialization, DeserializationFailure, RpcResult, RpcResultError,
};
use grammers_tl_types as tl;
use grammers_tl_types::Deserializable;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::sync::Arc;

/// Process the result of deserializing an MTP buffer.
pub fn process_mtp_buffer(
    results: Vec<Deserialization>,
    updates: &mut Vec<tl::enums::Updates>,
    requests: Arc<Mutex<Vec<Request>>>,
) {
    for result in results {
        match result {
            Deserialization::Update(update) => process_update(updates, update),
            Deserialization::RpcResult(result) => process_result(requests.clone(), result),
            Deserialization::RpcError(error) => process_error(requests.clone(), error),
            Deserialization::BadMessage(bad_msg) => process_bad_message(requests.clone(), bad_msg),
            Deserialization::Failure(failure) => {
                process_deserialize_error(requests.clone(), failure)
            }
        }
    }
}

fn process_update(updates: &mut Vec<tl::enums::Updates>, update: Vec<u8>) {
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

fn process_result(requests: Requests, result: RpcResult) {
    if let Some(req) = pop_request(requests, result.msg_id) {
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

fn process_error(requests: Requests, error: RpcResultError) {
    if let Some(req) = pop_request(requests, error.msg_id) {
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

fn process_bad_message(requests: Requests, bad_msg: BadMessage) {
    let mut requests = requests.lock();
    for i in (0..requests.len()).rev() {
        match &requests[i].state {
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
                    requests[i].state = RequestState::NotSerialized;
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
                    let req = requests.swap_remove(i);
                    drop(req.result.send(Err(InvocationError::Dropped)));
                }
            }
            _ => {}
        }
    }
}

fn process_deserialize_error(requests: Requests, failure: DeserializationFailure) {
    if let Some(req) = pop_request(requests, failure.msg_id) {
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

fn pop_request(requests: Requests, msg_id: MsgId) -> Option<Request> {
    let mut requests = requests.lock();
    for i in 0..requests.len() {
        match &requests[i].state {
            RequestState::Serialized(pair) if pair.msg_id == msg_id => {
                panic!("got response {msg_id:?} for unsent request {pair:?}");
            }
            RequestState::Sent(pair) if pair.msg_id == msg_id => {
                return Some(requests.swap_remove(i));
            }
            _ => {}
        }
    }
    None
}
