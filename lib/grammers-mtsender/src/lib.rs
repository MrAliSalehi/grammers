// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![deny(unsafe_code)]

mod errors;
mod net;
pub mod process_mtp;
mod reconnection;
pub mod sender;
pub mod utils;

pub use crate::reconnection::*;
pub use errors::{AuthorizationError, InvocationError, ReadError, RpcError};
use grammers_mtproto::{
    MsgId,
    mtp::{self},
};
pub use net::ServerAddr;
use std::time::Duration;
use tokio::sync::oneshot;

/// The maximum data that we're willing to send or receive at once.
///
/// By having a fixed-size buffer, we can avoid unnecessary allocations
/// and trivially prevent allocating more than this limit if we ever
/// received invalid data.
///
/// Telegram will close the connection with roughly a megabyte of data,
/// so to account for the transports' own overhead, we add a few extra
/// kilobytes to the maximum data size.
pub const MAXIMUM_DATA: usize = (1024 * 1024) + (8 * 1024);

/// How much leading space should be reserved in a buffer to avoid moving memory.
pub const LEADING_BUFFER_SPACE: usize = mtp::MAX_TRANSPORT_HEADER_LEN
    + mtp::ENCRYPTED_PACKET_HEADER_LEN
    + mtp::PLAIN_PACKET_HEADER_LEN
    + mtp::MESSAGE_CONTAINER_HEADER_LEN;

/// Every how often are pings sent?
pub const PING_DELAY: Duration = Duration::from_secs(20);

/// After how many seconds should the server close the connection when we send a ping?
///
/// What this value essentially means is that we have `NO_PING_DISCONNECT - PING_DELAY` seconds
/// to keep sending pings, or the server will close the connection.
///
/// Pings ensure the connection is kept active, and the delayed disconnect ensures the messages
/// are getting through consistently enough.
pub const NO_PING_DISCONNECT: i32 = 75;

pub struct Request {
    body: Vec<u8>,
    state: RequestState,
    result: oneshot::Sender<Result<Vec<u8>, InvocationError>>,
}

#[derive(Clone, Debug)]
pub struct MsgIdPair {
    msg_id: MsgId,
    container_msg_id: MsgId,
}

#[derive(Debug)]
pub enum RequestState {
    NotSerialized,
    Serialized(MsgIdPair),
    Sent(MsgIdPair),
}

impl MsgIdPair {
    fn new(msg_id: MsgId) -> Self {
        Self {
            msg_id,
            container_msg_id: msg_id, // by default, no container (so the last msg_id is itself)
        }
    }
}
