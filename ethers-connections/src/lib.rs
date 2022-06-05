pub mod connections {
    //! The umbrella module containing all [`Connection`](crate::Connection)
    //! implementations.

    pub mod batch;

    #[cfg(feature = "http")]
    pub mod http;
    #[cfg(all(unix, feature = "ipc"))]
    pub mod ipc;
    #[cfg(feature = "ws")]
    pub mod ws;

    pub mod noop;
    // pub mod mock;

    #[cfg(any(feature = "ipc", feature = "ws"))]
    mod common;
}

pub mod types;

mod err;
mod jsonrpc;
mod pending;
mod provider;
mod sub;

use std::{future::Future, ops::Deref, pin::Pin};

use jsonrpc::JsonRpcError;
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};

use ethers_core::types::U256;

pub use crate::{
    pending::PendingTransaction,
    provider::{ErrorKind, Provider, ProviderError},
    sub::SubscriptionStream,
};

#[cfg(all(unix, feature = "ipc"))]
pub use crate::connections::ipc::Ipc;

use crate::{err::TransportError, jsonrpc::Request};

#[cfg(target_arch = "wasm32")]
type DynFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
type DynFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// ...
pub type PendingRequest = oneshot::Sender<ResponsePayload>;

/// The future returned by [`Connection::send_raw_request`] that resolves to the
/// JSON value returned by the transport.
pub type RequestFuture<'a> = DynFuture<'a, ResponsePayload>;

/*
IDEA?:
pub struct RequestFuture<'a> {
    pub params: Option<(u64, Box<RawValue>)>,
    response: DynFuture<'a, ResponsePayload>,
}*/

pub type BatchRequestFuture<'a> = DynFuture<'a, Result<(), TransportError>>;

/// The payload of a request response from a transport.
pub type ResponsePayload = Result<Box<RawValue>, TransportError>;

/// A connection allowing the exchange of Ethereum API JSON-RPC messages between
/// a local client and a remote API provider.
pub trait Connection {
    /// Returns a reasonably unique request ID.
    fn request_id(&self) -> u64;

    /// Sends a JSON-RPC request to the underlying API provider and returns its
    /// response.
    ///
    /// The caller has to ensure that `id` is identical to the id encoded in
    /// `request` and that the latter represents a valid JSONRPC 2.0 request
    /// whose contents match the specification defined by the Ethereum
    /// [JSON-RPC API](https://eth.wiki/json-rpc/API).
    fn send_raw_request(&self, id: u64, request: Box<RawValue>) -> RequestFuture<'_>;

    /// TODO...
    fn send_raw_batch_request(
        &self,
        batch: Vec<(u64, PendingRequest)>,
        request: Box<RawValue>,
    ) -> BatchRequestFuture<'_> {
        unimplemented!("only implemented for http, ws and ipc connections")
    }

    fn send_raw_batch_request_alternative(
        &self,
        batch: Vec<u64>,
        request: Box<RawValue>,
    ) -> crate::DynFuture<
        '_,
        Result<Vec<Result<Option<Box<RawValue>>, JsonRpcError>>, TransportError>,
    > {
        unimplemented!("only implemented for http, ws and ipc connections")
    }
}

// blanket impl for all types derefencing to a transport (but not nested refs)
impl<T, D> Connection for D
where
    T: Connection + ?Sized + 'static,
    D: Deref<Target = T>,
{
    fn request_id(&self) -> u64 {
        self.deref().request_id()
    }

    fn send_raw_request(&self, id: u64, request: Box<RawValue>) -> RequestFuture<'_> {
        self.deref().send_raw_request(id, request)
    }

    fn send_raw_batch_request(
        &self,
        batch: Vec<(u64, PendingRequest)>,
        request: Box<RawValue>,
    ) -> BatchRequestFuture<'_> {
        self.deref().send_raw_batch_request(batch, request)
    }
}

/// A trait providing convenience methods for the [`Connection`] trait.
pub trait ConnectionExt: Connection {
    /// Serializes and sends an RPC request for `method` and using `params`.
    ///
    /// In order to match the JSON-RPC specification, `params` must serialize
    /// either to `null` (e.g., with `()`), an array or a map.
    fn send_request<T: Serialize>(&self, method: &str, params: T) -> RequestFuture<'_> {
        let id = self.request_id();
        let request = Request { id, method, params }.to_json();
        self.send_raw_request(id, request)
    }
}

// blanket impl for all `Connection` implementors
impl<T: Connection> ConnectionExt for T {}
// blanket impl for all (dyn) `Connection` trait objects
impl ConnectionExt for dyn Connection + '_ {}

/// The future returned by [`DuplexConnection::subscribe`] that resolves to the
/// ID of the subscription and the channel receiver for all notifications
/// received for this subscription.
pub type SubscribeFuture<'a> = DynFuture<'a, SubscribePayload>;

/// The payload of a response to a subscribe request.
pub type SubscribePayload = Result<Option<NotificationReceiver>, TransportError>;

/// The receiver channel half for subscription notifications.
pub type NotificationReceiver = mpsc::UnboundedReceiver<Box<RawValue>>;

/// A [`Connection`] that allows publish/subscribe communication with the API
/// provider.
pub trait DuplexConnection: Connection {
    /// Subscribes to all notifications received for the given `id` and returns
    /// a [`NotificationReceiver`] for them.
    ///
    /// Additionaly, a RPC call to `eth_subscribe` is necessary, otherwise, no
    /// notifications will be received.
    /// If the ID is already subscribed to, `None` is returned.
    fn subscribe(&self, id: U256) -> SubscribeFuture<'_>;

    /// Unsubscribes to all notifications received for the given `id`.
    ///
    /// A previous RPC call to `eth_unsubscribe` is necessary, otherwise, the
    /// provider will continue to send further notifications for this ID.
    fn unsubscribe(&self, id: U256) -> Result<(), TransportError>;
}

#[cfg(test)]
fn block_on(future: impl Future<Output = ()>) {
    use tokio::runtime::Builder;
    Builder::new_current_thread().enable_all().build().unwrap().block_on(future);
}