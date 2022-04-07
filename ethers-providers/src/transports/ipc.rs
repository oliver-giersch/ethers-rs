use std::{
    cell::RefCell,
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_channel::mpsc;
use futures_util::stream::StreamExt;
use oneshot::error::RecvError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{value::RawValue, Deserializer};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        unix::{OwnedReadHalf, OwnedWriteHalf},
        UnixStream,
    },
    sync::oneshot,
};
use tracing::{error, warn};

use ethers_core::types::U256;

use crate::{
    provider::ProviderError,
    transports::common::{JsonRpcError, Notification, Request, Response},
    JsonRpcClient, PubsubClient,
};

type Pending = oneshot::Sender<Result<Box<RawValue>, IpcError>>;
type Subscription = mpsc::UnboundedSender<Box<RawValue>>;

#[derive(Debug)]
enum TransportMessage {
    Request { id: u64, request: Box<[u8]>, sender: Pending },
    Subscribe { id: U256, sink: Subscription },
    Unsubscribe { id: U256 },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Incoming<'a> {
    #[serde(borrow)]
    Notification(Notification<'a>),
    #[serde(borrow)]
    Response(Response<'a>),
}

/// Unix Domain Sockets (IPC) transport.
#[derive(Debug, Clone)]
pub struct Ipc {
    id: Arc<AtomicU64>,
    messages_tx: mpsc::UnboundedSender<TransportMessage>,
}

#[cfg(unix)]
impl Ipc {
    /// Creates a new IPC transport from a given path using Unix sockets
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, IpcError> {
        let ipc = UnixStream::connect(path).await?;
        let id = Arc::new(AtomicU64::new(1));

        let (messages_tx, messages_rx) = mpsc::unbounded();
        spawn_ipc_server(ipc, messages_rx);

        Ok(Self { id, messages_tx })
    }

    fn send(&self, msg: TransportMessage) -> Result<(), IpcError> {
        self.messages_tx
            .unbounded_send(msg)
            .map_err(|_| IpcError::ChannelError("IPC server receiver dropped".to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl JsonRpcClient for Ipc {
    type Error = IpcError;

    async fn request<T: Serialize + Send + Sync, R: DeserializeOwned>(
        &self,
        method: &str,
        params: T,
    ) -> Result<R, IpcError> {
        let next_id = self.id.fetch_add(1, Ordering::SeqCst);

        // Create the request and initialize the response channel
        let (sender, receiver) = oneshot::channel();
        let payload = TransportMessage::Request {
            id: next_id,
            request: serde_json::to_string(&Request::new(next_id, method, params))?
                .into_bytes()
                .into_boxed_slice(),
            sender,
        };

        // Send the request to the IPC server to be handled.
        self.send(payload)?;

        // Wait for the response from the IPC server.
        let result = receiver.await?;
        // In case the response was an error, return it to the caller
        let response = result?;

        // Parse the JSON result to the expected type.
        Ok(serde_json::from_str(response.get())?)
    }
}

impl PubsubClient for Ipc {
    type NotificationStream = mpsc::UnboundedReceiver<Box<RawValue>>;

    fn subscribe<T: Into<U256>>(&self, id: T) -> Result<Self::NotificationStream, IpcError> {
        let (sink, stream) = mpsc::unbounded();
        self.send(TransportMessage::Subscribe { id: id.into(), sink })?;
        Ok(stream)
    }

    fn unsubscribe<T: Into<U256>>(&self, id: T) -> Result<(), IpcError> {
        self.send(TransportMessage::Unsubscribe { id: id.into() })
    }
}

fn spawn_ipc_server(ipc: UnixStream, requests: mpsc::UnboundedReceiver<TransportMessage>) {
    // IDEA: spawn a thread with a thread-local runtime?
    let _ = tokio::task::spawn_blocking(|| {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async move {
            let (reader, writer) = ipc.into_split();
            let shared = Shared { pending: Default::default(), subscriptions: Default::default() };

            let handle_reads = shared.handle_ipc_reads(reader);
            let handle_writes = shared.handle_ipc_writes(writer, requests);

            if let Err(e) = futures_util::try_join!(handle_reads, handle_writes) {
                // FIXME: all subscriptions should also be resolved with an error
                // notify all pending requests of the error, which "unblocks" them
                error!(error = ?e, "IPC server exited due to an error");
                for (_, sender) in shared.pending.borrow_mut().drain() {
                    let _ = sender.send(Err(IpcError::ChannelError(
                        "IPC connection was closed unexpectedly".to_string(),
                    )));
                }
            }
        });
    });
}

struct Shared {
    pending: RefCell<HashMap<u64, Pending>>,
    subscriptions: RefCell<HashMap<U256, Subscription>>,
}

impl Shared {
    async fn handle_ipc_reads(&self, mut reader: OwnedReadHalf) -> Result<(), IpcError> {
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            // Try to read the next batch of bytes into the buffer
            let read = reader.read_buf(&mut buf).await?;
            if read == 0 {
                // eof, socket was closed
                return Ok(());
            }

            // Split off the received bytes
            let bytes = buf.split_to(read).freeze();
            // Parse all complete jsonrpc messages and send each to its
            // respective receiver
            self.handle_bytes(bytes)?;
        }
    }

    async fn handle_ipc_writes(
        &self,
        mut writer: OwnedWriteHalf,
        mut requests: mpsc::UnboundedReceiver<TransportMessage>,
    ) -> Result<(), IpcError> {
        use TransportMessage::*;

        while let Some(msg) = requests.next().await {
            match msg {
                Request { id, request, sender } => {
                    if self.pending.borrow_mut().insert(id, sender).is_some() {
                        warn!("Replacing a pending request with id {:?}", id);
                    }

                    if let Err(err) = writer.write(&request).await {
                        error!("IPC connection error: {:?}", err);
                        self.pending.borrow_mut().remove(&id);
                    }
                }
                Subscribe { id, sink } => {
                    if self.subscriptions.borrow_mut().insert(id, sink).is_some() {
                        warn!("Replacing already-registered subscription with id {:?}", id);
                    }
                }
                Unsubscribe { id } => {
                    if self.subscriptions.borrow_mut().remove(&id).is_none() {
                        warn!("Unsubscribing from non-existent subscription with id {:?}", id);
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_bytes(&self, bytes: Bytes) -> Result<(), IpcError> {
        let mut de = Deserializer::from_slice(&bytes).into_iter();
        while let Some(incoming) = de.next().transpose()? {
            match incoming {
                Incoming::Response(response) => {
                    if let Err(e) = self.send_response(response) {
                        error!(err = %e, "Failed to send IPC response");
                    }
                }
                Incoming::Notification(notification) => {
                    // Send notify response if okay.
                    if let Err(e) = self.send_notification(notification) {
                        error!(err = %e, "Failed to send IPC notification");
                    }
                }
            }
        }

        Ok(())
    }

    fn send_response(&self, response: Response<'_>) -> Result<(), IpcError> {
        let id = response.id();
        let res = response.into_result().map_err(Into::into);

        let response_tx = self.pending.borrow_mut().remove(&id).ok_or_else(|| {
            IpcError::ChannelError("No response channel exists for the response ID".to_string())
        })?;

        response_tx.send(res).map_err(|_| {
            IpcError::ChannelError("Receiver channel for response has been dropped".to_string())
        })?;

        Ok(())
    }

    /// Sends notification through the channel based on the ID of the subscription.
    /// This handles streaming responses.
    fn send_notification(&self, notification: Notification<'_>) -> Result<(), IpcError> {
        let id = notification.params.subscription;
        if let Some(tx) = self.subscriptions.borrow().get(&id) {
            tx.unbounded_send(notification.params.result.to_owned()).map_err(|_| {
                IpcError::ChannelError(format!("Subscription receiver {} dropped", id))
            })?;
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
/// Error thrown when sending or receiving an IPC message.
pub enum IpcError {
    /// Thrown if deserialization failed
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    /// std IO error forwarding.
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    /// Thrown if the response could not be parsed
    JsonRpcError(#[from] JsonRpcError),

    #[error("{0}")]
    ChannelError(String),

    #[error(transparent)]
    Canceled(#[from] RecvError),
}

impl From<IpcError> for ProviderError {
    fn from(src: IpcError) -> Self {
        ProviderError::JsonRpcClientError(Box::new(src))
    }
}
#[cfg(all(test, target_family = "unix"))]
#[cfg(not(feature = "celo"))]
mod test {
    use super::*;
    use ethers_core::{
        types::{Block, TxHash, U256},
        utils::Geth,
    };
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn request() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.into_temp_path().to_path_buf();
        let _geth = Geth::new().block_time(1u64).ipc_path(&path).spawn();
        let ipc = Ipc::connect(path).await.unwrap();

        let block_num: U256 = ipc.request("eth_blockNumber", ()).await.unwrap();
        std::thread::sleep(std::time::Duration::new(3, 0));
        let block_num2: U256 = ipc.request("eth_blockNumber", ()).await.unwrap();
        assert!(block_num2 > block_num);
    }

    #[tokio::test]
    async fn subscription() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.into_temp_path().to_path_buf();
        let _geth = Geth::new().block_time(2u64).ipc_path(&path).spawn();
        let ipc = Ipc::connect(path).await.unwrap();

        let sub_id: U256 = ipc.request("eth_subscribe", ["newHeads"]).await.unwrap();
        let mut stream = ipc.subscribe(sub_id).unwrap();

        // Subscribing requires sending the sub request and then subscribing to
        // the returned sub_id
        let block_num: u64 = ipc.request::<_, U256>("eth_blockNumber", ()).await.unwrap().as_u64();
        let mut blocks = Vec::new();
        for _ in 0..3 {
            let item = stream.next().await.unwrap();
            let block: Block<TxHash> = serde_json::from_str(item.get()).unwrap();
            blocks.push(block.number.unwrap_or_default().as_u64());
        }
        let offset = blocks[0] - block_num;
        assert_eq!(blocks, &[block_num + offset, block_num + offset + 1, block_num + offset + 2])
    }
}
