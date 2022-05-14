use std::{
    cell::RefCell,
    error, fmt,
    hash::BuildHasherDefault,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use bytes::{Buf as _, BytesMut};
use ethers_core::types::U256;
use hashers::fx_hash::FxHasher64;
use serde_json::{value::RawValue, Deserializer};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{
        unix::{ReadHalf, WriteHalf},
        UnixStream,
    },
    runtime,
    sync::{mpsc, oneshot},
};

use crate::{
    err::TransportError,
    jsonrpc::{Params, Response},
    Connection, DuplexConnection, NotificationReceiver, RequestFuture, ResponsePayload,
    SubscribeFuture,
};

type FxHashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<FxHasher64>>;
type PendingRequest = oneshot::Sender<ResponsePayload>;

enum Request {
    Call { id: u64, tx: PendingRequest, request: String },
    Subscribe { id: U256, tx: oneshot::Sender<Option<NotificationReceiver>> },
    Unsubscribe { id: U256 },
}

/// The handle for an IPC connection to an Ethereum JSON-RPC provider.
///
/// **Note** Dropping an [`Ipc`] handle will invalidate all pending requests
/// that were made through it.
pub struct Ipc {
    /// The counter for unique request ids.
    next_id: AtomicU64,
    /// The instance for sending requests to the IPC request server
    request_tx: mpsc::UnboundedSender<Request>,
}

impl Ipc {
    /// Connects to the IPC socket at `path`.
    ///
    /// # Errors
    ///
    /// Fails, if establishing the connection to the socket fails.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, IpcError> {
        let next_id = AtomicU64::new(0);
        let (request_tx, request_rx) = mpsc::unbounded_channel();

        // try to connect to the IPC socket at `path`
        let path = path.as_ref();
        let stream = UnixStream::connect(path)
            .await
            .map_err(|source| IpcError::InvalidSocket { path: path.into(), source })?;

        // spawn an IPC server thread with its own async runtime
        spawn_ipc_server(stream, request_rx);

        Ok(Self { next_id, request_tx })
    }
}

impl Connection for Ipc {
    fn request_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn send_raw_request(&self, id: u64, request: String) -> RequestFuture<'_> {
        Box::pin(async move {
            // send the request to the IPC server
            let (tx, rx) = oneshot::channel();
            self.request_tx.send(Request::Call { id, tx, request }).map_err(|_| server_exit())?;

            // await the response
            rx.await.map_err(|_| server_exit())?
        })
    }
}

impl DuplexConnection for Ipc {
    fn subscribe(&self, id: U256) -> SubscribeFuture<'_> {
        Box::pin(async move {
            // send the subscribe request to the IPC server
            let (tx, rx) = oneshot::channel();
            self.request_tx.send(Request::Subscribe { id, tx }).map_err(|_| server_exit())?;

            // await the response
            let res = rx.await.map_err(|_| server_exit())?;
            Ok(res)
        })
    }

    fn unsubscribe(&self, id: U256) -> Result<(), Box<TransportError>> {
        self.request_tx.send(Request::Unsubscribe { id }).map_err(|_| server_exit())
    }
}

fn spawn_ipc_server(stream: UnixStream, request_rx: mpsc::UnboundedReceiver<Request>) {
    // 65 KiB should be more than enough for this thread, as all unbounded data
    // growth occurs on heap-allocated data structures/buffers and the call
    // stack is not going to do anything odd either
    const STACK_SIZE: usize = 1 << 16;
    let _ = thread::Builder::new()
        .name("ipc-server-thread".to_string())
        .stack_size(STACK_SIZE)
        .spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("failed to create IPC server thread async runtime");

            rt.block_on(run_ipc_server(stream, request_rx));
        })
        .expect("failed to spawn IPC server thread");
}

async fn run_ipc_server(mut stream: UnixStream, request_rx: mpsc::UnboundedReceiver<Request>) {
    // the shared state for both reads & writes
    let shared = Shared {
        pending: FxHashMap::with_capacity_and_hasher(64, BuildHasherDefault::default()).into(),
        subs: FxHashMap::with_capacity_and_hasher(64, BuildHasherDefault::default()).into(),
    };

    // split the stream and run two independent concurrently (local), thereby
    // allowing reads and writes to occurr concurrently
    let (reader, writer) = stream.split();
    let read = shared.handle_ipc_reads(reader);
    let write = shared.handle_ipc_writes(writer, request_rx);

    // run both loops concurrently & abort (drop) the other once either of them finishes.
    let res = tokio::select! {
        biased;
        res = read => res,
        res = write => res,
    };

    if let Err(e) = res {
        tracing::error!(err = ?e, "exiting IPC server due to error");
    }
}

type Subscription = (mpsc::UnboundedSender<Box<RawValue>>, Option<NotificationReceiver>);

struct Shared {
    pending: RefCell<FxHashMap<u64, PendingRequest>>,
    subs: RefCell<FxHashMap<U256, Subscription>>,
}

impl Shared {
    async fn handle_ipc_reads(&self, reader: ReadHalf<'_>) -> Result<(), IpcError> {
        let mut reader = BufReader::new(reader);
        let mut buf = BytesMut::with_capacity(4096);

        loop {
            // try to read the next batch of bytes into the buffer
            let read = reader.read_buf(&mut buf).await?;
            if read == 0 {
                // eof, socket was closed
                return Ok(());
            }

            // parse the received bytes into 0-n jsonrpc messages
            let read = self.handle_bytes(&buf)?;
            // split off all bytes that were parsed into complete messages
            // any remaining bytes that correspond to incomplete messages remain
            // in the buffer
            buf.advance(read);
        }
    }

    async fn handle_ipc_writes(
        &self,
        mut writer: WriteHalf<'_>,
        mut request_rx: mpsc::UnboundedReceiver<Request>,
    ) -> Result<(), IpcError> {
        use Request::*;

        while let Some(msg) = request_rx.recv().await {
            match msg {
                Call { id, tx, request } => {
                    let prev = self.pending.borrow_mut().insert(id, tx);
                    assert!(prev.is_none(), "replaced pending IPC request (id={})", id);
                    writer.write_all(request.as_bytes()).await?;
                }
                Subscribe { id, tx } => {
                    use std::collections::hash_map::Entry::*;
                    let res = match self.subs.borrow_mut().entry(id) {
                        // the entry already exists, e.g., because it was
                        // earlier instantiated by an incoming notification
                        Occupied(mut occ) => {
                            // take the receiver half, which is `None` if a
                            // subscription stream has already been created for
                            // this ID.
                            let (_, rx) = occ.get_mut();
                            rx.take()
                        }
                        Vacant(vac) => {
                            // insert a new channel tx/rx pair
                            let (sub_tx, sub_rx) = mpsc::unbounded_channel();
                            vac.insert((sub_tx, None));
                            Some(sub_rx)
                        }
                    };

                    let _ = tx.send(res);
                }
                Unsubscribe { id } => {
                    // removes the subscription entry and drops the sender half,
                    // ending the registered subscription stream (if any)
                    // NOTE: if the subscription has not been removed at the
                    // provider side as well, it will keep sending further
                    // notifications, which will re-create the entry
                    let _ = self.subs.borrow_mut().remove(&id);
                }
            }
        }

        // the IPC handle has been dropped
        Ok(())
    }

    fn handle_bytes(&self, bytes: &BytesMut) -> Result<usize, IpcError> {
        // deserialize all complete jsonrpc responses contained in the buffer
        let mut de = Deserializer::from_slice(bytes.as_ref()).into_iter();
        while let Some(Ok(response)) = de.next() {
            match response {
                Response::Success { id, result } => self.handle_response(id, Ok(result.to_owned())),
                Response::Error { id, error } => {
                    self.handle_response(id, Err(TransportError::jsonrpc(error)))
                }
                Response::Notification { params, .. } => self.handle_notification(params),
            };
        }

        Ok(de.byte_offset())
    }

    fn handle_response(&self, id: u64, res: Result<Box<RawValue>, Box<TransportError>>) {
        match self.pending.borrow_mut().remove(&id) {
            Some(tx) => {
                // if send fails, request has been dropped at the callsite
                let _ = tx.send(res);
            }
            None => tracing::warn!(%id, "no pending request exists for response ID"),
        };
    }

    /// Sends notification through the channel based on the ID of the subscription.
    /// This handles streaming responses.
    fn handle_notification(&self, params: Params<'_>) {
        use std::collections::hash_map::Entry;
        let notification = params.result.to_owned();
        let mut borrow = self.subs.borrow_mut();

        let ok = match borrow.entry(params.subscription) {
            // the subscription entry has already been inserted (e.g., if the
            // sub has already been registered)
            Entry::Occupied(occ) => {
                let (tx, _) = occ.get();
                tx.send(notification).is_ok()
            }
            // the subscription has not yet been registered, insert a new tx/rx
            // pair and push the current notification to ensure that none get
            // lost
            Entry::Vacant(vac) => {
                let (tx, rx) = mpsc::unbounded_channel();
                // insert the tx/rx pair, which can be taken by the first
                // arriving registration
                let (tx, _) = vac.insert((tx, Some(rx)));
                tx.send(notification).is_ok()
            }
        };

        if !ok {
            // the channel has been dropped without unsubscribing
            let _ = borrow.remove(&params.subscription);
        }
    }
}

#[derive(Debug)]
pub enum IpcError {
    /// The file at `path` is not a valid IPC socket.
    InvalidSocket {
        path: PathBuf,
        source: io::Error,
    },
    Io(io::Error),
    ServerExit,
}

impl error::Error for IpcError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::InvalidSocket { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSocket { path, .. } => write!(f, "invalid IPC socket at {path:?}"),
            Self::Io(io) => write!(f, "{io}"),
            Self::ServerExit => f.write_str("the IPC server has exited unexpectedly"),
        }
    }
}

impl From<io::Error> for IpcError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

fn server_exit() -> Box<TransportError> {
    TransportError::transport(IpcError::ServerExit)
}
