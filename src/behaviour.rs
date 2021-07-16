
//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
#[cfg(feature = "compat")]
use crate::compat::{CompatMessage, CompatProtocol, InboundMessage};
use crate::protocol::{
    BitswapCodec, BitswapProtocol, BitswapRequest, BitswapResponse, RequestType,
};
use crate::query::{QueryEvent, QueryId, QueryManager, Request, Response};
use crate::stats::*;
use fnv::FnvHashMap;
#[cfg(feature = "compat")]
use fnv::FnvHashSet;
use futures::{
    channel::mpsc,
    stream::{Stream, StreamExt},
    task::{Context, Poll},
};
use libipld::{error::BlockNotFound, store::StoreParams, Block, Cid, Result};
#[cfg(feature = "compat")]
use libp2p::core::either::EitherOutput;
use libp2p::core::{connection::ConnectionId, Multiaddr, PeerId};
use libp2p::swarm::derive_prelude::{ConnectionClosed, DialFailure, FromSwarm, ListenFailure};
#[cfg(feature = "compat")]
use libp2p::swarm::{ConnectionHandlerSelect, NotifyHandler, OneShotHandler};
use libp2p::{
    request_response::{
        InboundFailure, OutboundFailure, ProtocolSupport, RequestId, RequestResponse,
        RequestResponseConfig, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
    },
    swarm::{ConnectionHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters},
};
use prometheus::Registry;
use std::{pin::Pin, time::Duration};

/// Bitswap response channel.
pub type Channel = ResponseChannel<BitswapResponse>;

/// Event emitted by the bitswap behaviour.
#[derive(Debug)]
pub enum BitswapEvent {
    /// Received a block from a peer. Includes the number of known missing blocks for a
    /// sync query. When a block is received and missing blocks is not empty the counter
    /// is increased. If missing blocks is empty the counter is decremented.
    Progress(QueryId, usize),
    /// A get or sync query completed.
    Complete(QueryId, Result<()>),
}

/// Trait implemented by a block store.
pub trait BitswapStore: Send + Sync + 'static {
    /// The store params.
    type Params: StoreParams;
    /// A have query needs to know if the block store contains the block.
    fn contains(&mut self, cid: &Cid) -> Result<bool>;
    /// A block query needs to retrieve the block from the store.
    fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    /// A block response needs to insert the block into the store.
    fn insert(&mut self, block: &Block<Self::Params>) -> Result<()>;
    /// A sync query needs a list of missing blocks to make progress.
    fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>>;
}

/// Bitswap configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitswapConfig {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
}

impl BitswapConfig {
    /// Creates a new `BitswapConfig`.
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            connection_keep_alive: Duration::from_secs(10),
        }
    }
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BitswapId {
    Bitswap(RequestId),
    #[cfg(feature = "compat")]
    Compat(Cid),
}

enum BitswapChannel {
    Bitswap(Channel),
    #[cfg(feature = "compat")]
    Compat(PeerId, Cid),
}

/// Network behaviour that handles sending and receiving blocks.
pub struct Bitswap<P: StoreParams> {
    /// Inner behaviour.
    inner: RequestResponse<BitswapCodec<P>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Requests.
    requests: FnvHashMap<BitswapId, QueryId>,
    /// Db request channel.
    db_tx: mpsc::UnboundedSender<DbRequest<P>>,
    /// Db response channel.
    db_rx: mpsc::UnboundedReceiver<DbResponse>,
    /// Compat peers.
    #[cfg(feature = "compat")]
    compat: FnvHashSet<PeerId>,
}

impl<P: StoreParams> Bitswap<P> {
    /// Creates a new `Bitswap` behaviour.
    pub fn new<S: BitswapStore<Params = P>>(config: BitswapConfig, store: S) -> Self {
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let protocols = std::iter::once((BitswapProtocol, ProtocolSupport::Full));
        let inner = RequestResponse::new(BitswapCodec::<P>::default(), protocols, rr_config);
        let (db_tx, db_rx) = start_db_thread(store);
        Self {
            inner,
            query_manager: Default::default(),
            requests: Default::default(),
            db_tx,
            db_rx,
            #[cfg(feature = "compat")]
            compat: Default::default(),
        }
    }

    /// Adds an address for a peer.
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }

    /// Removes an address for a peer.
    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.inner.remove_address(peer_id, addr);
    }

    /// Starts a get query with an initial guess of providers.
    pub fn get(&mut self, cid: Cid, peers: impl Iterator<Item = PeerId>) -> QueryId {
        self.query_manager.get(None, cid, peers)
    }

    /// Starts a sync query with an the initial set of missing blocks.
    pub fn sync(
        &mut self,
        cid: Cid,
        peers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
    ) -> QueryId {
        self.query_manager.sync(cid, peers, missing)
    }

    /// Cancels an in progress query. Returns true if a query was cancelled.
    pub fn cancel(&mut self, id: QueryId) -> bool {
        let res = self.query_manager.cancel(id);
        if res {
            REQUESTS_CANCELED.inc();
        }
        res
    }

    /// Registers prometheus metrics.
    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(REQUESTS_TOTAL.clone()))?;
        registry.register(Box::new(REQUEST_DURATION_SECONDS.clone()))?;
        registry.register(Box::new(REQUESTS_CANCELED.clone()))?;
        registry.register(Box::new(BLOCK_NOT_FOUND.clone()))?;
        registry.register(Box::new(PROVIDERS_TOTAL.clone()))?;
        registry.register(Box::new(MISSING_BLOCKS_TOTAL.clone()))?;
        registry.register(Box::new(RECEIVED_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RECEIVED_INVALID_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(SENT_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RESPONSES_TOTAL.clone()))?;
        registry.register(Box::new(THROTTLED_INBOUND.clone()))?;
        registry.register(Box::new(THROTTLED_OUTBOUND.clone()))?;
        registry.register(Box::new(OUTBOUND_FAILURE.clone()))?;
        registry.register(Box::new(INBOUND_FAILURE.clone()))?;
        Ok(())
    }
}

enum DbRequest<P: StoreParams> {
    Bitswap(BitswapChannel, BitswapRequest),
    Insert(Block<P>),
    MissingBlocks(QueryId, Cid),
}

enum DbResponse {
    Bitswap(BitswapChannel, BitswapResponse),
    MissingBlocks(QueryId, Result<Vec<Cid>>),
}

fn start_db_thread<S: BitswapStore>(
    mut store: S,
) -> (
    mpsc::UnboundedSender<DbRequest<S::Params>>,
    mpsc::UnboundedReceiver<DbResponse>,
) {
    let (tx, requests) = mpsc::unbounded();
    let (responses, rx) = mpsc::unbounded();
    std::thread::spawn(move || {
        let mut requests: mpsc::UnboundedReceiver<DbRequest<S::Params>> = requests;
        while let Some(request) = futures::executor::block_on(requests.next()) {
            match request {
                DbRequest::Bitswap(channel, request) => {
                    let response = match request.ty {
                        RequestType::Have => {
                            let have = store.contains(&request.cid).ok().unwrap_or_default();
                            if have {
                                RESPONSES_TOTAL.with_label_values(&["have"]).inc();
                            } else {
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                            }
                            tracing::trace!("have {}", have);
                            BitswapResponse::Have(have)
                        }
                        RequestType::Block => {
                            let block = store.get(&request.cid).ok().unwrap_or_default();
                            if let Some(data) = block {
                                RESPONSES_TOTAL.with_label_values(&["block"]).inc();
                                SENT_BLOCK_BYTES.inc_by(data.len() as u64);
                                tracing::trace!("block {}", data.len());
                                BitswapResponse::Block(data)
                            } else {
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                                tracing::trace!("have false");
                                BitswapResponse::Have(false)
                            }
                        }
                    };
                    responses
                        .unbounded_send(DbResponse::Bitswap(channel, response))
                        .ok();
                }
                DbRequest::Insert(block) => {
                    if let Err(err) = store.insert(&block) {
                        tracing::error!("error inserting blocks {}", err);
                    }
                }
                DbRequest::MissingBlocks(id, cid) => {
                    let res = store.missing_blocks(&cid);
                    responses
                        .unbounded_send(DbResponse::MissingBlocks(id, res))
                        .ok();
                }
            }
        }
    });
    (tx, rx)
}

impl<P: StoreParams> Bitswap<P> {
    /// Processes an incoming bitswap request.
    fn inject_request(&mut self, channel: BitswapChannel, request: BitswapRequest) {
        self.db_tx
            .unbounded_send(DbRequest::Bitswap(channel, request))
            .ok();
    }

    /// Processes an incoming bitswap response.
    fn inject_response(&mut self, id: BitswapId, peer: PeerId, response: BitswapResponse) {
        if let Some(id) = self.requests.remove(&id) {
            match response {
                BitswapResponse::Have(have) => {
                    self.query_manager
                        .inject_response(id, Response::Have(peer, have));
                }
                BitswapResponse::Block(data) => {
                    if let Some(info) = self.query_manager.query_info(id) {
                        let len = data.len();
                        if let Ok(block) = Block::new(info.cid, data) {
                            RECEIVED_BLOCK_BYTES.inc_by(len as u64);
                            self.db_tx.unbounded_send(DbRequest::Insert(block)).ok();
                            self.query_manager
                                .inject_response(id, Response::Block(peer, true));
                        } else {
                            tracing::error!("received invalid block");
                            RECEIVED_INVALID_BLOCK_BYTES.inc_by(len as u64);
                            self.query_manager
                                .inject_response(id, Response::Block(peer, false));
                        }
                    }
                }
            }
        }
    }

    fn inject_outbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: RequestId,
        error: &OutboundFailure,
    ) {
        tracing::debug!(
            "bitswap outbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            OutboundFailure::DialFailure => {
                OUTBOUND_FAILURE.with_label_values(&["dial_failure"]).inc();
            }
            OutboundFailure::Timeout => {
                OUTBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            OutboundFailure::ConnectionClosed => {
                OUTBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            OutboundFailure::UnsupportedProtocols => {
                OUTBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
        }
    }

    fn inject_inbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: RequestId,
        error: &InboundFailure,
    ) {
        tracing::error!(
            "bitswap inbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            InboundFailure::Timeout => {
                INBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            InboundFailure::ConnectionClosed => {
                INBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            InboundFailure::UnsupportedProtocols => {
                INBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
            InboundFailure::ResponseOmission => {
                INBOUND_FAILURE
                    .with_label_values(&["response_omission"])
                    .inc();
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviour for Bitswap<P> {
    #[cfg(not(feature = "compat"))]
    type ConnectionHandler =
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ConnectionHandler;

    #[cfg(feature = "compat")]
    #[allow(clippy::type_complexity)]
    type ConnectionHandler = ConnectionHandlerSelect<
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ConnectionHandler,
        OneShotHandler<CompatProtocol, CompatMessage, InboundMessage>,
    >;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        #[cfg(not(feature = "compat"))]
        return self.inner.new_handler();
        #[cfg(feature = "compat")]
        ConnectionHandler::select(self.inner.new_handler(), OneShotHandler::default())
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ev) => self
                .inner
                .on_swarm_event(FromSwarm::ConnectionEstablished(ev)),
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                handler,
                remaining_established,
            }) => {
                #[cfg(feature = "compat")]
                if remaining_established == 0 {
                    self.compat.remove(&peer_id);
                }
                #[cfg(feature = "compat")]
                let (handler, _oneshot) = handler.into_inner();
                self.inner
                    .on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
                        peer_id,
                        connection_id,
                        endpoint,
                        handler,
                        remaining_established,
                    }));
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id,
                handler,
                error,
            }) => {
                #[cfg(feature = "compat")]
                let (handler, _oneshot) = handler.into_inner();
                self.inner
                    .on_swarm_event(FromSwarm::DialFailure(DialFailure {
                        peer_id,
                        handler,
                        error,
                    }));
            }
            FromSwarm::AddressChange(ev) => self.inner.on_swarm_event(FromSwarm::AddressChange(ev)),
            FromSwarm::ListenFailure(ListenFailure {
                local_addr,
                send_back_addr,
                handler,
            }) => {
                #[cfg(feature = "compat")]
                let (handler, _oneshot) = handler.into_inner();
                self.inner
                    .on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
                        local_addr,
                        send_back_addr,
                        handler,
                    }));
            }
            FromSwarm::NewListener(ev) => self.inner.on_swarm_event(FromSwarm::NewListener(ev)),
            FromSwarm::NewListenAddr(ev) => self.inner.on_swarm_event(FromSwarm::NewListenAddr(ev)),
            FromSwarm::ExpiredListenAddr(ev) => {
                self.inner.on_swarm_event(FromSwarm::ExpiredListenAddr(ev))
            }
            FromSwarm::ListenerError(ev) => self.inner.on_swarm_event(FromSwarm::ListenerError(ev)),
            FromSwarm::ListenerClosed(ev) => {
                self.inner.on_swarm_event(FromSwarm::ListenerClosed(ev))
            }
            FromSwarm::NewExternalAddr(ev) => {
                self.inner.on_swarm_event(FromSwarm::NewExternalAddr(ev))
            }
            FromSwarm::ExpiredExternalAddr(ev) => self
                .inner
                .on_swarm_event(FromSwarm::ExpiredExternalAddr(ev)),
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::OutEvent,
    ) {
        tracing::trace!(?event, "on_connection_handler_event");
        #[cfg(not(feature = "compat"))]
        return self.inner.on_connection_handler_event(peer_id, conn, event);
        #[cfg(feature = "compat")]
        match event {
            EitherOutput::First(event) => {
                self.inner.on_connection_handler_event(peer_id, conn, event)
            }
            EitherOutput::Second(msg) => {
                for msg in msg.0 {
                    match msg {
                        CompatMessage::Request(req) => {
                            tracing::trace!("received compat request");
                            self.inject_request(BitswapChannel::Compat(peer_id, req.cid), req);
                        }
                        CompatMessage::Response(cid, res) => {
                            tracing::trace!("received compat response");
                            self.inject_response(BitswapId::Compat(cid), peer_id, res);
                        }
                    }
                }
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        pp: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(Some(response)) = Pin::new(&mut self.db_rx).poll_next(cx) {
                exit = false;
                match response {
                    DbResponse::Bitswap(channel, response) => match channel {
                        BitswapChannel::Bitswap(channel) => {
                            self.inner.send_response(channel, response).ok();
                        }
                        #[cfg(feature = "compat")]
                        BitswapChannel::Compat(peer_id, cid) => {
                            let compat = CompatMessage::Response(cid, response);
                            return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::Any,
                                event: EitherOutput::Second(compat),
                            });
                        }
                    },
                    DbResponse::MissingBlocks(id, res) => match res {
                        Ok(missing) => {
                            MISSING_BLOCKS_TOTAL.inc_by(missing.len() as u64);
                            self.query_manager
                                .inject_response(id, Response::MissingBlocks(missing));
                        }
                        Err(err) => {
                            self.query_manager.cancel(id);
                            let event = BitswapEvent::Complete(id, Err(err));
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                    },
                }
            }
            while let Some(query) = self.query_manager.next() {
                exit = false;
                match query {
                    QueryEvent::Request(id, req) => match req {
                        Request::Have(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Have,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::Block(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Block,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::MissingBlocks(cid) => {
                            self.db_tx
                                .unbounded_send(DbRequest::MissingBlocks(id, cid))
                                .ok();
                        }
                    },
                    QueryEvent::Progress(id, missing) => {
                        let event = BitswapEvent::Progress(id, missing);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    QueryEvent::Complete(id, res) => {
                        if res.is_err() {
                            BLOCK_NOT_FOUND.inc();
                        }
                        let event = BitswapEvent::Complete(
                            id,
                            res.map_err(|cid| BlockNotFound(cid).into()),
                        );
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                }
            }
            while let Poll::Ready(event) = self.inner.poll(cx, pp) {
                exit = false;
                let event = match event {
                    NetworkBehaviourAction::GenerateEvent(event) => event,
                    NetworkBehaviourAction::Dial { opts, handler } => {
                        #[cfg(feature = "compat")]
                        let handler = ConnectionHandler::select(handler, Default::default());
                        return Poll::Ready(NetworkBehaviourAction::Dial { opts, handler });
                    }
                    NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                            peer_id,
                            handler,
                            #[cfg(not(feature = "compat"))]
                            event,
                            #[cfg(feature = "compat")]
                            event: EitherOutput::First(event),
                        });
                    }
                    NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                        return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                            address,
                            score,
                        });
                    }
                    NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection,
                        });
                    }
                };
                match event {
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => self.inject_request(BitswapChannel::Bitswap(channel), request),
                        RequestResponseMessage::Response {
                            request_id,
                            response,
                        } => self.inject_response(BitswapId::Bitswap(request_id), peer, response),
                    },
                    RequestResponseEvent::ResponseSent { .. } => {}
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_outbound_failure(&peer, request_id, &error);
                        #[cfg(feature = "compat")]
                        if let OutboundFailure::UnsupportedProtocols = error {
                            if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id))
                            {
                                if let Some(info) = self.query_manager.query_info(id) {
                                    let ty = match info.label {
                                        "have" => RequestType::Have,
                                        "block" => RequestType::Block,
                                        _ => unreachable!(),
                                    };
                                    let request = BitswapRequest { ty, cid: info.cid };
                                    self.requests.insert(BitswapId::Compat(info.cid), id);
                                    tracing::trace!("adding compat peer {}", peer);
                                    self.compat.insert(peer);
                                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                        peer_id: peer,
                                        handler: NotifyHandler::Any,
                                        event: EitherOutput::Second(CompatMessage::Request(