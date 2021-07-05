
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