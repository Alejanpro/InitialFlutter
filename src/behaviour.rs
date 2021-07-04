
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