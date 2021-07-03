
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