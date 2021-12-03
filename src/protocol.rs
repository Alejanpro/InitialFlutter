
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libipld::cid::Cid;
use libipld::store::StoreParams;
use libp2p::request_response::{ProtocolName, RequestResponseCodec};
use std::convert::TryFrom;
use std::io::{self, Write};
use std::marker::PhantomData;
use thiserror::Error;
use unsigned_varint::{aio, io::ReadError};

// version codec hash size (u64 varint is max 10 bytes) + digest
const MAX_CID_SIZE: usize = 4 * 10 + 64;

#[derive(Clone, Debug)]
pub struct BitswapProtocol;

impl ProtocolName for BitswapProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/ipfs-embed/bitswap/1.0.0"
    }
}

#[derive(Clone)]
pub struct BitswapCodec<P> {
    _marker: PhantomData<P>,
    buffer: Vec<u8>,
}

impl<P: StoreParams> Default for BitswapCodec<P> {
    fn default() -> Self {
        let capacity = usize::max(P::MAX_BLOCK_SIZE, MAX_CID_SIZE) + 1;
        debug_assert!(capacity <= u32::MAX as usize);
        Self {
            _marker: PhantomData,
            buffer: Vec::with_capacity(capacity),