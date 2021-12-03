
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
        }
    }
}

#[async_trait]
impl<P: StoreParams> RequestResponseCodec for BitswapCodec<P> {
    type Protocol = BitswapProtocol;
    type Request = BitswapRequest;
    type Response = BitswapResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let msg_len = u32_to_usize(aio::read_u32(&mut *io).await.map_err(|e| match e {
            ReadError::Io(e) => e,
            err => other(err),
        })?);
        if msg_len > MAX_CID_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(msg_len)));
        }
        self.buffer.resize(msg_len, 0);
        io.read_exact(&mut self.buffer).await?;
        let request = BitswapRequest::from_bytes(&self.buffer).map_err(invalid_data)?;
        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        let msg_len = u32_to_usize(aio::read_u32(&mut *io).await.map_err(|e| match e {
            ReadError::Io(e) => e,
            err => other(err),
        })?);
        if msg_len > P::MAX_BLOCK_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(msg_len)));
        }
        self.buffer.resize(msg_len, 0);
        io.read_exact(&mut self.buffer).await?;
        let response = BitswapResponse::from_bytes(&self.buffer).map_err(invalid_data)?;
        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.buffer.clear();
        req.write_to(&mut self.buffer)?;
        if self.buffer.len() > MAX_CID_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(self.buffer.len())));
        }
        let mut buf = unsigned_varint::encode::u32_buffer();
        let msg_len = unsigned_varint::encode::u32(self.buffer.len() as u32, &mut buf);
        io.write_all(msg_len).await?;
        io.write_all(&self.buffer).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.buffer.clear();
        res.write_to(&mut self.buffer)?;
        if self.buffer.len() > P::MAX_BLOCK_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(self.buffer.len())));
        }
        let mut buf = unsigned_varint::encode::u32_buffer();
        let msg_len = unsigned_varint::encode::u32(self.buffer.len() as u32, &mut buf);
        io.write_all(msg_len).await?;
        io.write_all(&self.buffer).await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestType {
    Have,
    Block,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitswapRequest {
    pub ty: RequestType,