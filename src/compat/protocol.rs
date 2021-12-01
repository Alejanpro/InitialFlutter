
use crate::compat::{other, CompatMessage};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::{io, iter};

// 2MB Block Size according to the specs at https://github.com/ipfs/specs/blob/main/BITSWAP.md
const MAX_BUF_SIZE: usize = 2_097_152;

#[derive(Clone, Debug, Default)]
pub struct CompatProtocol;

impl UpgradeInfo for CompatProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/bitswap/1.2.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for CompatProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = InboundMessage;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;