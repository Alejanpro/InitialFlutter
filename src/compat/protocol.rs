
use crate::compat::{other, CompatMessage};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::{io, iter};

// 2MB Block Size according to the specs at https://github.com/ipfs/specs/blob/main/BITSWAP.md