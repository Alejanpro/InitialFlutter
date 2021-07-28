use crate::compat::other;
use crate::compat::prefix::Prefix;
use crate::protocol::{BitswapRequest, BitswapResponse, RequestType};
use libipld::Cid;
use prost::Message;
use std::convert::TryFrom;
use std::io;

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Comp