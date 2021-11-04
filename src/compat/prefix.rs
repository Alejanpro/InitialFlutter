use crate::compat::other;
use libipld::cid::{Cid, Version};
use libipld::multihash::{Code, MultihashDigest};
use std::convert::TryFrom;
use std::io::Result;
use unsigned_varint::{decode as varint_decode, encode as varint_encode};

/// Prefix represents all metadata of a CID, without the actual content.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Prefix {
    /// The version of CID.
    pub version: Version,
    /// The codec of CID.
    pub codec: u64,
    /// The multihash type of CID.
    pub mh_type: u64,
    /// The multihash length of CID.
    pub mh_len: usize,
}

impl Prefix {
    /// Create a new prefix from encoded bytes.
    pub fn new(data: &[u8]) -> Result<Prefix> {
