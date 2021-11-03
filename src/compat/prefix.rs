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
    /// The multihash len