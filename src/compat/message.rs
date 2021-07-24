use crate::compat::other;
use crate::compat::prefix::Prefix;
use crate::protocol::{BitswapRequest, BitswapResponse, RequestType};
use libipld::Cid;
use prost::Message;
use std::convert::TryFrom