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
pub enum CompatMessage {
    Request(BitswapRequest),
    Response(Cid, BitswapResponse),
}

impl CompatMessage {
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut msg = bitswap_pb::Message::default();
        match self {
            CompatMessage::Request(BitswapRequest { ty, cid }) => {
                let mut wantlist = bitswap_pb::message::Wantlist::default();
                let entry = bitswap_pb::message::wantlist::Entry {
                    block: cid.to_bytes(),
                    want_type: match ty {
                        RequestType::Have => bitswap_pb::message::wantlist::WantType::Have,
                        RequestType::Block => bitswap_pb::message::wantlist::WantType::Block,
                    } as _,
                    send_dont_have: true,
                    cancel: false,
                    priority: 1,
                };
                wantlist.entries.push(entry);
                msg.wantlist = Some(wantlist);
            }
            CompatMessage::Response(cid, BitswapResponse::Have(have)) => {
                let block_presence = bitswap_pb::message::BlockPresence {
                    cid: cid.to_bytes(),
                    r#type: if *have {
                        bitswap_pb::message::BlockPresenceType::Have
                    } else {
                        bitswap_pb::message::BlockPresenceType::DontHave
                    } as _,
                };
                ms