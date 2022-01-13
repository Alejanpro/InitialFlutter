use crate::stats::{REQUESTS_TOTAL, REQUEST_DURATION_SECONDS};
use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use libp2p::PeerId;
use prometheus::HistogramTimer;
use std::collections::VecDeque;

/// Query id.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QueryId(u64);

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Request.
#[derive(Debug, Eq, PartialEq)]
pub enum Request {
    /// Have query.
    Have(PeerId, Cid),
    /// Block query.
    Block(PeerId, Cid),
    /// Missing blocks query.
    MissingBlocks(Cid),
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Have(_, _) => write!(f, "have"),
            Self::Block(_, _) => write!(f, "block"),
            Self::MissingBlocks(_) => write!(f, "missing-blocks"),
        }
    }
}

/// Response.
#[derive(Debug)]
pub enum Response {
    /// Have query.
    Have(PeerId, bool),
    /// Block query.
    Block(PeerId, bool),
    /// Missing blocks query.
    MissingBlocks(Vec<Cid>),
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Have(_, have) => write!(f, "have {}", have),
            Self::Block(_, block) => write!(f, "block {}", block),
            Self::MissingBlocks(missing) => write!(f, "missing-blocks {}", missing.len()),
        }
    }
}

/// Event emitted by a query.
#[derive(Debug)]
pub enum QueryEvent {
    /// A subquery to run.
    Request(QueryId, Request),
    /// A progress event.
    Progress(QueryId, usize),
    /// Complete event.
    Complete(QueryId, Result<(), Cid>),
}

#[derive(Debug)]
pub struct Header {
    /// Query id.
    pub id: QueryId,
    /// Root query id.
    pub root: QueryId,
    /// Parent.
    pub parent: Option<QueryId>,
    /// Cid.
    pub cid: Cid,
    /// Timer.
    pub timer: HistogramTimer,
    /// Type.
    pub label: &'static str,
}

impl Drop for Header {
    fn drop(&mut self) {
        REQUESTS_TOTAL.with_label_values(&[self.label]).inc();
    }
}

/// Query.
#[derive(Debug)]
struct Query {
    /// Header.
    hdr: Header,
    /// State.
    state: State,
}

#[derive(Debug)]
enum State {
 