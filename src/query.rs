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
    None,
    Get(GetState),
    Sync(SyncState),
}

#[derive(Debug, Default)]
struct GetState {
    have: FnvHashSet<QueryId>,
    block: Option<QueryId>,
    providers: Vec<PeerId>,
}

#[derive(Debug, Default)]
struct SyncState {
    missing: FnvHashSet<QueryId>,
    children: FnvHashSet<QueryId>,
    providers: Vec<PeerId>,
}

enum Transition<S, C> {
    Next(S),
    Complete(C),
}

#[derive(Default)]
pub struct QueryManager {
    id_counter: u64,
    queries: FnvHashMap<QueryId, Query>,
    events: VecDeque<QueryEvent>,
}

impl QueryManager {
    /// Start a new subquery.
    fn start_query(
        &mut self,
        root: QueryId,
        parent: Option<QueryId>,
        cid: Cid,
        req: Request,
        label: &'static str,
    ) -> QueryId {
        let timer = REQUEST_DURATION_SECONDS
            .with_label_values(&[label])
            .start_timer();
        let id = QueryId(self.id_counter);
        self.id_counter += 1;
        let query = Query {
            hdr: Header {
                id,
                root,
                parent,
                cid,
                timer,
                label,
            },
            state: State::None,
        };
        self.queries.insert(id, query);
        tracing::trace!("{} {} {}", root, id, req);
        self.events.push_back(QueryEvent::Request(id, req));
        id
    }

    /// Starts a new have query to ask a peer if it has a block.
    fn have(&mut self, root: QueryId, parent: QueryId, peer_id: PeerId, cid: Cid) -> QueryId {
        self.start_query(root, Some(parent), cid, Request::Have(peer_id, cid), "have")
    }

    /// Starts a new block query to request a block from a peer.
    fn block(&mut self, root: QueryId, parent: QueryId, peer_id: PeerId, cid: Cid) -> QueryId {
        self.start_query(
            root,
            Some(parent),
            cid,
            Request::Block(peer_id, cid),
            "block",
        )
    }

    /// Starts a query to determine the missing blocks of a dag.
    fn missing_blocks(&mut self, parent: QueryId, cid: Cid) -> QueryId {
        self.start_query(
            parent,
            Some(parent),
            cid,
            Request::MissingBlocks(cid),
            "missing-blocks",
        )
    }

    /// Starts a query to locate and retrieve a block. Panics if no providers are supplied.
    pub fn get(
        &mut self,
        parent: Option<QueryId>,
        cid: Cid,
        providers: impl Iterator<Item = PeerId>,
    ) -> QueryId {
        let timer = REQUEST_DURATION_SECONDS
            .with_label_values(&["get"])
            .start_timer();
        let id = QueryId(self.id_counter);
        self.id_counter += 1;
        let root = parent.unwrap_or(id);
        tracing::trace!("{} {} get", root, id);
        let mut state = GetState::default();
        for peer in providers {
            if state.block.is_none() {
                state.block = Some(self.block(root, id, peer, cid));
            } else {
                state.have.insert(self.have(root, id, peer, cid));
            }
        }
        assert!(state.block.is_some());
        let query = Query {
            hdr: Header {
                id,
                root,
                parent,
                cid,
                timer,
                label: "get",
            },
            state: State::Get(state),
        };
        self.queries.insert(id, query);
        id
    }

    /// Starts a query to recursively retrieve a dag. The missing blocks are the first
    /// blocks that need to be retrieved.
    pub fn sync(
        &mut self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
    ) -> QueryId {
        let timer = REQUEST_DURATION_SECONDS
            .with_label_values(&["sync"])
            .start_timer();
        let id = QueryId(self.id_counter);
        self.id_counter += 1;
        tracing::trace!("{} {} sync", id, id);
        let mut state = SyncState::default();
        for cid in missing {
            state
                .missing
                .insert(self.get(Some(id), cid, providers.iter().copied()));
        }
        if state.missing.is_empty() {
            state.children.insert(self.missing_blocks(id, cid));
        }
        state.providers = providers;
        let query = Query {
            hdr: Header {
                id,
                root: id,
                parent: None,
                cid,
                timer,
                label: "sync",
            },
            state: State::Sync(state),
        };
        self.queries.insert(id, query);
        id
    }

    /// Cancels an in progress query.
    pub fn cancel(&mut self, root: QueryId) -> bool {
        let query = if let Some(query) = self.queries.remove(&root) {
            query
        } else {
            return false;
        };
        let queries = &self.queries;
        self.events.retain(|event| {
            let (id, req) = match event {
                QueryEvent::Request(id, req) => (id, req),
                QueryEvent::Progress(id, _) => return *id != root,
                QueryEvent::Complete(_, _) => return true,
            };
            if queries.get(id).map(|q| q.hdr.root) != Some(root) {
                return true;
            }
            tracing::trace!("{} {} {} cancel", root, id, req);
            false
        });
        match query.state {
            State::Get(_) => {
                tracing::trace!("{} {} get cancel", root, root);
                true
            }
            State::Sync(state) => {
                for id in state.missing {
                    tracing::trace!("{} {} get cancel", root, id);
                    self.queries.remove(&id);
                }
                tracing::trace!("{} {} sync cancel", root, root);
                true
            }
            State::None => {
                self.queries.insert(root, query);
                false
            }
        }
    }

    /// Advances a get query state machine using a transition function.
    fn get_query<F>(&mut self, id: QueryId, f: F)
    where
        F: FnOnce(&mut Self, &Header, GetState) -> Transition<GetState, Result<(), Cid>>,
    {
        if let Some(mut parent) = self.queries.remove(&id) {
            let state = if let State::Get(state) = parent.state {
                state
            } else {
                return;
            };
            match f(self, &parent.hdr, state) {
                Transition::Next(state) => {
                    parent.state = State::Get(state);
                    self.queries.insert(id, parent);
                }
                Transition::Complete(res) => {
                    match res {
                        Ok(()) => tracing::trace!("{} {} get ok", parent.hdr.root, parent.hdr.id),
                        Err(_) => tracing::trace!("{} {} get err", parent.hdr.root, parent.hdr.id),
                    }
                    self.recv_get(parent.hdr, res);
                }
            }
        }
    }

    /// Advances a sync query state machine using a transition function.
    fn sync_query<F>(&mut self, id: QueryId, f: F)
    where
        F: FnOnce(&mut Self, &Header, SyncState) -> Transition<SyncState, Result<(), Cid>>,
    {
        if let Some(mut parent) = self.queries.remove(&id) {
            let state = if let State::Sync(state) = parent.state {
                state
            } else {
                return;
            };
            match f(self, &parent.hdr, state) {
                Transition::Next(state) => {
                    parent.state = State::Sync(state);
                    self.queries.insert(id, parent);
                }
                Transition::Complete(res) => {
                    if res.is_ok() {
                        tracing::trace!("{} {} sync ok", parent.hdr.root, parent.hdr.id);
                    } else {
                        tracing::trace!("{} {} sync err", parent.hdr.root, parent.hdr.id);
                    }
                    self.recv_sync(parent.hdr, res);
                }
            }
        }
    }

    /// Processes the response of a have query.
    ///
    /// Marks the in progress query as complete and updates the set of peers that have
    /// a block. If there isn't an in progress block query a new block query will be
    /// started. If no block query can be started either a provider query is started or
    /// the get query is marked as complete with a block-not-found error.
    fn recv_have(&mut self, query: Header, peer_id: PeerId, have: bool) {
        self.get_query(query.parent.unwrap(), |mgr, parent, mut state| {
            state.have.remove(&query.id);
            if state.block == Some(query.id) {
                state.block = None;
            }
            if have {
                state.providers.push(peer_id);
            }
            if state.block.is_none() && !state.providers.is_empty() {
                state.block = Some(mgr.block(
                    parent.root,
                    parent.id,
                    state.providers.pop().unwrap(),
                    query.cid,
                ));
            }
            if state.have.is_empty() && state.block.is_none() && state.providers.is_empty() {
                if state.providers.is_empty() {
                    return Transition::Complete(Err(query.cid));
                } else {
                    return Transition::Complete(Ok(()));
                }
            }
            Transition::Next(state)
        });
    }

    /// Processes the response of a block query.
    ///
    /// Either completes the get query or processes it like a have query response.
    fn recv_block(&mut self, query: Header, peer_id: PeerId, block: bool) {
        if block {
            self.get_query(query.parent.unwrap(), |_mgr, _parent, mut state| {
                state.providers.push(peer_id);
                Transition::Complete(Ok(()))
            });
        } else {
            self.recv_have(query, peer_id, block);
        }
    }

    /// Processes the response of a missing blocks query.
    ///
    /// Starts a get query for each missing block. If there are no in progress queries
    /// the sync query is marked as complete.
    fn recv_missing_blocks(&mut self, query: Header, missing: Vec<Cid>) {
        let mut num_missing = 0;
        let num_missing_ref = &mut num_missing;
        self.sync_query(query.parent.unwrap(), |mgr, parent, mut state| {
            state.children.remove(&query.id);
            for cid in missing {
                state.missing.insert(mgr.get(
                    Some(parent.root),
                    cid,
                    state.providers.iter().copied(),
                ));
            }
            *num_missing_ref = state.missing.len();
            if state.missing.is_empty() && state.children.is_empty() {
                Transition::Complete(Ok(()))
            } else {
                Transition::Next(state)
            }
        });
        if num_missing != 0 {
            self.events
                .push_back(QueryEvent::Progress(query.root, num_missing));
        }
    }

    /// Processes the response of a get query.
    ///
    /// If it is part of a sync query a new missing blocks query is started. Otherwis