use crate::stats::{REQUESTS_TOTAL, REQUEST_DURATION_SECONDS};
use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use libp2p::PeerId;
use prometheus::HistogramTimer;
use std::collecti