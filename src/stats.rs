
use lazy_static::lazy_static;
use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts};

lazy_static! {
    pub static ref REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "bitswap_requests_total",
            "Number of bitswap requests labelled by type and result.",
        ),
        &["type"],
    )
    .unwrap();
    pub static ref REQUEST_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "bitswap_request_duration_seconds",
            "Duration of bitswap requests labelled by request type",
        ),
        &["type"],
    )
    .unwrap();
    pub static ref REQUESTS_CANCELED: IntCounter = IntCounter::new(
        "bitswap_requests_canceled_total",
        "Number of canceled requests",
    )