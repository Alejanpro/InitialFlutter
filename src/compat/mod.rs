mod message;
mod prefix;
mod protocol;

pub use message::CompatMessage;
pub use protocol::{CompatProtocol, InboundMessage};

fn other<E: std::error::Error + Send + Sync + 'static>