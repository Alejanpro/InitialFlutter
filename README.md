[![Crates.io](https://img.shields.io/crates/v/libp2p-bitswap.svg)](https://crates.io/crates/libp2p-bitswap)
[![docs.rs](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/libp2p-bitswap)

# libp2p-bitswap

Implementation of the bitswap protocol.

## Efficiently syncing dags of blocks

Bitswap is a very simple protocol. It was adapted and simplified for ipfs-embed. The message
format can be represented by the following enums.

```rust
pub enum BitswapRequest {
    Have(Cid),
    Block(Cid),
}

pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}
```

The mechanism for locating providers can be abstracted. A dh