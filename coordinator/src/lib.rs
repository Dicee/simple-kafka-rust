//! This crate mocks a coordinator service fulfilling functions such as service discovery, metadata and state management.
//! In the real world, we would use a proper and/or something like etcd/ZooKeeper. More information about this decision
//! in the global README (out-of-scope features).

pub mod dao;