//! This crate mocks a coordinator service fulfilling functions such as service discovery, metadata and state management.
//! In the real world, we would use a proper and/or something like etcd/ZooKeeper. More information about this decision
//! in the global README (out-of-scope features).

pub mod client;
pub mod model;

pub mod mock;

// some re-exports to unify how users import a client for any of the services in our project
pub use client::Client as Client;
pub use client::ClientImpl as ClientImpl;
pub use client::MockClient as MockClient;
pub use mock::DummyClient as DummyClient;