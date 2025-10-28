pub mod model;

mod client;

// some re-exports to unify how users import a client for any of the services in our project
pub use client::Client as Client;
pub use client::ClientImpl as ClientImpl;
pub use client::MockClient as MockClient;
pub use client::Result;
pub use client::Error;
