use super::*;
use assertor::{assert_that, IteratorAssertion};
use std::time::Duration;
use std::{iter, thread};

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 8000;

#[test]
fn test_register_broker_multiple() {
    let registry = BrokerRegistry::new();

    let host1 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT);
    let host2 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 1);
    let host3 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 2);

    registry.register_broker(host1.clone());
    registry.register_broker(host2.clone());
    registry.register_broker(host3.clone());

    assert_registered_brokers_are(&registry, vec![host1, host2, host3]);
}

#[test]
fn test_register_broker_duplicate() {
    let registry = BrokerRegistry::new();

    let host1 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT);
    let host2 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 1);
    let host3 = HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 2);

    registry.register_broker(host1.clone());
    registry.register_broker(host2.clone());
    registry.register_broker(host1.clone());
    registry.register_broker(host3.clone());
    registry.register_broker(host1.clone());

    assert_registered_brokers_are(&registry, vec![host1, host2, host3]);
}

#[test]
fn test_register_broker_reads_must_complete_before_writes_take_place() {
    let registry = BrokerRegistry::new();

    registry.register_broker(HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT));

    let registry_clone = registry.clone();
    let writer_thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        registry_clone.register_broker(HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 1));
    });

    let registry_clone = registry.clone();
    let reader_thread = thread::spawn(move || {
        {
            let brokers = registry_clone.brokers();
            thread::sleep(Duration::from_millis(200)); // hold the lock for longer than the writer thread sleeps
            assert_that!(brokers.iter()).contains_exactly_in_order(iter::once(&HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT)));
        }

        thread::sleep(Duration::from_millis(50)); // now wait for the writer to have its turn
        let brokers = registry_clone.brokers();
        assert_that!(brokers.iter()).contains_exactly_in_order(vec![
            HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT),
            HostAndPort::new(DEFAULT_HOST.to_owned(), DEFAULT_PORT + 1),
        ].iter());
    });

    reader_thread.join().unwrap();
    writer_thread.join().unwrap();
}

fn assert_registered_brokers_are(registry: &BrokerRegistry, expected: Vec<HostAndPort>) {
    assert_that!(registry.brokers().iter()).contains_exactly_in_order(expected.iter())
}