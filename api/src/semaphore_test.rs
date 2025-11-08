use std::ops::Mul;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use ntest_timeout::timeout;
use crate::semaphore::Semaphore;

#[test]
#[should_panic(expected = "Capacity must be non-zero")]
fn test_new_zeo_capacity() {
    Semaphore::new(0);
}

#[test]
#[timeout(10)]
fn test_acquires_immediately_when_permits_available() {
    let semaphore = Semaphore::new(3);
    semaphore.acquire();
    semaphore.acquire();
}

#[test]
#[timeout(200)]
fn test_blocks_when_no_permit_available() {
    let semaphore = Arc::new(Semaphore::new(1));
    let semaphore_clone = Arc::clone(&semaphore);

    let release_delay = Duration::from_millis(150);
    thread::spawn(move || {
        thread::sleep(release_delay);
        semaphore_clone.release();
    });

    let start = Instant::now();
    semaphore.acquire();
    assert!(start.elapsed().as_millis() < 10);

    semaphore.acquire();
    assert!(start.elapsed().ge(&release_delay));
}

#[test]
#[timeout(250)]
fn test_several_threads_unblocked_one_at_a_time() {
    let semaphore = Arc::new(Semaphore::new(1));
    semaphore.acquire(); // consume all permits so that the next acquire() blocks

    let semaphore_clone = Arc::clone(&semaphore);
    let handle1 = thread::spawn(move || semaphore_clone.acquire());

    let semaphore_clone = Arc::clone(&semaphore);
    let handle2 = thread::spawn(move || semaphore_clone.acquire());

    let start = Instant::now();
    let release_delay = Duration::from_millis(100);

    thread::sleep(release_delay);
    semaphore.release();

    thread::sleep(release_delay);
    semaphore.release();

    handle1.join().unwrap();
    handle2.join().unwrap();

    assert!(start.elapsed().ge(&release_delay.mul(2)));
}

#[test]
#[timeout(200)]
fn test_does_not_exceed_max_capacity() {
    let semaphore = Arc::new(Semaphore::new(1));
    let semaphore_clone = Arc::clone(&semaphore);
    semaphore.release();
    semaphore.release();

    let release_delay = Duration::from_millis(100);
    thread::spawn(move || {
        thread::sleep(release_delay);
        semaphore_clone.release();
    });

    let start = Instant::now();
    semaphore.acquire();
    assert!(start.elapsed().as_millis() < 10);

    semaphore.acquire();
    assert!(start.elapsed().ge(&release_delay));
}