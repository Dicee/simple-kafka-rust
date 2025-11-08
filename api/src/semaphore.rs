use std::sync::{Condvar, Mutex};

#[cfg(test)]
#[path = "semaphore_test.rs"]
mod semaphore_test;

/// Simple semaphore. I don't want to depend on third-party crates for just one thing like a semaphore or a bounded channel, so I'll just implement
/// a simple semaphore to guard access to a channel. This semaphore is bounded, meaning that it can never exceed a certain amount of permits.
pub struct Semaphore {
    capacity: usize,
    count: Mutex<usize>,
    condvar: Condvar,
}

impl Semaphore {
    /// Creates a new [Semaphore] with the specified capacity, i.e. the max amount of permits the semaphore will ever have. The semaphore gets initialized
    /// with a number of permits equal to this capacity.
    pub fn new(capacity: usize) -> Self {
        if capacity == 0 { panic!("Capacity must be non-zero"); }
        Self {
            capacity,
            count: Mutex::new(capacity),
            condvar: Condvar::new(),
        }
    }

    /// Blocks the current thread until a permit is available.
    pub fn acquire(&self) {
        let mut count = self.condvar.wait_while(self.count.lock().unwrap(), |c| *c == 0).unwrap();
        *count -= 1;
    }

    /// Releases one permit if and only if the number of available permits is smaller than the max capacity. Only one waiter will be woken up by calling
    /// this method.
    pub fn release(&self) {
        let mut count = self.count.lock().unwrap();
        if *count < self.capacity {
            *count += 1;
            self.condvar.notify_one();
        }
    }
}
