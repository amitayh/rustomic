pub struct Instant(pub u64);

pub trait Clock {
    fn now(&self) -> u64;
}

#[derive(Default)]
pub struct MockClock {
    now: u64,
}

impl Clock for MockClock {
    fn now(&self) -> u64 {
        self.now
    }
}

impl MockClock {
    pub fn new() -> Self {
        Self { now: 0 }
    }
}
