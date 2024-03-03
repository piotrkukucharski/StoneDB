use std::ops::Add;
use chrono::{DateTime, Duration, TimeZone, Utc};

pub trait Clock {
    type Timezone: TimeZone;
    fn now(&mut self) -> DateTime<Self::Timezone>;
    fn now_as_nanos(&mut self) -> i64;
}

pub struct MockClock {
    current_time: DateTime<Utc>,
    step: Duration,
}

pub struct ChronoUtcSystemClock;

impl Clock for ChronoUtcSystemClock {
    type Timezone = Utc;

    fn now(&mut self) -> DateTime<Self::Timezone> {
        chrono::offset::Utc::now()
    }

    fn now_as_nanos(&mut self) -> i64 {
        self.now().timestamp_nanos_opt().unwrap()
    }
}

impl Clock for MockClock {
    type Timezone = Utc;

    fn now(&mut self) -> DateTime<Self::Timezone> {
        let time = self.current_time;
        self.increase();
        time
    }
    fn now_as_nanos(&mut self) -> i64 {
        let time = self.current_time;
        self.increase();
        time.timestamp_nanos_opt().unwrap()
    }
}

impl MockClock {
    pub fn new(start_time: DateTime<Utc>, step: Duration) -> MockClock {
        MockClock {
            current_time: start_time,
            step,
        }
    }
    pub fn from_seconds(start_timestamp: i64, step: i64) -> MockClock {
        MockClock {
            current_time: DateTime::from_timestamp(start_timestamp, 0).unwrap(),
            step: Duration::seconds(step),
        }
    }

    fn increase(&mut self) {
        self.current_time = self.current_time.add(self.step);
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use chrono::{DateTime, Duration};
    use crate::clock::{Clock, MockClock};

    #[test]
    fn test_mock_clock() {
        let start_time = DateTime::from_timestamp(1, 0).unwrap();
        let mut clock = MockClock::new(start_time, Duration::seconds(1));
        assert_eq!(start_time.timestamp_nanos_opt().unwrap(), clock.now_as_nanos());
        assert_eq!(start_time.add(Duration::seconds(1)).timestamp_nanos_opt().unwrap(), clock.now_as_nanos());
    }
}
