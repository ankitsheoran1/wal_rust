use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task;
use crate::buffer::LogBuffer;
use crate::storage::{Error, Store, LSN};
use crate::test_util::TestStorage;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SegmentState {
    Queued,
    Active,
    Writing,
}

struct Segment {
    buffer: Arc<LogBuffer>,
    state_and_count: AtomicI64,
    base_lsn: AtomicU64
}

impl Segment {
    const STATE_MASK: i64 = 0b11;
    const COUNT_SHIFT: i64= 2;
    const COUNT_INC: i64 = 1 << Self::COUNT_SHIFT;

    const QUEUED: i64 = 0;
    const ACTIVE: i64 = 1;
    const WRITING: i64 = 2;

    fn new(buffer: Arc<LogBuffer>, state: SegmentState, base_lsn: u64) -> Self {
        let state_val = match state {
            SegmentState::Queued => Self::QUEUED,
            SegmentState::Active => Self::ACTIVE,
            SegmentState::Writing => Self::WRITING
        };
        Self {
            buffer,
            state_and_count: AtomicI64::new(state_val),
            base_lsn: AtomicU64::new(base_lsn),
        }
    }

    fn get_state(&self) -> SegmentState {
        match self.state_and_count.load(Acquire) & Self::STATE_MASK {
            Self::QUEUED => SegmentState::Queued,
            Self::ACTIVE => SegmentState::Active,
            Self::WRITING => SegmentState::Writing,
            _ => unreachable!("Invalid state value"),
        }
    }

    fn get_writer_count(&self) -> i64 {
        self.state_and_count.load(Ordering::Acquire) >> Self::COUNT_SHIFT
    }

    fn set_state(&self, state: SegmentState) {
        let state_val = match state {
            SegmentState::Queued => Self::QUEUED,
            SegmentState::Active => Self::ACTIVE,
            SegmentState::Writing => Self::WRITING,
        };

        let old = self.state_and_count.load(Acquire);
        let new = (old & !Self::STATE_MASK) | state_val;
        self.state_and_count.store(new, Release);
    }

    fn try_to_reserve_space(&self, len: usize) -> Option<(usize, usize)> {
        assert!(len > 0, "invalid space");
        loop {
            let current = self.state_and_count.load(Ordering::Acquire);
            let state = current & Self::STATE_MASK;
            if state != Self::ACTIVE {
                return None
            }

            // writer count
            let new = current + Self::COUNT_INC;

            match self.state_and_count.compare_exchange(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    match self.buffer.try_to_save_space(len) {
                        Some(result) => return Some(result),
                        None => {
                            // Couldn't reserve space, decrement count
                            self.state_and_count.fetch_sub(Self::COUNT_INC, Ordering::Release);
                            return None;
                        }
                    }
                }
                Err(_) => continue,
            }
        }
    }

    fn write(&self, pos: usize, data: &[u8])  {
        println!("inside segment write funtion with pos and len is {} {}", pos, data.len());
        let current = self.state_and_count.load(Ordering::Acquire);
        println!("inside segment write funtion with state  is {}", (current & Self::STATE_MASK));
        if (current & Self::STATE_MASK) != Self::ACTIVE {
            return;
        }
        self.buffer.write(pos, data);
        println!("inside segment buffer written");
        self.state_and_count.fetch_sub(Self::COUNT_INC, Ordering::Release);
        println!("returning from seg write function");
    }

    fn buffer(&self) -> &[u8] {
        self.buffer.buffer()
    }

    fn base_lsn(&self) -> u64 {
        self.base_lsn.load(Ordering::Acquire)
    }

    fn set_base_lsn(&self, lsn: u64) {
        self.base_lsn.store(lsn, Ordering::Release);
    }

    fn clear(&self) {
        self.buffer.clear();
    }

    fn get_lsn(&self, pos: usize) -> LSN {
        LSN::new(self.base_lsn.load(Ordering::Acquire) + pos as u64)
    }


}

pub struct Log<T: Store> {
    storage: Arc<Mutex<T>>,
    current_index: AtomicUsize,
    log_segments: Box<[Segment]>,
    rotate_in_progress: AtomicBool,
}

impl<T: Store> Log<T> {

    fn create_segments(num_segments: usize, segment_size: usize, initial_lsn: u64) -> Box<[Segment]>{
        let mut segments = Vec::with_capacity(num_segments);
        for i in 0..num_segments {
            let buffer = Box::new(vec![0; segment_size]);
            let state = if i == 0 { SegmentState::Active } else { SegmentState::Queued };
            segments.push(Segment::new(LogBuffer::new(buffer), state, initial_lsn));

        }
        segments.into_boxed_slice()
    }

    fn get_segment(&self, index: usize) -> &Segment {
        &self.log_segments[index]
    }

    fn get_current_index(&self) -> usize {
        self.current_index.load(Ordering::Acquire)
    }

    fn new(lsn: LSN, num_segments: usize, segment_size: usize, storage: T) -> Self {
        assert!(num_segments > 1, "Must have at least 2 segments");
        Self {
            log_segments: Self::create_segments(num_segments, segment_size, lsn.value()),
            current_index: AtomicUsize::new(0),
            storage: Arc::new(Mutex::new(storage)),
            rotate_in_progress: AtomicBool::new(false),
        }
    }

    async fn rotate(&self) -> Result<(), Error> {
        let curr_idx = self.get_current_index();
        let curr_seg = self.get_segment(curr_idx);
        let next_idx = (curr_idx + 1) % self.log_segments.len();
        let next_segment = self.get_segment(next_idx);

        if !self.rotate_in_progress.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        ).is_ok() {
            if next_segment.get_state() == SegmentState::Active {
                return Ok(());  // Can proceed with next segment
            }
            task::yield_now().await;
            return Ok(());  // Let other tasks try
        }

        loop {
            let current = curr_seg.state_and_count.load(Ordering::Acquire);
            let state = current & Segment::STATE_MASK;
            let count = current >> Segment::COUNT_SHIFT;

            // If not ACTIVE or writers still present, wait
            if state != Segment::ACTIVE || count > 0 {
                task::yield_now().await;
                continue;
            }
            let old_state = (0 << Segment::COUNT_SHIFT) | Segment::ACTIVE;
            let new_state = (0 << Segment::COUNT_SHIFT) | Segment::WRITING;

            if curr_seg.state_and_count.compare_exchange(
                old_state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                next_segment.set_state(SegmentState::Active);
                self.current_index.store(next_idx, Ordering::Release);
                let next_base_lsn = curr_seg.base_lsn() + curr_seg.buffer.pos() as u64;
                next_segment.set_base_lsn(next_base_lsn);

                let mut storage = self.storage.lock().await;
                let _persisted_size = storage.persist(curr_seg.buffer()).await?;
                curr_seg.clear();
                drop(storage);
                curr_seg.set_state(SegmentState::Queued);
                self.rotate_in_progress.store(false, Ordering::Release);
                return Ok(());

            }
        }
    }

    async fn write(&self, data: &[u8]) -> Result<LSN, Error> {
        assert!(!data.is_empty(), "Cannot write empty data");
        let mut remaining_data = data;
        let mut write_lsn = None;

        while !remaining_data.is_empty() {
            let curr_idx = self.get_current_index();
            let curr_seg = self.get_segment(curr_idx);
            if curr_seg.get_state() != SegmentState::Active {
                // Segment not active, yield and retry
                task::yield_now().await;
                continue;
            }

            let Some((pos, len)) = curr_seg.try_to_reserve_space(remaining_data.len()) else {
                self.rotate().await?;
                continue;
            };

            if write_lsn.is_none() {
                write_lsn = Some(curr_seg.get_lsn(pos));
            }
            if len == remaining_data.len() {
                curr_seg.write(pos, remaining_data);
                break;

            } else {
                curr_seg.write(pos, &remaining_data[..len]);
                remaining_data = &remaining_data[len..];
                self.rotate().await?;

            }
        }

        Ok(write_lsn.unwrap())

    }

    pub fn lsn(&self) -> LSN {
        let current_idx = self.current_index.load(Ordering::Acquire);
        let log_segment = &self.log_segments[current_idx];
        LSN::new(log_segment.get_lsn(log_segment.buffer.pos()).value())
    }

    async fn persist(&self) -> Result<LSN, Error> {
        let curr_idx = self.get_current_index();
        let curr_seg = self.get_segment(curr_idx);
        if curr_seg.get_state() == SegmentState::Active && curr_seg.buffer.pos() > 0 {
            self.rotate().await?;
        }
        let result = self.storage.lock().await.flush().await;
        if let Ok(_) = &result {
            result
        } else {
            Ok(LSN::new(self.lsn().value()))
        }

    }

    pub async fn flush(&self) -> Result<LSN, Error> {
        self.persist().await?;
        self.storage.lock().await.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_write() {
        let storage = TestStorage::new(false);
        let log = Log::new(LSN::new(0), 16, 2, storage);

        let data = b"0123456789";
        let lsn = log.write(data).await.unwrap();
        log.flush().await.unwrap();
        assert_eq!(lsn.value(), 0);
    }

    #[tokio::test]
    async fn test_large_dataset() {
        let storage = TestStorage::new(false);
        let log =  Log::new(LSN::new(0), 2, 1, storage);
        let data = b"0123456789";
        let lsn = log.write(data).await.unwrap();
        log.flush().await.unwrap();
        assert_eq!(lsn.value(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let storage = TestStorage::new(false);
        let log = Arc::new(Log::new(LSN::new(0), 2, 1, storage));

        let mut handles = vec![];
        let lsns = Arc::new(Mutex::new(vec![]));

        for i in 0..9 {
            let log = log.clone();
            let lsns = lsns.clone();
            let data = format!("{} data", i);
            handles.push(tokio::spawn(async move {
                let lsn = log.write(data.as_bytes()).await.unwrap();
                lsns.lock().await.push((i, lsn));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Get the LSNs in order of completion
        let captured_lsns = lsns.lock().await;
        println!("Writes completed in order: {:?}", captured_lsns);

        // Verify LSNs are monotonically increasing
        for window in captured_lsns.windows(2) {
            if let [(_, lsn1), (_, lsn2)] = window {
                assert!(lsn2.value() > lsn1.value(),
                        "LSNs should be monotonically increasing");
            }
        }

        // Flush and verify all data was written
        log.flush().await.unwrap();

        let log_data = log.storage.lock().await.get_written_data().await;
        //
        for (i, lsn) in captured_lsns.iter() {
            let expected_data = format!("{} data", i);
            let start_pos = lsn.value() as usize;
            let end_pos = start_pos + expected_data.len();
            let actual_data = &log_data[start_pos..end_pos];
            assert_eq!(actual_data, expected_data.as_bytes());
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Cannot write empty data")]
    async fn test_empty_writes() {
        let storage = TestStorage::new(false);
        let log = Arc::new(Log::new(LSN::new(0), 2, 1, storage));

        // Writing empty data should panic
        log.write(b"").await.unwrap();
    }

}


