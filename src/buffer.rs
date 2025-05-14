use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub struct LogBuffer {
    buffer: Box<Vec<u8>>,
    pos: AtomicUsize
}

impl LogBuffer {
    pub fn new(buffer: Box<Vec<u8>>) -> Arc<Self> {
        return Arc::new(LogBuffer {
            buffer,
            pos: AtomicUsize::new(0),
        });
    }

    pub fn is_full(&self) -> bool {
        return self.pos.load(Ordering::Acquire) == self.size();
    }

    pub fn size(&self) -> usize {
        return self.buffer.len()
    }

    pub fn write(&self, pos: usize, data: &[u8]) {
        assert!(!data.is_empty(), "Cannot write empty data")

    }


}