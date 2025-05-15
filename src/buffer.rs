use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{AcqRel, Acquire};

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

    // pub fn size(&self) -> usize {
    //     return self.buffer.len()
    // }

    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    pub fn write(&self, pos: usize, data: &[u8]) {
        assert!(!data.is_empty(), "Cannot write empty data");
        assert!(pos + data.len() <= self.size(), "size is not available");

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.buffer[pos..].as_ptr() as *mut u8,
                data.len(),
            )
        }

    }

    pub fn try_to_save_space(&self, len: usize) -> Option<(usize, usize)> {
            let write_pos = self.pos.load(Ordering::Acquire);
            let available_space = self.size() - write_pos;

            if available_space <= write_pos {
                match self.pos.compare_exchange(
                    write_pos,
                    write_pos + len,
                    AcqRel,
                    Acquire,
                ) {
                    Ok(_) => Some((write_pos, len)),
                    Err(_) => None
                }
            } else if available_space > 0 {
                match self.pos.compare_exchange(
                    write_pos,
                    self.size(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => Some((write_pos, len)),
                    Err(_) => None
                }
            } else {
                None
            }
    }

    pub fn clear(&self) {
        self.pos.store(0, Ordering::Release);
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_buffer() {
        let data = "0123456789";
        let buffer = Box::new(vec![0; 10]);
        let log_buffer = LogBuffer::new(buffer);
        assert_eq!(log_buffer.size(), 10);
        assert!(!log_buffer.is_full());


    }

}