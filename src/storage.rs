
#[derive(Debug, thiserror::Error)]
pub enum  Error {
    #[error("IO error: {0}")]
    IoError(std::io::Error),
    #[error("Storage is full")]
    StorageFull,
    #[error("Buffer is full")]
    BufferFull,
    #[error("No available buffers")]
    NoAvailableBuffers,
}


#[async_trait::async_trait]
pub trait  Store: Send + Sync {
    async fn persist<'a>(&'a mut self, data: &'a [u8]) ->  Result<usize, Error>;
    async fn flush(&mut self) -> Result<LSN, Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LSN(u64);

impl LSN {
    pub fn new(value: u64) -> Self {
        LSN(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn advance(&self, by: u64) -> Self {
        Self(self.0 + by)
    }
}





