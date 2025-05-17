/*

 LogBuffer {
   buffer AtomicUsize
   write_pos Box<Vec<u8>>

 }

*/
mod buffer;

mod storage;

mod log;

mod test_util;