//! Implement the core file IO abstraction.

use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const READ_BUFFER_SIZE: usize = 16 * 1024; // 16kb
const WRITE_BUFFER_SIZE: usize = 16 * 1024; // 16kb

/// Wrapper around a file that implements read and write buffers and manages
/// them seamlessly via standard IO traits.
#[derive(Debug)]
pub struct DataFile {
    data_file: File,
    data_file_end: u64,
    write_buffer: Vec<u8>,
    read_buffer: Vec<u8>,
    read_buffer_start: usize,
    read_buffer_len: usize,
    seek_pos: u64,
    read_buffer_size: u32,
    write_buffer_size: u32,
}

impl DataFile {
    /// Open a new data file, read only if ro is true.
    pub fn open<P: AsRef<Path>>(path: P, ro: bool) -> Result<Self, io::Error> {
        /*XXXXif config.truncate && config.write {
            // truncate is incompatible with append so truncate then open for append.
            OpenOptions::new()
                .write(true)
                .create(config.create)
                .truncate(true)
                .open(config.files.data_path())?;
        }*/
        let mut data_file = OpenOptions::new()
            .read(true)
            .append(!ro)
            //XXXX.create(config.create && config.write)
            .open(path.as_ref())?; //XXXXconfig.files.data_path())?;
        data_file.seek(SeekFrom::End(0))?;
        let data_file_end = data_file.stream_position()?;
        let write_buffer = if ro {
            // If opening read only wont need capacity.
            Vec::new()
        } else {
            Vec::with_capacity(WRITE_BUFFER_SIZE)
        };
        let mut read_buffer = vec![0; READ_BUFFER_SIZE];
        // Prime the read buffer so we don't have to check if it is empty, etc.
        let mut read_buffer_len = 0_usize;
        if data_file_end > 0 {
            data_file.rewind()?;
            if data_file_end < READ_BUFFER_SIZE as u64 {
                data_file.read_exact(&mut read_buffer[..data_file_end as usize])?;
                read_buffer_len = data_file_end as usize;
            } else {
                data_file.read_exact(&mut read_buffer[..])?;
                read_buffer_len = READ_BUFFER_SIZE;
            }
        }
        Ok(Self {
            data_file,
            data_file_end,
            write_buffer,
            read_buffer,
            read_buffer_start: 0,
            read_buffer_len,
            seek_pos: 0,
            read_buffer_size: READ_BUFFER_SIZE as u32,
            write_buffer_size: WRITE_BUFFER_SIZE as u32,
        })
    }

    /// The files end position (i.e. bytes on disk but not any buffered bytes).
    pub fn data_file_end(&self) -> u64 {
        self.data_file_end
    }

    /// Size of the file (on disk plus unwritten buffered bytes).
    pub fn len(&self) -> u64 {
        self.data_file_end + self.write_buffer.len() as u64
    }

    /// Set the file length by truncating or extending.
    /// Used to truncate an incomplete record.
    pub fn set_len(&mut self, len: u64) -> Result<(), io::Error> {
        self.data_file.set_len(len)?;
        self.data_file_end = len;
        Ok(())
    }

    /// Attempt to clone and return the underlying file.
    pub fn try_clone(&self) -> Result<File, io::Error> {
        self.data_file.try_clone()
    }

    /// Sync to disk.
    pub fn sync_all(&self) -> Result<(), io::Error> {
        self.data_file.sync_all()
    }

    /// Refresh the data_file_end, useful for readonly DBs to sync.
    pub fn refresh_data_file_end(&mut self) {
        self.data_file_end = self.data_file.seek(SeekFrom::End(0)).unwrap_or(self.data_file_end);
    }

    /// Copy bytes form the read buffer into buf.  This expects seek_pos to be within the
    /// read_buffer (will panic if called incorrectly).
    fn copy_read_buffer(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut size = buf.len();
        let read_depth = self.seek_pos as usize - self.read_buffer_start;
        if read_depth + size > self.read_buffer_len {
            size = self.read_buffer_len - read_depth;
        }
        buf[..size].copy_from_slice(&self.read_buffer[read_depth..read_depth + size]);
        self.seek_pos += size as u64;
        if size == 0 {
            panic!("Invalid call to from_read_buffer, size: {}, read buffer index: {}, seek pos: {}, read buffer start: {}",
                   size, read_depth, self.seek_pos, self.read_buffer_start);
        }
        Ok(size)
    }
}

impl Read for DataFile {
    /// Read for the DbInner.  This allows other code to not worry about whether data is read from
    /// the file, write buffer or the read buffer.  The file and write buffer will not have overlapping records
    /// so this will not read across them in one call.  This will not happen on a proper DB although
    /// the Read contract should handle this fine.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.seek_pos >= self.data_file_end {
            let write_pos = (self.seek_pos - self.data_file_end) as usize;
            if write_pos < self.write_buffer.len() {
                let mut size = buf.len();
                if write_pos + size > self.write_buffer.len() {
                    size = self.write_buffer.len() - write_pos;
                }
                buf[..size].copy_from_slice(&self.write_buffer[write_pos..write_pos + size]);
                self.seek_pos += size as u64;
                Ok(size)
            } else {
                Ok(0)
            }
        } else if self.seek_pos >= self.read_buffer_start as u64
            && self.seek_pos < (self.read_buffer_start + self.read_buffer_len) as u64
        {
            self.copy_read_buffer(buf)
        } else {
            let mut seek_pos = self.seek_pos;
            let mut end = self.data_file_end - seek_pos;
            if end < self.read_buffer_size as u64 {
                // If remaining bytes are less then the buffer pull back seek_pos to fill the buffer.
                seek_pos = if self.data_file_end > self.read_buffer_size as u64 {
                    self.data_file_end - self.read_buffer_size as u64
                } else {
                    0
                };
            } else {
                // Put the seek position in the mid point of the read buffer.  This might help increase
                // buffer hits or might do nothing or hurt depending on fetch patterns.
                seek_pos = if seek_pos > (self.read_buffer_size / 2) as u64 {
                    seek_pos - (self.read_buffer_size / 2) as u64
                } else {
                    0
                };
            }
            end = self.data_file_end - seek_pos;
            if end > 0 {
                self.data_file.seek(SeekFrom::Start(seek_pos))?;
                if end < self.read_buffer_size as u64 {
                    self.data_file.read_exact(&mut self.read_buffer[..end as usize])?;
                    self.read_buffer_len = end as usize;
                } else {
                    self.data_file.read_exact(&mut self.read_buffer[..])?;
                    self.read_buffer_len = self.read_buffer_size as usize;
                }
                self.read_buffer_start = seek_pos as usize;
                self.copy_read_buffer(buf)
            } else {
                Ok(0)
            }
        }
    }
}

impl Seek for DataFile {
    /// Seek on the DbInner treating the file and write cache as one byte array.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => self.seek_pos = pos,
            SeekFrom::End(pos) => {
                let end = (self.data_file_end + self.write_buffer.len() as u64) as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
            SeekFrom::Current(pos) => {
                let end = self.seek_pos as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
        }
        Ok(self.seek_pos)
    }
}

impl Write for DataFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.write_buffer.len() >= self.write_buffer_size as usize {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        let write_buffer_len = self.write_buffer.len();
        let write_capacity = self.write_buffer_size as usize - write_buffer_len;
        let buf_len = buf.len();
        if write_capacity > buf_len {
            self.write_buffer.write_all(buf)?;
            Ok(buf_len)
        } else {
            self.write_buffer.write_all(&buf[..write_capacity])?;
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
            Ok(write_capacity)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.data_file.write_all(&self.write_buffer)?;
        self.data_file_end += self.write_buffer.len() as u64;
        self.write_buffer.clear();
        Ok(())
    }
}
