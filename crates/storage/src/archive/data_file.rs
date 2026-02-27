//! Implement the core file IO abstraction.

use std::{
    fs::{self, File, OpenOptions},
    io,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::archive::error::rename::RenameError;

const READ_BUFFER_SIZE: usize = 16 * 1024; // 16kb
const WRITE_BUFFER_SIZE: usize = 16 * 1024; // 16kb

/// Wrapper around a file that implements read and write buffers and manages
/// them seamlessly via standard IO traits.
#[derive(Debug)]
pub struct DataFile {
    data_file: File,
    data_file_path: PathBuf,
    data_file_end: u64,
    write_buffer: Vec<u8>,
    read_buffer: Vec<u8>,
    read_buffer_start: u64,
    read_buffer_len: usize,
    seek_pos: u64,
    read_buffer_size: u32,
    write_buffer_size: u32,
    remove_on_drop: bool,
    read_only: bool,
}

impl DataFile {
    /// Open a new data file, read only if ro is true.
    pub fn open<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, io::Error> {
        let path = path.as_ref();
        if !read_only {
            // If we are opening for write then make sure the file exists.
            // This function will create it if it does not exist or produce
            // an error if it does so ignore the errors.
            let _ = File::create_new(path);
        }
        let mut data_file = OpenOptions::new().read(true).append(!read_only).open(path)?;
        let data_file_end = data_file.seek(SeekFrom::End(0))?;
        let write_buffer = if read_only {
            // If opening read only won't need capacity.
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
            data_file_path: path.to_owned(),
            data_file_end,
            write_buffer,
            read_buffer,
            read_buffer_start: 0,
            read_buffer_len,
            seek_pos: 0,
            read_buffer_size: READ_BUFFER_SIZE as u32,
            write_buffer_size: WRITE_BUFFER_SIZE as u32,
            remove_on_drop: false,
            read_only,
        })
    }

    /// Return the path to this file.
    pub fn path(&self) -> &Path {
        &self.data_file_path
    }

    /// The files end position (i.e. bytes on disk but not any buffered bytes).
    pub fn data_file_end(&self) -> u64 {
        self.data_file_end
    }

    /// Size of the file (on disk plus unwritten buffered bytes).
    pub fn len(&self) -> u64 {
        self.data_file_end + self.write_buffer.len() as u64
    }

    /// Is the data file empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the file length by truncating or extending.
    /// Used to truncate an incomplete record.
    pub fn set_len(&mut self, len: u64) -> Result<(), io::Error> {
        if !self.read_only && !self.write_buffer.is_empty() {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        self.data_file.set_len(len)?;
        self.data_file_end = len;
        if !self.read_only {
            // Reopen file so the append cursor will be correct.
            self.data_file =
                OpenOptions::new().read(true).append(!self.read_only).open(&self.data_file_path)?;
        }
        if self.read_buffer_start + self.read_buffer_len as u64 > len {
            if self.data_file_end > 0 {
                self.data_file.rewind()?;
                if self.data_file_end < READ_BUFFER_SIZE as u64 {
                    self.data_file
                        .read_exact(&mut self.read_buffer[..self.data_file_end as usize])?;
                    self.read_buffer_len = self.data_file_end as usize;
                } else {
                    self.data_file.read_exact(&mut self.read_buffer[..])?;
                    self.read_buffer_len = READ_BUFFER_SIZE;
                }
            } else {
                self.read_buffer_len = 0;
            }
            self.read_buffer_start = 0;
        }
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

    /// Delete the file.
    pub fn delete(mut self) {
        self.remove_on_drop = true;
    }

    /// Rename the underlying file.
    pub fn rename<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RenameError> {
        let path = path.as_ref();
        if self.data_file_path == path {
            return Ok(());
        }
        if path.exists() {
            return Err(RenameError::FilesExist);
        }
        let res = fs::rename(&self.data_file_path, path);
        if res.is_ok() {
            self.data_file_path = path.to_owned();
        }
        res.map_err(RenameError::RenameIO)
    }

    /// Copy bytes from the read buffer into buf.  This expects seek_pos to be within the
    /// read_buffer (will panic if called incorrectly).
    fn copy_read_buffer(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut size = buf.len();
        // The read buffer will never be larger than a u32 so this should be fine even on a 32bit
        // platform.
        let read_depth = (self.seek_pos - self.read_buffer_start) as usize;
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
    /// the file, write buffer or the read buffer.  The file and write buffer will not have
    /// overlapping records so this will not read across them in one call.  This will not happen
    /// on a proper DB although the Read contract should handle this fine.
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
        } else if self.seek_pos >= self.read_buffer_start
            && self.seek_pos < (self.read_buffer_start + self.read_buffer_len as u64)
        {
            self.copy_read_buffer(buf)
        } else {
            let mut seek_pos = self.seek_pos;
            let mut end = self.data_file_end - seek_pos;
            if end < self.read_buffer_size as u64 {
                // If remaining bytes are less then the buffer pull back seek_pos to fill the
                // buffer.
                seek_pos = self.data_file_end.saturating_sub(self.read_buffer_size as u64);
            } else {
                // Put the seek position in the mid point of the read buffer.  This might help
                // increase buffer hits or might do nothing or hurt depending on
                // fetch patterns.
                seek_pos = seek_pos.saturating_sub((self.read_buffer_size / 2) as u64);
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
                self.read_buffer_start = seek_pos;
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
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek to negative position",
                    ));
                }
            }
            SeekFrom::Current(pos) => {
                let end = self.seek_pos as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek to negative position",
                    ));
                }
            }
        }
        Ok(self.seek_pos)
    }
}

impl Write for DataFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.read_only {
            Err(io::Error::new(io::ErrorKind::ReadOnlyFilesystem, "file not open for write"))
        } else {
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
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.read_only {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        Ok(())
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        if self.remove_on_drop {
            if let Err(e) = fs::remove_file(&self.data_file_path) {
                if !std::thread::panicking() {
                    tracing::error!("DataFile: failed to remove file on drop: {e}");
                }
            }
        } else {
            // If read only the flush will just return Ok(())
            if let Err(e) = self.flush() {
                if !std::thread::panicking() {
                    tracing::error!("DataFile: failed to flush on drop: {e}");
                }
            }
        }
    }
}
