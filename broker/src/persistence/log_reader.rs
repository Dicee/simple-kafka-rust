use crate::persistence::indexing;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[cfg(test)]
#[path = "log_reader_test.rs"]
mod log_reader_test;

/// Simple wrapper around a [BufReader] backed by a file to perform the basic operations we need on a log file
/// (seeking an offset from the current position, reading a given amount of bytes from the current position etc).
struct LogReader {
    reader: BufReader<File>,
}

impl LogReader {
    fn open_at_offset(path: &Path, byte_offset: u64) -> io::Result<Self> {
        let mut log_reader = Self::open(path)?;
        log_reader.reader.seek(SeekFrom::Start(byte_offset))?;
        Ok(log_reader)
    }

    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self { reader: BufReader::new(File::open(path)?) })
    }

    /// Seeks the given amount of offset bytes (can be negative) from the current position.
    fn seek(&mut self, byte_offset: i64) -> io::Result<()> {
        // more efficient than seek for this use case, as it reuses the existing buffer when it can
        self.reader.seek_relative(byte_offset)
    }

    /// Reads exactly the specified amount of bytes from the current position and returns them as a vector.
    /// # Errors
    /// This will fail if EOF is reached before consuming the requested amount of bytes. Of course, any other IO failure will
    /// also cause this function to return an error.
    fn read_exact(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?; // will fail if EOF is reached before we can fill the buffer
        Ok(buf)
    }

    /// Attempts reading exactly 8 bytes from the current position and parses it as a [u64] value if exactly 8 bytes were read. Otherwise, the method will
    /// reset the reader in the state it was before attempting the read.
    fn try_read_u64_le(&mut self) -> io::Result<Option<u64>> {
        let mut buf = [0u8; 8];
        let len_read = self.reader.read(&mut buf)?;
        Ok(if len_read < 8 {
            // Note that this discards the buffer, which we'd normally want to avoid but here it's required otherwise the next call to any try_read_u64_le
            // may not attempt reading from the disk as the buffer could already be full. In this case, we can only have 7 bytes at most in the buffer beyond
            // the current position, so we're not losing much. It's still possible that the user will want to re-read bytes before the current position, in
            // which case tough luck, we'll make another disk call...
            self.reader.seek(SeekFrom::Current(-(len_read as i64)))?;
            None
        } else {
            Some(u64::from_le_bytes(buf))
        })
    }

    /// Discards the buffer and goes back to the first byte in the file
    fn reset_to_start(&mut self) -> io::Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    //noinspection RsSelfConvention
    fn is_eof(&mut self) -> io::Result<bool> {
        self.reader.fill_buf().map(|bytes| bytes.is_empty())
    }
}

/// This reader understands the rotation format of [RotatingAppendOnlyLog]. It assumes that it is being called with long polling, which allows it
/// to perform operations such as checking the existence of the next file on every call where a read was requested but the end of the file was reached.
/// If this code was called in a tight loop, this would be inefficient and we'd rather want to add a blocking notification from the writer thread.
/// However, this is very tricky to do since there can be multiple readers from different consumer groups, and they're not necessarily up to speed with
/// the producer. With this assumption we're making, we can make the read rotation logic simple, and performance shouldn't suffer.
pub struct RotatingLogReader {
    log_path: PathBuf,
    log_reader: LogReader,
}

impl RotatingLogReader {
    /// Opens a log file with the provided start index, at the first byte of the file.
    pub fn open_for_index(root_path: String, index: u64) -> io::Result<Self> {
        Self::open_at_offset(super::get_log_path(&root_path, index), 0)
    }

    /// Opens a specific log path at the given byte offset within that file.
    pub fn open_at_offset(log_path: PathBuf, byte_offset: u64) -> io::Result<Self> {
        let log_reader = LogReader::open_at_offset(&log_path, byte_offset)?;
        Ok(Self { log_path, log_reader })
    }

    /// Discards the current state and loads the log file at the specified path, directly at the given offset. This is useful when a consumer needs to
    /// go back to a previous offset (which may not be located in the same file), for example if they failed consuming records they have read due to a
    /// temporary failure, and need to read them again. Doing this in place rather than creating a new instance makes things a little simpler in the
    /// [crate::persistence] module.
    pub fn reset_to_offset(&mut self, log_path: PathBuf, byte_offset: u64) -> io::Result<()> {
        // Even if the path is the same as the current one, we'll need to seek to the correct position. We could optimize by making the smallest possible
        // seek between reopening and seeking from the start and seeking from the current position, but the logic below is simpler and this operation should
        // not be called frequently, so no need to be fancy.
        let log_reader = LogReader::open_at_offset(&log_path, byte_offset)?;
        self.log_path = log_path;
        self.log_reader = log_reader;
        Ok(())
    }

    /// Remains within the current file but resets the cursor to the first byte. The use case is similar to that of [RotatingLogReader::reset_to_offset]
    /// but for cases where there is a single log file and no index file yet, so the only place we can reset at is the start of the only log file.
    pub fn reset_to_start(&mut self) -> io::Result<()> {
        self.log_reader.reset_to_start()
    }

    /// Seeks [LogReader]. Note that this method won't trigger any rotation,only reading data will do that.
    pub fn seek(&mut self, byte_offset: i64) -> io::Result<()> {
        self.log_reader.seek(byte_offset)
    }

    /// Reads the exact amount of bytes requested, or 0 bytes if we are currently at the end of the file and no new rotated file
    /// exists yet. The data may be from the current file or the next one, as this method can trigger a rotation. Caution, this only works
    /// within a single file, if the requested length exceeds the file size, this method will fail **except** if the cursor is exactly at EOF,
    /// in which case either one of two things will happen:
    /// - if the next file already exists, rotation will take place automatically and the bytes will be read from the next file. In this case,
    ///   the number of bytes in the next file must still be equal or larger than the requested number of bytes.
    /// - if the next file doesn't exist, this method will return an empty vector to signal that there is no new data to be read at the moment.
    ///
    /// In short, reads with this method need to perfectly align with EOF for the current file (either not intersect EOF, or end exactly at EOF,
    /// or start exactly at EOF).
    pub fn read(&mut self, len: usize) -> io::Result<Vec<u8>> {
        self.rotate_if_needed_and_read(|r| r.log_reader.read_exact(len))
            .map(|r| r.unwrap_or_else(Vec::new))
    }

    /// Attempts reading exactly 8 bytes from the current position and parses them as a [u64] value from its little endian representation. Returns [None]
    /// if the file is at EOF and there is no next file. If this happens, the reader will be in the same state as it was before the method was called, so
    /// no clean-up action is required from the caller. This method will trigger a rotation if reading exactly 8 bytes caused us to reach EOF and the next
    /// file is available.
    pub fn try_read_u64_le(&mut self) -> Result<Option<u64>, io::Error> {
        self.rotate_if_needed_and_read(|r| r.log_reader.try_read_u64_le())
            .map(|r| r.unwrap_or(None))
    }

    fn rotate_if_needed_and_read<T, F>(&mut self, mut do_read: F) -> io::Result<Option<T>>
    where F: FnMut(&mut Self) -> io::Result<T> {
        if self.log_reader.is_eof()? {
            return if let Some(next_file_path) = indexing::get_next_log_file_path(&self.log_path)? {
                self.log_reader = LogReader::open(&next_file_path)?;
                self.log_path = next_file_path;
                self.rotate_if_needed_and_read(do_read)
            } else {
                Ok(None)
            }
        }
        do_read(self).map(Some)
    }
}

impl Read for RotatingLogReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Ok(self.rotate_if_needed_and_read(|r| r.log_reader.reader.read(buf))?.unwrap_or(0))
    }
}