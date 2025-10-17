use crate::persistence::append_only_log::RotatingAppendOnlyLog;
use std::fs;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};

#[cfg(test)]
#[path = "log_reader_test.rs"]
mod log_reader_test;

/// Simple wrapper around a [BufReader] backed by a file to perform the basic operations we need on a log file
/// (seeking an offset from the current position, reading a given amount of bytes from the current position etc).
struct LogReader {
    reader: BufReader<File>,
}

impl LogReader {
    fn open(path: &str) -> io::Result<Self> {
        Ok(Self { reader: BufReader::new(File::open(path)?) })
    }

    /// Seeks the given amount of offset bytes (can be negative) from the current position.
    fn seek(&mut self, byte_offset: i64) -> io::Result<()> {
        // more efficient than seek for this use case, as it reuses the existing buffer when it can
        self.reader.seek_relative(byte_offset)
    }

    /// Reads exactly the specified amount of bytes from the current position and returns them as a vector.
    /// # Errors
    /// This will fail if EOF is reached befoe consuming the requested amount of bytes. Of course, any other IO failure will
    /// also cause this function to return an error.
    fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?; // will fail if EOF is reached before we
        Ok(buf)
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
    root_path: String,
    base_file_name: &'static str,
    next_file_name: String,
    log_index: u32,
    log_reader: LogReader,
}

impl RotatingLogReader {
    fn open(root_path: String, base_file_name: &'static str, log_index: u32) -> io::Result<Self> {
        let file_name = RotatingAppendOnlyLog::get_log_name(&root_path, base_file_name, log_index);
        let next_file_name = RotatingAppendOnlyLog::get_log_name(&root_path, base_file_name, log_index + 1);
        Ok(Self {
            root_path,
            base_file_name,
            next_file_name,
            log_index,
            log_reader: LogReader::open(&file_name)?,
        })
    }

    /// See [LogReader]. Note that this method won't trigger any rotation,only reading data will do that.
    fn seek(&mut self, byte_offset: i64) -> io::Result<()> {
        self.log_reader.seek(byte_offset)
    }

    /// Reads the exact amount of bytes requested, or 0 bytes if we are currently at the end of the file and no new rotated file
    /// exists yet. The data may be from the current file or the next one, as this method can trigger a rotation.
    fn read(&mut self, len: usize) -> io::Result<Vec<u8>> {
        if self.log_reader.is_eof()? {
            return if fs::exists(&self.next_file_name)? {
                let next_file_name = self.get_next_file_name();
                self.log_reader = LogReader::open(&next_file_name)?;
                self.log_index += 1;
                self.next_file_name = next_file_name;
                self.read(len)
            } else {
                Ok(vec![])
            }
        }
        self.log_reader.read_exact(len)
    }

    fn get_next_file_name(&self) -> String {
        RotatingAppendOnlyLog::get_log_name(&self.root_path, self.base_file_name, self.log_index + 1)
    }
}