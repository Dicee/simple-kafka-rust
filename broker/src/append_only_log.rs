use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Write};

#[cfg(test)]
#[path="append_only_log_test.rs"]
mod append_only_log_test;

/// Simplistic implementation of an append-only log. It must be written to by a single thread at a time.
pub struct AppendOnlyLog {
    writer: BufWriter<File>,
}

impl AppendOnlyLog {
    /// Will create the file if it doesn't exist (without creating directories in the path), or open it
    /// in append mode otherwise.
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(AppendOnlyLog { writer: BufWriter::new(file) })
    }

    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Flushes the writer and causes it to be dropped along with the [AppendOnlyLog] since this method takes ownership of self.
    /// Calling this method will thus close all resources properly and will make the log instance unusable.
    pub fn close(mut self) -> io::Result<()> {
        self.flush()
    }
}

impl Drop for AppendOnlyLog {
    fn drop(&mut self) {
        let _ = self.flush(); // ignore errors during drop
    }
}
