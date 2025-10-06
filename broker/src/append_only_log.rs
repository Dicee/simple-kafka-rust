use std::fs;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io;
use std::io::{BufWriter, Write};

#[cfg(test)]
#[path="append_only_log_test.rs"]
mod append_only_log_test;

pub trait LogFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize>;

    fn flush(&mut self) -> io::Result<()>;
}

/// Simplistic implementation of an append-only log. It must be written to by a single thread at a time.
pub struct AppendOnlyLog {
    writer: BufWriter<File>,
}

impl AppendOnlyLog {
    /// Will create the file if it doesn't exist (creating missing directories in the path if needed), or open it
    /// in append mode otherwise.
    pub fn open(path: &str) -> io::Result<Self> {
        let path = Path::new(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(AppendOnlyLog { writer: BufWriter::new(file) })
    }
}

impl LogFile for AppendOnlyLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl Drop for AppendOnlyLog {
    fn drop(&mut self) {
        let _ = self.flush(); // ignore errors during drop
    }
}