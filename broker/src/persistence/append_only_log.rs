use std::fs;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io;
use std::io::{BufWriter, Write};
use walkdir::{DirEntry, Error, WalkDir};

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

/// This [LogFile] implementation abstracts the rotation of [AppendOnlyLog] files based on a maximum byte size.
/// For simplicity, we will store all the files for a single rotating log in the same directory, neglecting the impact
/// it may have on listing speed. In our end-to-end tests, we'll never reach a large scale where this will start becoming
/// significant. Otherwise, we'd need to design a more efficient layout.
/// Additionally, and for the same motivation of keeping the solution simple and suitable for modest scale, we will support
/// a single file naming scheme, allowing at most 10^5 files per [RotatingAppendOnlyLog].
pub struct RotatingAppendOnlyLog {
    root_path: String,
    base_file_name: &'static str,
    log_index: u32,
    max_byte_size: u64,
    current_byte_size: u64,
    log: AppendOnlyLog,
}

// Note that we won't implement drop manually because the default implementation should be correct, since AppendOnlyLog itself implements Drop
// to make sure any buffered data is flushed to disk before closing resources
impl RotatingAppendOnlyLog {
    /// Detects and open the latest rotated file for a given base file name and root path, or initializes a new one with index 0.
    /// Files are expected directly under the root path, any sub-directory and its contents will be silently ignored. Files which name
    /// does no starting with the specified base file name will also be skipped.
    ///
    /// ## Errors
    /// This function will panic if a file starting with the base file name has an invalid format. The parsing is quite relaxed, we only require
    /// that it contains at least one '.' character and that whatever comes after it is a valid u64. We could be more stringent on parsing since
    /// we know the format is exactly `format!("{base_file_name}.{index:05}")`, but this will do for our little project.
    pub fn open_latest(root_path: String, base_file_name: &'static str, max_byte_size: u64) -> io::Result<Self> {
        if fs::exists(&root_path)? {
            let mut files = WalkDir::new(&root_path)
                .sort_by(|a, b| a.file_name().cmp(b.file_name()).reverse())
                .into_iter();

            while let Some(ref file) = files.next() {
                let f = to_io_res(file)?;
                let file_name = f.file_name().to_str().unwrap();

                let is_file= to_io_res(&f.metadata())?.is_file();
                let matches_prefix = file_name.starts_with(base_file_name);

                if is_file && matches_prefix {
                    return Self::open(root_path, base_file_name, Self::parse_index(file_name), max_byte_size);
                }
            }
        }

        Self::open(root_path, base_file_name, 0, max_byte_size)
    }

    fn parse_index(file_name: &str) -> u32 {
        let dot_index = file_name.rfind('.').expect(&format!("Invalid file name {file_name}, it should have at least one . character"));
        file_name[dot_index + 1..].parse::<u32>().expect(&format!("Invalid file name {file_name}, failed to parse the index number"))
    }

    fn open(root_path: String, base_file_name: &'static str, log_index: u32, max_byte_size: u64) -> io::Result<Self> {
        let full_path = Self::get_log_name(&root_path, base_file_name, log_index);
        let mut len = 0u64;

        if fs::exists(&full_path)? {
            len = fs::metadata(&full_path)?.len();
        }

        Ok(RotatingAppendOnlyLog {
            root_path,
            base_file_name,
            log_index,
            max_byte_size,
            current_byte_size: len,
            log: AppendOnlyLog::open(&full_path)?
        })
    }

    fn get_next_log_name(&self) -> String {
        Self::get_log_name(&self.root_path, &self.base_file_name, self.log_index + 1)
    }

    fn get_log_name(root_path: &String, base_file_name: &str, log_index: u32) -> String {
        format!("{root_path}/{base_file_name}.{log_index:05}")
    }
}

impl LogFile for RotatingAppendOnlyLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // note that while converting usize to u64 is always safe, the reverse is not true
        if buf.len() as u64 + self.current_byte_size >= self.max_byte_size {
            self.flush()?;
            let full_path = self.get_next_log_name();
            assert!(!fs::exists(&full_path)?, "Next rotated file should not already exist, but did. Path: {full_path}");

            self.log = AppendOnlyLog::open(&full_path)?;
            self.current_byte_size = 0;
            self.log_index += 1;
        }

        // order matters as we only update the size if the write was successful to prevent an inconsistent state
        let result = self.log.write(buf)?;
        self.current_byte_size += buf.len() as u64;
        Ok(result)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.log.flush()
    }
}

fn to_io_res<T>(entry: &walkdir::Result<T>) -> io::Result<&T> {
    match entry {
        Ok(f) => Ok(f),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{e:?}"))),
    }
}