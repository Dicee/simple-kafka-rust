use super::append_only_log::{AppendOnlyLog, RotatingAppendOnlyLog, LogFile};
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::{fs, io};
use std::ffi::OsStr;
use std::fmt::format;
use mockall::automock;

#[cfg(test)]
#[path = "./indexing_test.rs"]
mod indexing_test;

///! This module takes care of the indexing logic (writing the index as well as using it for look-ups) of our log files, to allow fast search
///! by record offset. We do not support time-based indexing at the moment. Note that in this module, ze zill never mention the term "offset",
///! and instead talk about an "index". This is because this module will be used by [super::append_only_log] and [super::log_reader], which are
///! not expected to understand business logic, and are purely persistence layer abstractions that can be reused in different contexts. For example
///! if the implementation changes for some key business logic, we should still be able to reuse these building blocks largely unchanged. The
///! other reason to decouple this module from the notion of offset is that we could use this same code for timestamps in the future, so having
///! a single terminology for all indexing use cases is helpful. Note that it is a requirement that an index is unique for a record, and monotonically
/// increasing. Failing that, indexing will have undefined behaviour.

/// In real life this could be a configurable setting, or calculated from other settings. In our case, we made the following assumptions:
/// - the max file size is 10MB (see [super::MAX_FILE_SIZE_BYTES])
/// - a single record is about 1kB (so there are about 10,000 records in one file)
/// - a record batch contains about 50 records (so there are about 200 batches in one file).
/// We want to have roughly logarithmic (in the number of records) access to a record, so roughly 10 disk seeks to find the base offset of the batch.
/// To achieve that, we need to index every 20 batches, namely every 1000 records.
const MAX_INDEX_GAP: u64 = 1000;

#[automock]
pub trait LogIndexWriter {
    /// Notifies the index writer that a new record has been written with a given index, at a given byte offset. The [LogIndexWriter] will decide
    /// whether to write a new index entry or not.
    /// # Panicking
    /// The index must be greater than the last written index.
    fn ack_bytes_written(&mut self, index: u64, byte_offset: u64) -> io::Result<()>;

    /// Notifies the index writer that a rotation occurred for the log file, and provides the new path for the writer to update its internal state.
    /// This function mainly exists for mocking, otherwise the [RotatingAppendOnlyLog] could have instantiated a new [LogIndexWriter]. With this method,
    /// we can reuse the same instance in-place and facilitate mocking it.
    /// # Panicking
    /// Note that the file's name must be composed of a [u64] index (first index in the file) and an optional extension. The function will panic if
    /// this condition is not met as the index cannot work properly without it. Additionally, the index must be greater than the last written index.
    fn ack_rotation(&mut self, new_path: &Path) -> io::Result<()>;

    /// Flushes the content of the internal buffer to the disk. It is recommended to always flush after flushing the record writer's buffer,
    /// in order to properly "commit" the records written so far as long with their index, though not strictly required (logs would remain
    /// searchable, though slower, even if part of the index is missing as long as the last bytes aren't corrupted/partially written).
    fn flush(&mut self) -> io::Result<()>;
}

struct BinaryLogIndexWriter {
    index_file: AppendOnlyLog,
    last_written_index: u64,
    max_index_gap: u64,
}

impl BinaryLogIndexWriter {
    /// Takes the path of a log file and opens (or create if needed) the corresponding index file. If an index file already exists, the state of the
    /// [BinaryLogIndexWriter] will be properly initialized by making a quick read at the end of the file to find the latest index.
    /// # Panicking
    /// Note that the file's name must be composed of a [u64] index (first index in the file) and an optional extension. The function will panic if
    /// this condition is not met as the index cannot work properly without it.
    pub fn open_for_log_file(path: &Path) -> io::Result<Self> {
        let (_, index_file_path) = Self::log_path_to_index_path(path);
        let index_file = AppendOnlyLog::open(&index_file_path)?; // ensures the index file now exists
        let index = Self::extract_last_index(&index_file_path)?;

        Ok(Self {
            index_file,
            last_written_index: index,
            max_index_gap: MAX_INDEX_GAP,
        })
    }

    fn extract_last_index(index_file_path: &Path) -> io::Result<u64> {
        let file_len = fs::metadata(index_file_path)?.len();
        if file_len == 0 { return Ok(0); }

        let mut reader = BufReader::new(File::open(index_file_path)?);

        // -16 to account for two u64 values
        reader.seek(io::SeekFrom::Start(file_len - 16))?;
        Ok(read_u64(&mut reader)?)
    }

    fn log_path_to_index_path(log_path: &Path) -> (u64, PathBuf) {
        let extension_length = log_path.extension().map(OsStr::len).unwrap_or(0);
        let log_file_name = log_path.file_name().unwrap().to_str().unwrap(); // we want to panic if the file's name is not expected

        let index = log_file_name[0..(log_file_name.len() - extension_length - 1)]
            .to_owned()
            .parse::<u64>()
            .expect(&format!("Log file names should be composed solely of a u64 index (first index in the log) and an \
                optional extension but was: {}", &log_file_name));

        (index, log_path.with_extension("index"))
    }

    fn validate_index(&mut self, index: u64) {
        if index <= self.last_written_index {
            panic!("Indices must monotonically increase, but tried to write {index} while the last written index is {}", self.last_written_index);
        }
    }
}

impl LogIndexWriter for BinaryLogIndexWriter {
    fn ack_bytes_written(&mut self, index: u64, byte_offset: u64) -> io::Result<()> {
        self.validate_index(index);

        if index - self.last_written_index >= self.max_index_gap {
            self.index_file.write_all(&index.to_le_bytes())?;
            self.index_file.write_all(&byte_offset.to_le_bytes())?;
            self.last_written_index = index;
        }
        Ok(())
    }

    fn ack_rotation(&mut self, new_path: &Path) -> io::Result<()> {
        self.flush()?;

        let (first_index, new_index_file_path) = Self::log_path_to_index_path(new_path);
        let new_index_file = AppendOnlyLog::open(&new_index_file_path)?; // ensures the index file now exists

        self.index_file = new_index_file;

        self.validate_index(first_index);
        // we expect indices to be monotonically increasing, and we still want to write the next index with a large enough gap from the start
        // of this file (it's useless to write a row for the first record of the file)
        self.last_written_index = first_index;

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.index_file.flush()
    }
}

impl Drop for BinaryLogIndexWriter {
    fn drop(&mut self) {
        let _ = self.flush(); // ignore errors during drop
    }
}

fn read_u64(reader: &mut BufReader<File>) -> io::Result<u64> {
    let mut buf = [0_u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
