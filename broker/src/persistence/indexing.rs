use super::append_only_log::{AppendOnlyLog, LogFile};
use crate::persistence::{INDEX_EXTENSION, LOG_EXTENSION};
use mockall::automock;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::{fs, io};
use walkdir::WalkDir;

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
pub trait LogIndexWriter : Send {
    /// Notifies the index writer that a new record has been written with a given index, at a given byte offset. The [LogIndexWriter] will decide
    /// whether to write a new index entry or not.
    /// # Panicking
    /// The index must be greater than the last written index.
    fn ack_bytes_written(&mut self, index: u64, byte_offset: u64) -> io::Result<()>;

    /// Notifies the index writer that a rotation occurred for the log file, and provides the cutoff index and final byte offset before rotation
    /// to update its internal state. The cutoff index is the first index that will be written to the next log file. The cutoff index will be written
    /// at the end of the current index file before rotating it, which is useful for the [LogReader] to determine the next file's name by looking at the
    /// current file's index (last 16 bytes only). The writer doesn't need that because it receives the index as an input, but the reader doesn't get any hint.
    /// This function also exists for mocking, otherwise the [RotatingAppendOnlyLog] could have instantiated a new [LogIndexWriter]. With this method,
    /// we can reuse the same instance in-place and facilitate mocking it.
    /// # Panicking
    /// Note that the file's name must be composed of a [u64] index (first index in the file) and an optional extension. The function will panic if
    /// this condition is not met as the index cannot work properly without it. Additionally, the index must be greater than the last written index.
    fn ack_rotation(&mut self, cut_off_index: u64, last_byte_offset: u64) -> io::Result<()>;

    /// Flushes the content of the internal buffer to the disk. It is recommended to always flush after flushing the record writer's buffer,
    /// in order to properly "commit" the records written so far as long with their index, though not strictly required (logs would remain
    /// searchable, though slower, even if part of the index is missing as long as the last bytes aren't corrupted/partially written).
    fn flush(&mut self) -> io::Result<()>;
}

pub struct BinaryLogIndexWriter {
    root_path: PathBuf,
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
        let root_path = path.parent().unwrap_or(Path::new("")).to_owned();
        let (_, index_file_path) = log_path_to_index_path(path);
        let index_file = AppendOnlyLog::open(&index_file_path)?; // ensures the index file now exists
        let index = extract_last_index(&index_file_path)?;

        Ok(Self {
            root_path,
            index_file,
            last_written_index: index.unwrap_or(0),
            max_index_gap: MAX_INDEX_GAP,
        })
    }

    fn force_ack_bytes_written(&mut self, index: u64, byte_offset: u64) -> io::Result<()> {
        Ok({
            self.index_file.write_all(&index.to_le_bytes())?;
            self.index_file.write_all(&byte_offset.to_le_bytes())?;
            self.last_written_index = index;
        })
    }

    fn validate_index(&mut self, index: u64) -> u64 {
        if index > 0 && index <= self.last_written_index {
            panic!("Indices must monotonically increase, but tried to write {index} while the last written index is {}", self.last_written_index);
        }
        index
    }
}

impl LogIndexWriter for BinaryLogIndexWriter {
    fn ack_bytes_written(&mut self, index: u64, byte_offset: u64) -> io::Result<()> {
        self.validate_index(index);

        if index - self.last_written_index >= self.max_index_gap {
            self.force_ack_bytes_written(index, byte_offset)?;
        }
        Ok(())
    }

    fn ack_rotation(&mut self, cut_off_index: u64, last_byte_offset: u64) -> io::Result<()> {
        self.validate_index(cut_off_index);

        if cut_off_index != self.last_written_index {
            // Helpful for the LogReader, see the doc on the trait. Note that the cutoff index is actually outside of this file, but we
            // still write it at the end. This is not an issue because the lookup function will select the next file when searching for
            // this index, so this footer will never be an issue as no value greater than cut_off_index - 1 will ever be searched in the
            // current index file
            self.force_ack_bytes_written(cut_off_index, last_byte_offset)?;
        }
        self.flush()?;

        let new_index_file_path = self.root_path.clone().join(super::get_index_name(cut_off_index));
        let new_index_file = AppendOnlyLog::open(&new_index_file_path)?; // ensures the index file now exists

        // we expect indices to be monotonically increasing, and we still want to write the next index with a large enough gap from the start
        // of this file (it's useless to write a row for the first record of the file)
        self.last_written_index = cut_off_index - 1; // in the current file indices end just before the cutoff
        self.index_file = new_index_file;

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

#[derive(Eq, PartialEq, Debug)]
pub struct IndexLookupResult {
    log_file_path: PathBuf,
    byte_offset: u64,
}

/// Finds the log file, if any, which contains a given index, and provides a byte offset within this file which is close to the actual byte offset
/// of the corresponding record. The caller will need to make some disk seeks from this position to find the actual offset. This function is not
/// very demanding (assuming a small number of log files), but it's also not extremely efficient. It will be slow if executed frequently. It is expected
/// to be called mostly for initializing a log reader, and following that the reader is the one that has to track the offset. Note that [None] will be
/// returned if no data has been written yet.
pub fn look_up(root_path: &Path, index: u64) -> io::Result<Option<IndexLookupResult>> {
    let mut index_files = Vec::new();

    WalkDir::new(&root_path)
        .sort_by_file_name()
        .into_iter()
        // not super elegant compared to using iterator methods, but it makes handling the many layers of Result nicely
        .try_for_each(|entry| {
            let entry = entry?;
            let is_file = entry.file_type().is_file();
            let x = entry.path();
            let extension = x.extension().map(|os_str| os_str.to_str().unwrap()).unwrap_or("");
            if is_file && extension == INDEX_EXTENSION {
                let (start_index, _) = log_path_to_index_path(x);
                index_files.push((start_index, x.to_owned()));
            }
            io::Result::Ok(())
        })?;

    if index_files.is_empty() { return Ok(None) }

    let index_file_path = match index_files.binary_search_by_key(&index, |item| item.0) {
        Ok(i) => &index_files[i].1,
        Err(0) => &index_files[0].1,
        Err(i) => { &index_files[i - 1].1 },
    };

    find_closest_byte_offset(index_file_path, index)
}

// A more efficient implementation could leverage a memory-mapped file but to simplify, I'll just load it in memory. With the assumptions
// we made on file size and index sparsity,this is not going to be a large amount of data and we load a single index file at a time.
fn find_closest_byte_offset(index_file_path: &Path, index: u64) -> io::Result<Option<IndexLookupResult>> {
    let index_rows = parse_index_file(index_file_path)?;
    let log_file_path = index_path_to_log_path(index_file_path).1;

    let byte_offset = match index_rows.binary_search_by_key(&index, |item| item.0) {
        Ok(i) => index_rows[i].1,
        Err(0) => 0,
        Err(i) => index_rows[i - 1].1,
    };

    Ok(Some(IndexLookupResult { log_file_path, byte_offset }))
}

fn parse_index_file(log_file_path: &Path) -> io::Result<Vec<(u64, u64)>> {
    let mut rows = Vec::new();
    let mut reader = BufReader::new(File::open(log_file_path)?);
    let mut buf = [0u8; 8];

    while reader.fill_buf()?.len() > 0 {
        reader.read_exact(&mut buf)?;
        let index = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let byte_offset = u64::from_le_bytes(buf);

        rows.push((index, byte_offset));
    }

    Ok(rows)
}

/// This function determines what the path of the next log file path should be if the provided log file is at eof. If the generated next file
/// path doesn't exist, it means rotation has not happened yet and the reader should continue polling for new data on the current log file.
/// In this case, [None] is returned. If the next log file exists, its path is returned.
pub fn get_next_log_file_path(log_file_path: &Path) -> io::Result<Option<PathBuf>> {
    let root_path = log_file_path.parent().unwrap_or(Path::new("")).to_owned();
    Ok(match extract_last_index(&log_path_to_index_path(&log_file_path).1)? {
        None => None,
        Some(last_index) => {
            let next_log_file_path = root_path.join(super::get_log_name(last_index));
            if next_log_file_path.exists() { Some(next_log_file_path) } else { None }
        }
    })
}

fn extract_last_index(index_file_path: &Path) -> io::Result<Option<u64>> {
    if !fs::exists(&index_file_path)? { return Ok(None) }

    let file_len = fs::metadata(index_file_path)?.len();
    if file_len == 0 { return Ok(None); }

    let mut reader = BufReader::new(File::open(index_file_path)?);

    // -16 to account for two u64 values
    reader.seek(io::SeekFrom::Start(file_len - 16))?;
    let mut buf = [0_u8; 8];
    reader.read_exact(&mut buf)?;

    Ok(Some(u64::from_le_bytes(buf)))
}

fn log_path_to_index_path(log_path: &Path) -> (u64, PathBuf) {
    validate_path_and_change_extension(log_path, INDEX_EXTENSION)
}

fn index_path_to_log_path(log_path: &Path) -> (u64, PathBuf) {
    validate_path_and_change_extension(log_path, LOG_EXTENSION)
}

fn validate_path_and_change_extension(log_path: &Path, new_extension: &str) -> (u64, PathBuf) {
    let extension_length = log_path.extension().map(OsStr::len).unwrap_or(0);
    let log_file_name = log_path.file_name().unwrap().to_str().unwrap(); // we want to panic if the file's name is not expected

    let index = log_file_name[0..(log_file_name.len() - extension_length - 1)]
        .to_owned()
        .parse::<u64>()
        .expect(&format!("Log file names should be composed solely of a u64 index (first index in the log) and an \
                optional extension but was: {}", &log_file_name));

    (index, log_path.with_extension(new_extension))
}