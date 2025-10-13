use std::fmt::Formatter;
use rusqlite;
use std::path::Path;
use std::result;
use rusqlite::Error::SqliteFailure;
use rusqlite::ErrorCode::ConstraintViolation;
use r2d2_sqlite::SqliteConnectionManager;
use r2d2;
use r2d2::{Pool, PooledConnection};
use crate::dao::Error::*;

#[cfg(test)]
#[path = "./dao_test.rs"]
mod dao_test;

const SQL_CREATE_TOPICS: &str =
    "CREATE TABLE IF NOT EXISTS topics (
        name            STRING PRIMARY KEY,
        partition_count INTEGER NOT NULL
    )";

const SQL_CREATE_WRITE_OFFSETS: &str =
    "CREATE TABLE IF NOT EXISTS write_offsets (
        topic     STRING,
        partition INTEGER,
        offset    INTEGER NOT NULL,
        PRIMARY KEY(topic, partition)
    )";

const SQL_CREATE_READ_OFFSETS: &str =
    "CREATE TABLE IF NOT EXISTS read_offsets (
        topic          STRING,
        partition      INTEGER,
        consumer_group STRING,
        offset         INTEGER NOT NULL,
        PRIMARY KEY(topic, partition, consumer_group)
    )";

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct Topic {
    pub name: String,
    pub partition_count: u32,
}

pub struct Dao {
    connection_pool: Pool<SqliteConnectionManager>,
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(PartialEq, Debug)]
pub enum Error {
    TopicAlreadyExists(String),
    TopicNotFound(String),
    OutOfRangePartition { topic: Topic, invalid_partition: u32 },
    InvalidReadOffset(InvalidOffset),
    Internal(String),
}

#[derive(PartialEq, Debug)]
pub enum InvalidOffset {
    TooLarge { max_offset: u64, invalid_offset: u64 },
    NoDataWritten,
}

impl Dao {
    pub fn new(db_path: &Path) -> Result<Self> {
        let connection_manager = SqliteConnectionManager::file(db_path);
        let connection_pool = Pool::new(connection_manager)?;

        let connection = connection_pool.get()?;
        connection.execute(SQL_CREATE_TOPICS, ())?;
        connection.execute(SQL_CREATE_WRITE_OFFSETS, ())?;
        connection.execute(SQL_CREATE_READ_OFFSETS, ())?;

        Ok(Self { connection_pool })
    }

    /// Creates a new topic with a given name and partition count.
    /// # Errors
    ///
    /// - Returns [`TopicAlreadyExists`] if the topic already exists.
    /// - Returns [`Internal`] for any other database error.
    pub fn create_topic(&self, topic: &str, partition_count: u32) -> Result<()> {
        let result = self.get_connection()?.execute("INSERT INTO topics (name, partition_count) VALUES (:topic, :partition_count)", &[
            (":topic", topic),
            (":partition_count", &partition_count.to_string()),
        ]);

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(match e {
                SqliteFailure(cause, _) if cause.code == ConstraintViolation => {
                    TopicAlreadyExists(topic.to_string())
                }
                _ => Internal(e.to_string()),
            })
        }
    }

    /// Returns the metadata for a topic of the given name.
    /// # Errors
    ///
    /// - Returns [`TopicNotFound`] if the topic does not exist.
    /// - Returns [`Internal`] for any other database error.
    pub fn get_topic(&self, name: &str) -> Result<Topic> {
        self.get_connection()?.query_one(
            "SELECT * FROM topics WHERE name = :topic",
            &[(":topic", name)],
            |row| {
                Ok(Topic {
                    name: row.get(0)?,
                    partition_count: row.get(1)?,
                })
            }
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => TopicNotFound(name.to_string()),
            _ => Internal(e.to_string()),
        })
    }

    /// Increments a write offset by a given [u32] value for a given topic and partition.
    /// # Errors
    ///
    /// - Returns [`TopicNotFound`] if the topic does not exist.
    /// - Returns [`OutOfRangePartition`] if the requested partition is out of range.
    /// - Returns [`Internal`] for any other database error.
    pub fn inc_write_offset_by(&self, topic_name: &str, partition: u32, inc: u32) -> Result<()> {
        self.validate_topic_and_partition(topic_name, partition)?;

        let query =
            "INSERT INTO write_offsets (topic, partition, offset)
            VALUES (:topic, :partition, :inc)
            ON CONFLICT (topic, partition)
            DO UPDATE SET offset = offset + :inc;";

        self.get_connection()?.execute(query, &[
            (":topic", topic_name),
            (":partition", &partition.to_string()),
            (":inc", &inc.to_string()),
        ])?;

        Ok(())
    }

    /// Returns the current write offset for a given topic and partition.
    /// # Errors
    ///
    /// - Returns [`TopicNotFound`] if the topic does not exist.
    /// - Returns [`OutOfRangePartition`] if the requested partition is out of range.
    /// - Returns [`Internal`] for any other database error.
    pub fn get_write_offset(&self, topic_name: &str, partition: u32) -> Result<Option<u64>> {
        self.validate_topic_and_partition(topic_name, partition)?;

        Self::to_opt_offset(self.get_connection()?.query_one(
            "SELECT offset from write_offsets WHERE topic = :topic AND partition = :partition",
            &[(":topic", topic_name), (":partition", &partition.to_string())],
            |row| row.get::<_, u64>(0)
        ))
    }

    /// Commits a read offset to a given [u32] value for a given topic, partition and consumer group.
    /// # Errors
    ///
    /// - Returns [`TopicNotFound`] if the topic does not exist.
    /// - Returns [`OutOfRangePartition`] if the requested partition is out of range.
    /// - Returns [`InvalidReadOffset`] if the requested offset exceeds the write offset for this partition, or no data has been written yet
    /// - Returns [`Internal`] for any other database error.
    pub fn ack_read_offset(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64) -> Result<()> {
        self.validate_topic_and_partition(topic, partition)?;

        // The write offset can only increase, so it's fine to error if at the time of the ack call, the acknowledged offset
        // was too large. Indeed, the monotonic nature of the write offset makes it impossible that it's small enough, and
        // then becomes too large by the time we write the read offset.
        let write_offset = self.get_write_offset(topic, partition)?;
        if let Some(max_offset) = write_offset && offset > max_offset {
            return Err(InvalidReadOffset(InvalidOffset::TooLarge { max_offset, invalid_offset: offset }));
        } else if write_offset.is_none() {
            return Err(InvalidReadOffset(InvalidOffset::NoDataWritten));
        }

        let query=
            "INSERT INTO read_offsets (topic, partition, consumer_group, offset)
            VALUES (:topic, :partition, :consumer_group, :offset)
            ON CONFLICT (topic, partition, consumer_group)
            DO UPDATE SET offset = :offset";

        self.get_connection()?.execute(query, &[
            (":topic", topic),
            (":partition", &partition.to_string()),
            (":consumer_group", consumer_group),
            (":offset", &offset.to_string()),
        ])?;

        Ok(())
    }

    /// Returns the current read offset for a given topic, partition and consumer group.
    /// # Errors
    ///
    /// - Returns [`TopicNotFound`] if the topic does not exist.
    /// - Returns [`OutOfRangePartition`] if the requested partition is out of range.
    /// - Returns [`Internal`] for any other database error.
    pub fn get_read_offset(&self, topic_name: &str, partition: u32, consumer_group: &str) -> Result<Option<u64>> {
        self.validate_topic_and_partition(topic_name, partition)?;

        Self::to_opt_offset(self.get_connection()?.query_one(
            "SELECT offset from read_offsets WHERE topic = :topic AND partition = :partition AND consumer_group = :consumer_group",
            &[
                (":topic", topic_name),
                (":partition", &partition.to_string()),
                (":consumer_group", consumer_group),
            ],
            |row| row.get::<_, u64>(0)
        ))
    }

    fn get_connection(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        self.connection_pool.get().map_err(Error::from)
    }

    fn validate_topic_and_partition(&self, topic_name: &str, partition: u32) -> Result<()> {
        let topic = self.get_topic(topic_name)?;
        if partition >= topic.partition_count {
            Err(OutOfRangePartition { topic, invalid_partition: partition })
        } else {
            Ok(())
        }
    }

    //noinspection RsSelfConvention
    fn to_opt_offset(result: rusqlite::Result<u64>) -> Result<Option<u64>> {
        match result {
            Ok(offset) => Ok(Some(offset)),
            Err(e) => match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                _ => Err(Internal(e.to_string())),
            }
        }
    }
}

// default conversion but in some cases we should handle it manually to have a more precise return type
impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Error {
        Internal(err.to_string())
    }
}

// default conversion but in some cases we should handle it manually to have a more precise return type
impl From<r2d2::Error> for Error {
    fn from(err: r2d2::Error) -> Error {
        Internal(err.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            TopicAlreadyExists(topic) => &format!("Topic already exists: {topic}"),
            TopicNotFound(topic) => &format!("Topic not found: {topic}"),
            InvalidReadOffset(InvalidOffset::NoDataWritten) => "Invalid read offset: no data written has been written yet",
            Internal(message) => &format!("Internal error: {message}"),
            OutOfRangePartition { topic, invalid_partition} =>
                &format!("Invalid partition {invalid_partition} for topic {}, cannot exceed {}", topic.name, topic.partition_count),
            InvalidReadOffset(InvalidOffset::TooLarge { max_offset, invalid_offset }) =>
                &format!("Invalid read offset {invalid_offset}: it exceeds the maximum written offset of {max_offset}."),
        };
        formatter.write_str(message)
    }
}