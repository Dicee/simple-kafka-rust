use std::fs::File;
use std::io;
use std::io::{stdout, BufReader, ErrorKind, Read, Write};
use argh::FromArgs;
use broker::model::RecordBatchWithOffset;
use protocol::record::{read_next_batch};

#[derive(FromArgs)]
/// Utility tool to decode the binary data in a log file and pipe it as JSON to stdout
struct Args {
    /// path to the file to decode
    #[argh(option, short = 'r')]
    path: String,

    /// maximum number of records to read, starting from the currently acknowledged offset position
    #[argh(option, default = "20")]
    limit: usize,
}

fn main() -> io::Result<()> {
    let args: Args = argh::from_env();
    let mut reader = BufReader::new(File::open(args.path)?);

    for _ in 1..=args.limit {
        let mut offset_bytes = [0u8; 8];
        match reader.read(&mut offset_bytes)? {
            0 => break,
            0..8 => return Err(io::Error::new(ErrorKind::UnexpectedEof, "Could not read next offset")),
            _ => (),
        }

        let base_offset = u64::from_le_bytes(offset_bytes);
        let batch = read_next_batch(&mut reader)?;
        let json_batch = serde_json::to_string(&RecordBatchWithOffset { base_offset, batch })
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        stdout().write_all(json_batch.as_bytes())?;
        stdout().write(b"\n")?;
    }

    stdout().flush()
}