use anyhow::Context;
use bpaf::{Bpaf, Parser};
use bytesize::ByteSize;
use itertools::Itertools;
use parquet::{
    basic::{Compression, Encoding},
    file::reader::FileReader,
};
use std::{collections::BTreeMap, fs::File, io::Write, path::PathBuf};

#[derive(Debug, Clone, Bpaf)]
pub struct Opts {
    /// Show size after encoding but before heavyweight compression
    #[bpaf(short, long)]
    uncompressed: bool,
    /// The parquet file to analyze
    #[bpaf(positional("PATH"))]
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opts = opts().to_options().run();
    let file = File::open(&opts.path).context(opts.path.display().to_string())?;
    let file = parquet::file::reader::SerializedFileReader::new(file)?;
    let meta = file.metadata();

    let mut total = 0;
    let mut cols = BTreeMap::<String, (Vec<Encoding>, Compression, i64)>::new();
    for meta in meta.row_groups() {
        total += if opts.uncompressed {
            meta.total_byte_size()
        } else {
            meta.compressed_size()
        };
        for col in meta.columns() {
            let x = cols
                .entry(col.column_path().string())
                .or_insert_with(|| (col.encodings().clone(), col.compression(), 0));
            x.2 += if opts.uncompressed {
                col.uncompressed_size()
            } else {
                col.compressed_size()
            };
        }
    }
    let mut wtr = tabwriter::TabWriter::new(std::io::stdout());
    let mut cols: Vec<_> = cols.into_iter().collect();
    cols.sort_by_key(|(_, (_, _, x))| -x);
    for (name, (encs, comp, size)) in cols {
        write!(
            &mut wtr,
            "{name}\t{:>9}\t{:>2.0}%\t(",
            ByteSize(size as u64).to_string_as(true),
            100.0 * size as f64 / total as f64,
        )?;
        if opts.uncompressed {
            write!(&mut wtr, "{})", encs.iter().format(", "))?;
        } else {
            for enc in encs {
                write!(&mut wtr, "{}, ", enc)?;
            }
            write!(&mut wtr, "{})", print_comp(comp))?;
        }
        writeln!(&mut wtr)?;
    }
    wtr.flush()?;
    Ok(())
}

fn print_comp(comp: Compression) -> &'static str {
    match comp {
        Compression::UNCOMPRESSED => "UNCOMPRESSED",
        Compression::SNAPPY => "SNAPPY",
        Compression::GZIP(_) => "GZIP",
        Compression::LZO => "LZO",
        Compression::BROTLI(_) => "BROTLI",
        Compression::LZ4 => "LZ4",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZ4_RAW => "LZ4_RAW",
    }
}
