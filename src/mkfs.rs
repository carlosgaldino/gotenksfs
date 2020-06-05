use crate::gotenks::{types::Superblock, util, SUPERBLOCK_SIZE};
use anyhow::anyhow;
use byte_unit::{Byte, ByteUnit};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

pub fn make<P>(path: P, file_size: u32, blk_size: u32) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let bg_size = util::block_group_size(blk_size);
    if file_size < bg_size - 2 * blk_size {
        return Err(anyhow!(format!(
            "File size must be at least {} for block size of {}",
            Byte::from_bytes(bg_size as _).get_appropriate_unit(true),
            Byte::from_bytes(blk_size as _).get_adjusted_unit(ByteUnit::B)
        )));
    }

    let groups = file_size / bg_size + 1;
    let file = create_file(path.as_ref())?;
    let mut buf = BufWriter::new(file);
    let mut sb = Superblock::new(blk_size, groups as _);

    sb.checksum();

    buf.write_all(&bincode::serialize(&sb)?)?;
    buf.write_all(&vec![
        0u8;
        (SUPERBLOCK_SIZE - bincode::serialized_size(&sb)?)
            as usize
    ])?;

    buf.flush()?;

    let file = OpenOptions::new().write(true).open(path)?;
    Ok(file.set_len(1024 + bg_size as u64 * groups as u64)?)
}

fn create_file<P: AsRef<Path>>(name: P) -> anyhow::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(name)?;

    Ok(file)
}
