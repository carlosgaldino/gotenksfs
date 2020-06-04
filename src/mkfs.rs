use crate::fs;
use std::{
    fs as std_fs,
    io::{BufWriter, Write},
    path::Path,
};

pub(crate) fn make<P>(path: P, file_size: u64, blk_size: u32) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let bg_size = fs::block_group_size(blk_size);
    if file_size < bg_size - 2 * blk_size as u64 {
        return Err(anyhow!(format!(
            "File size must be at least {} for block size of {}",
            byte_unit::Byte::from_bytes(bg_size).get_appropriate_unit(true),
            byte_unit::Byte::from_bytes(blk_size as _).get_adjusted_unit(byte_unit::ByteUnit::B)
        )));
    }

    let groups = file_size / bg_size + 1;
    let file = create_file(path.as_ref())?;
    let mut buf = BufWriter::new(file);
    let mut sb = fs::Superblock::new(blk_size, groups as _);

    sb.checksum();

    buf.write_all(&bincode::serialize(&sb)?)?;
    buf.write_all(&vec![
        0u8;
        (fs::SUPERBLOCK_SIZE - bincode::serialized_size(&sb)?)
            as usize
    ])?;

    buf.flush()?;

    let file = std_fs::OpenOptions::new().write(true).open(path)?;
    Ok(file.set_len(1024 + bg_size * groups)?)
}

fn create_file<P: AsRef<Path>>(name: P) -> anyhow::Result<std_fs::File> {
    let file = std_fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(name)?;

    Ok(file)
}
