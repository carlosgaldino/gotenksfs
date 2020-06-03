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
    let bg_size = block_group_size(blk_size);
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
    let mut sb = fs::Superblock::new(blk_size, groups);

    sb.checksum();

    buf.write_all(&bincode::serialize(&sb)?)?;
    buf.write_all(&vec![
        0u8;
        (1024 as u64 - bincode::serialized_size(&sb)?) as usize
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

fn block_group_size(blk_size: u32) -> u64 {
    let x = blk_size + // data bitmap
        blk_size + // inode bitmap
        inode_table_size(blk_size) +
        data_table_size(blk_size);
    x as u64
}

fn inode_table_size(blk_size: u32) -> u32 {
    let inode_size = bincode::serialized_size(&fs::Inode::default()).unwrap() as u32;
    blk_size * 8 * inode_size.next_power_of_two()
}

fn data_table_size(blk_size: u32) -> u32 {
    blk_size * blk_size * 8
}
