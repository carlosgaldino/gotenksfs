use crate::gotenks::{types::Superblock, util, SUPERBLOCK_SIZE};
use anyhow::anyhow;
use byte_unit::{Byte, ByteUnit};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
};

pub fn make<P>(path: P, file_size: u64, blk_size: u32) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let bg_size = util::block_group_size(blk_size);
    if file_size < (bg_size - 2 * blk_size as u64) {
        return Err(anyhow!(format!(
            "File size must be at least {} for block size of {}. Specified size: {}",
            Byte::from_bytes(bg_size as _).get_appropriate_unit(true),
            Byte::from_bytes(blk_size as _).get_adjusted_unit(ByteUnit::B),
            Byte::from_bytes(file_size as _).get_appropriate_unit(true)
        )));
    }

    let groups = (file_size as f64 / bg_size as f64).ceil();
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut buf = BufWriter::new(&file);
    let uid = nix::unistd::geteuid().as_raw();
    let gid = nix::unistd::getegid().as_raw();
    let mut sb = Superblock::new(blk_size, groups as _, uid, gid);

    sb.serialize_into(&mut buf)?;

    buf.flush()?;

    Ok(file.set_len(SUPERBLOCK_SIZE + bg_size * groups as u64)?)
}
