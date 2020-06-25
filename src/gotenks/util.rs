use super::INODE_SIZE;
use std::time::{self, SystemTime};

#[inline]
pub fn calculate_checksum<S>(s: &S) -> u32
where
    S: serde::Serialize,
{
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bincode::serialize(&s).unwrap());
    hasher.finalize()
}

#[inline]
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[inline(always)]
pub fn block_group_size(blk_size: u32) -> u64 {
    let size = blk_size + // data bitmap
        blk_size + // inode bitmap
        inode_table_size(blk_size) +
        data_table_size(blk_size);
    size as u64
}

#[inline(always)]
pub fn inode_table_size(blk_size: u32) -> u32 {
    blk_size * 8 * INODE_SIZE as u32
}

#[inline(always)]
pub fn data_table_size(blk_size: u32) -> u32 {
    blk_size * blk_size * 8
}
