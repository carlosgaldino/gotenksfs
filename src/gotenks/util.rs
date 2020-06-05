use super::types::Inode;
use std::time::{self, SystemTime};

pub fn calculate_checksum<S>(s: &S) -> u32
where
    S: serde::Serialize,
{
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bincode::serialize(&s).unwrap());
    hasher.finalize()
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn block_group_size(blk_size: u32) -> u32 {
    blk_size + // data bitmap
        blk_size + // inode bitmap
        inode_table_size(blk_size) +
        data_table_size(blk_size)
}

pub fn inode_table_size(blk_size: u32) -> u32 {
    blk_size * 8 * Inode::size()
}

pub fn data_table_size(blk_size: u32) -> u32 {
    blk_size * blk_size * 8
}
