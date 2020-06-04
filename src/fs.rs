use serde::{Deserialize, Serialize};
use std::time::{self, SystemTime};

const GOTENKS_MAGIC: u32 = 0x64627a;
pub const SUPERBLOCK_SIZE: u64 = 1024;
pub const ROOT_INODE: u32 = 1;

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct Superblock {
    pub magic: u32,
    pub block_size: u32,
    pub created_at: u64,
    pub modified_at: Option<u64>,
    pub last_mounted_at: Option<u64>,
    pub block_count: u32,
    pub inode_count: u32,
    pub free_blocks: u32,
    pub free_inodes: u32,
    pub groups: u32,
    pub data_blocks_per_group: u32,
    pub checksum: u32,
}

impl Superblock {
    pub fn new(block_size: u32, groups: u32) -> Self {
        let total_blocks = block_size * 8 * groups;
        Self {
            block_size,
            groups,
            magic: GOTENKS_MAGIC,
            created_at: now(),
            modified_at: None,
            last_mounted_at: None,
            free_blocks: total_blocks,
            free_inodes: total_blocks,
            block_count: total_blocks,
            inode_count: total_blocks,
            data_blocks_per_group: block_size * 8,
            checksum: 0,
        }
    }

    pub fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = calculate_checksum(&self);
    }

    pub fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        checksum == calculate_checksum(&self)
    }

    pub fn update_last_mounted_at(&mut self) {
        self.last_mounted_at = Some(now());
    }

    pub fn update_modified_at(&mut self) {
        self.modified_at = Some(now());
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct Inode {
    pub mode: libc::mode_t,
    pub hard_links: u16,
    pub user_id: libc::uid_t,
    pub group_id: libc::gid_t,
    pub block_count: u64, // should be in 512 bytes blocks
    pub size: u64,
    pub created_at: u64,
    pub accessed_at: Option<i64>,
    pub modified_at: Option<i64>,
    pub changed_at: Option<i64>,
    pub direct_blocks: [u32; 12],
    pub checksum: u32,
}

impl Inode {
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|_e| anyhow!("Failed to save inode"))
    }

    #[allow(dead_code)]
    pub fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = calculate_checksum(&self);
    }
}

fn calculate_checksum<S>(s: &S) -> u32
where
    S: serde::Serialize,
{
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bincode::serialize(&s).unwrap());
    hasher.finalize()
}

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub(crate) fn block_group_size(blk_size: u32) -> u32 {
    blk_size + // data bitmap
        blk_size + // inode bitmap
        inode_table_size(blk_size) +
        data_table_size(blk_size)
}

pub(crate) fn inode_table_size(blk_size: u32) -> u32 {
    blk_size * 8 * inode_size()
}

pub(crate) fn data_table_size(blk_size: u32) -> u32 {
    blk_size * blk_size * 8
}

pub(crate) fn inode_size() -> u32 {
    let serialized_size = bincode::serialized_size(&Inode::default()).unwrap();
    serialized_size.next_power_of_two() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_new() {
        let sb = Superblock::new(1024, 3);
        assert_eq!(sb.free_inodes, 8192 * 3);
        assert_eq!(sb.free_blocks, 8192 * 3);
    }

    #[test]
    fn superblock_checksum() {
        let mut sb = Superblock::new(1024, 3);
        sb.checksum();

        assert_ne!(sb.checksum, 0);

        let checksum = sb.checksum;
        let mut sb = Superblock::new(1024, 3);
        sb.checksum();

        assert_eq!(sb.checksum, checksum);

        sb.last_mounted_at = Some(
            SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        sb.checksum();

        assert_ne!(sb.checksum, checksum);
    }

    #[test]
    fn inode_checksum() {
        let mut inode = Inode::default();
        inode.block_count = 24;
        inode.checksum();

        assert_ne!(inode.checksum, 0);

        let checksum = inode.checksum;
        let mut inode = Inode::default();
        inode.block_count = 24;
        inode.checksum();

        assert_eq!(inode.checksum, checksum);

        inode.accessed_at = Some(
            SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        inode.checksum();

        assert_ne!(inode.checksum, checksum);
    }
}
