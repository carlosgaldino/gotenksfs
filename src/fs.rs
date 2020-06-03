use serde::{Deserialize, Serialize};
use std::time::{self, SystemTime};

const GOTENKS_MAGIC: u32 = 0x64627a;

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
    pub checksum: u32,
}

impl Superblock {
    pub fn new(block_size: u32, groups: u64) -> Self {
        let total_blocks = block_size * 8 * groups as u32;
        Self {
            block_size,
            magic: GOTENKS_MAGIC,
            created_at: SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            modified_at: None,
            last_mounted_at: None,
            free_blocks: total_blocks,
            free_inodes: total_blocks,
            block_count: total_blocks,
            inode_count: total_blocks,
            checksum: 0,
        }
    }

    pub fn checksum(&mut self) {
        self.checksum = calculate_checksum(&self);
    }

    pub fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        checksum == calculate_checksum(&self)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct Inode {
    pub mode: libc::mode_t,
    pub hard_links: u64,
    pub user_id: libc::uid_t,
    pub group_id: libc::gid_t,
    pub block_count: u64, // should be in 512 bytes blocks
    pub size: u64,
    pub created_at: u64,
    pub accessed_at: Option<u64>,
    pub modified_at: Option<u64>,
    pub changed_at: Option<u64>,
    pub direct_blocks: [u32; 12],
    pub checksum: u32,
}

impl Inode {
    #[allow(dead_code)]
    pub fn checksum(&mut self) {
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
