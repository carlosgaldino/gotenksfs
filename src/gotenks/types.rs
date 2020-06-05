use super::{util, GOTENKS_MAGIC, SUPERBLOCK_SIZE};
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Superblock {
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
            created_at: util::now(),
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

    pub fn size() -> u64 {
        SUPERBLOCK_SIZE
    }

    pub fn serialize(&mut self) -> anyhow::Result<Vec<u8>> {
        self.checksum();
        bincode::serialize(self).map_err(|e| e.into())
    }

    pub fn serialize_into<W>(&mut self, w: W) -> anyhow::Result<()>
    where
        W: Write,
    {
        self.checksum();
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    pub fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        checksum == util::calculate_checksum(&self)
    }

    pub fn update_last_mounted_at(&mut self) {
        self.last_mounted_at = Some(util::now());
    }

    pub fn update_modified_at(&mut self) {
        self.modified_at = Some(util::now());
    }

    fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = util::calculate_checksum(&self);
    }
}

#[derive(Debug)]
pub struct Group {
    pub data_bitmap: BitVec<Lsb0, u8>,
    pub inode_bitmap: BitVec<Lsb0, u8>,
}

impl Group {
    pub fn has_inode(&self, i: usize) -> bool {
        let mut x = i;
        if x > 0 {
            x -= 1;
        }
        let b = self.inode_bitmap.get(x).unwrap_or(&false);
        b == &true
    }

    pub fn add_inode(&mut self, i: usize) {
        self.inode_bitmap.set(i - 1, true);
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Inode {
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
    pub fn size() -> u32 {
        let serialized_size = bincode::serialized_size(&Self::default()).unwrap();
        serialized_size.next_power_of_two() as u32
    }

    pub fn serialize(&mut self) -> anyhow::Result<Vec<u8>> {
        self.checksum();
        bincode::serialize(self).map_err(|e| e.into())
    }

    pub fn serialize_into<W>(&mut self, w: W) -> anyhow::Result<()>
    where
        W: Write,
    {
        self.checksum();
        bincode::serialize_into(w, self).map_err(|e| e.into())
    }

    #[allow(dead_code)]
    pub fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = util::calculate_checksum(&self);
    }
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
