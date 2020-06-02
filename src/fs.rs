use serde::{Deserialize, Serialize};
use std::time::{self, SystemTime};

const GOTENKS_MAGIC: u32 = 0x64627a;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Superblock {
    pub magic: u32,
    pub block_size: u32,
    pub created_at: u64,
    pub modified_at: Option<u64>,
    pub last_mounted_at: Option<u64>,
    pub free_blocks: u32,
    pub free_inodes: u32,
    pub checksum: u32,
}

impl Superblock {
    pub fn new(block_size: u32, groups: u64) -> Self {
        Self {
            magic: GOTENKS_MAGIC,
            block_size,
            created_at: SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            modified_at: None,
            last_mounted_at: None,
            free_blocks: block_size * 8 * groups as u32,
            free_inodes: block_size * 8 * groups as u32,
            checksum: 0,
        }
    }

    pub fn checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    fn calculate_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bincode::serialize(self).unwrap());
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let sb = Superblock::new(1024, 3);
        assert_eq!(sb.free_inodes, 8192 * 3);
        assert_eq!(sb.free_blocks, 8192 * 3);
    }

    #[test]
    fn test_checksum() {
        let mut sb = Superblock::new(1024, 3);
        sb.checksum();

        assert_ne!(sb.checksum, 0);

        let checksum = sb.checksum;
        let mut sb = Superblock::new(1024, 3);

        assert_eq!(sb.calculate_checksum(), checksum);

        sb.last_mounted_at = Some(
            SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        assert_ne!(sb.calculate_checksum(), checksum);
    }
}
