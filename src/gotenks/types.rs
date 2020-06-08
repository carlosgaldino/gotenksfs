use super::{util, GOTENKS_MAGIC, SUPERBLOCK_SIZE};
use anyhow::anyhow;
use bitvec::{order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};
use std::io::{prelude::*, SeekFrom};

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

    pub fn update_last_mounted_at(&mut self) {
        self.last_mounted_at = Some(util::now());
    }

    pub fn update_modified_at(&mut self) {
        self.modified_at = Some(util::now());
    }

    #[allow(dead_code)]
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

    pub fn deserialize_from<R>(r: R) -> anyhow::Result<Self>
    where
        R: Read,
    {
        let mut sb: Self = bincode::deserialize_from(r)?;
        if !sb.verify_checksum() {
            return Err(anyhow!("Superblock checksum verification failed"));
        }

        Ok(sb)
    }

    fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = util::calculate_checksum(&self);
    }

    fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        let ok = checksum == util::calculate_checksum(&self);
        self.checksum = checksum;

        ok
    }
}

#[derive(Debug, Default)]
pub struct Group {
    pub data_bitmap: BitVec<Lsb0, u8>,
    pub inode_bitmap: BitVec<Lsb0, u8>,
}

impl Group {
    pub fn serialize_into<W>(mut w: W, groups: &[Group]) -> anyhow::Result<()>
    where
        W: Write + Seek,
    {
        assert!(!groups.is_empty());
        let blk_size = groups.first().unwrap().data_bitmap.len() / 8;
        for (i, g) in groups.iter().enumerate() {
            let offset =
                util::block_group_size(blk_size as u32) as u64 * i as u64 + SUPERBLOCK_SIZE;
            w.seek(SeekFrom::Start(offset))?;
            w.write_all(g.data_bitmap.as_slice())?;
            w.write_all(g.inode_bitmap.as_slice())?;
        }

        Ok(())
    }

    pub fn deserialize_from<R>(mut r: R, blk_size: u32, count: usize) -> anyhow::Result<Vec<Group>>
    where
        R: Read + Seek,
    {
        let mut groups = Vec::with_capacity(count);
        let mut buf = Vec::with_capacity(blk_size as usize);
        unsafe {
            buf.set_len(blk_size as usize);
        }

        for i in 0..count {
            let offset = util::block_group_size(blk_size) as u64 * i as u64 + SUPERBLOCK_SIZE;
            r.seek(SeekFrom::Start(offset))?;
            r.read_exact(&mut buf)?;
            let data_bitmap = BitVec::<Lsb0, u8>::from_slice(&buf);
            r.read_exact(&mut buf)?;
            let inode_bitmap = BitVec::<Lsb0, u8>::from_slice(&buf);
            groups.push(Group {
                data_bitmap,
                inode_bitmap,
            });
        }

        Ok(groups)
    }

    pub fn has_inode(&self, i: usize) -> bool {
        self.inode_bitmap.get(i - 1).unwrap_or(&false) == &true
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
    #[allow(dead_code)]
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

    pub fn deserialize_from<R: std::io::Read>(r: R) -> anyhow::Result<Self> {
        let mut inode: Self = bincode::deserialize_from(r)?;
        if !inode.verify_checksum() {
            return Err(anyhow!("Inode checksum verification failed"));
        }

        Ok(inode)
    }

    fn checksum(&mut self) {
        self.checksum = 0;
        self.checksum = util::calculate_checksum(&self);
    }

    fn verify_checksum(&mut self) -> bool {
        let checksum = self.checksum;
        self.checksum = 0;
        let ok = checksum == util::calculate_checksum(&self);
        self.checksum = checksum;

        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::{self, SystemTime};

    #[test]
    fn superblock_new() {
        let sb = Superblock::new(1024, 3);
        assert_eq!(sb.free_inodes, 8192 * 3);
        assert_eq!(sb.free_blocks, 8192 * 3);
        assert_eq!(sb.data_blocks_per_group, 1024 * 8);
    }

    #[test]
    fn superblock_checksum() -> anyhow::Result<()> {
        let mut sb = Superblock::new(1024, 3);
        let buf = <Superblock>::serialize(&mut sb)?;
        let mut deserialised_sb = Superblock::deserialize_from(buf.as_slice())?;
        assert_ne!(deserialised_sb.checksum, 0);
        assert_eq!(deserialised_sb.checksum, sb.checksum);

        deserialised_sb.update_last_mounted_at();
        let buf = <Superblock>::serialize(&mut deserialised_sb)?;
        let deserialised_sb = Superblock::deserialize_from(buf.as_slice())?;

        assert_ne!(sb.checksum, deserialised_sb.checksum);
        Ok(())
    }

    #[test]
    fn inode_checksum() -> anyhow::Result<()> {
        let mut inode = Inode::default();
        inode.block_count = 24;
        let buf = <Inode>::serialize(&mut inode)?;
        let mut deserialised_inode = Inode::deserialize_from(buf.as_slice())?;
        assert_ne!(deserialised_inode.checksum, 0);
        assert_eq!(deserialised_inode.checksum, inode.checksum);

        deserialised_inode.accessed_at = Some(
            SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as _,
        );
        let buf = <Inode>::serialize(&mut deserialised_inode)?;
        let deserialised_inode = Inode::deserialize_from(buf.as_slice())?;

        assert_ne!(inode.checksum, deserialised_inode.checksum);

        Ok(())
    }

    #[test]
    fn group_has_inode() {
        let mut bitmap = BitVec::<Lsb0, u8>::with_capacity(1024);
        bitmap.resize(1024, false);

        let mut group = Group::default();
        group.inode_bitmap = bitmap;

        assert!(!group.has_inode(1));

        group.add_inode(1);
        assert!(group.has_inode(1));
        assert!(group.inode_bitmap[0]);

        group.add_inode(1024);
        assert!(group.has_inode(1024));
        assert!(group.inode_bitmap[1023]);
    }

    #[test]
    fn group_serialization() -> anyhow::Result<()> {
        let block_group_size = util::block_group_size(8);
        let mut groups = Vec::new();
        for i in 0..3 {
            let iter = std::iter::successors(Some(i & 1), |n| Some(n ^ 1));
            let mut db = BitVec::new();
            db.extend(iter.take(64).map(|n| n != 0));

            let iter = std::iter::successors(Some(i + 1 & 1), |n| Some(n ^ 1));
            let mut ib = BitVec::new();
            ib.extend(iter.take(64).map(|n| n != 0));
            groups.push(Group {
                data_bitmap: db,
                inode_bitmap: ib,
            });
        }

        let buf = vec![0u8; SUPERBLOCK_SIZE as usize + block_group_size as usize * 3];
        let mut cursor = Cursor::new(buf);
        Group::serialize_into(&mut cursor, &groups)?;

        let deserialized = Group::deserialize_from(&mut cursor, 8, 3)?;
        for (i, g) in deserialized.into_iter().enumerate() {
            let bitmap = if i & 1 == 0 { 0b10101010 } else { 0b01010101 };
            let vec = std::iter::repeat(bitmap).take(8).collect::<Vec<u8>>();
            assert_eq!(g.data_bitmap.into_vec(), vec);

            let vec = std::iter::repeat(!bitmap).take(8).collect::<Vec<u8>>();
            assert_eq!(g.inode_bitmap.into_vec(), vec);
        }

        Ok(())
    }
}
