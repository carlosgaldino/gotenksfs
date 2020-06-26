use super::{util, DIRECT_POINTERS, GOTENKS_MAGIC, SUPERBLOCK_SIZE};
use anyhow::anyhow;
use bitvec::{order::Lsb0, vec::BitVec};
use fuse_rs::fs::FileStat;
use nix::errno::Errno;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{prelude::*, SeekFrom},
    path::Path,
};

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
    pub uid: u32,
    pub gid: u32,
    pub checksum: u32,
}

impl Superblock {
    pub fn new(block_size: u32, groups: u32, uid: u32, gid: u32) -> Self {
        let total_blocks = block_size * 8 * groups;
        Self {
            block_size,
            groups,
            uid,
            gid,
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
    next_inode: Option<usize>,
    next_data_block: Option<usize>,
}

impl Group {
    pub fn serialize_into<W>(mut w: W, groups: &[Group]) -> anyhow::Result<()>
    where
        W: Write + Seek,
    {
        assert!(!groups.is_empty());
        let blk_size = groups.first().unwrap().data_bitmap.len() / 8;
        for (i, g) in groups.iter().enumerate() {
            let offset = util::block_group_size(blk_size as u32) * i as u64 + SUPERBLOCK_SIZE;
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
            let offset = util::block_group_size(blk_size) * i as u64 + SUPERBLOCK_SIZE;
            r.seek(SeekFrom::Start(offset))?;
            r.read_exact(&mut buf)?;
            let data_bitmap = BitVec::<Lsb0, u8>::from_slice(&buf);
            r.read_exact(&mut buf)?;
            let inode_bitmap = BitVec::<Lsb0, u8>::from_slice(&buf);
            groups.push(Group::new(data_bitmap, inode_bitmap));
        }

        Ok(groups)
    }

    pub fn new(data_bitmap: BitVec<Lsb0, u8>, inode_bitmap: BitVec<Lsb0, u8>) -> Self {
        let mut group = Group {
            data_bitmap,
            inode_bitmap,
            ..Default::default()
        };
        group.next_data_block = group.next_free_data_block();
        group.next_inode = group.next_free_inode();

        group
    }

    #[inline]
    pub fn has_inode(&self, i: usize) -> bool {
        self.inode_bitmap.get(i - 1).unwrap_or(&false) == &true
    }

    #[inline]
    pub fn has_data_block(&self, i: usize) -> bool {
        self.data_bitmap.get(i - 1).unwrap_or(&false) == &true
    }

    #[inline]
    pub fn free_inodes(&self) -> usize {
        self.inode_bitmap.count_zeros()
    }

    #[inline]
    pub fn free_data_blocks(&self) -> usize {
        self.data_bitmap.count_zeros()
    }

    #[inline]
    pub fn allocate_inode(&mut self) -> Option<usize> {
        self.next_inode.and_then(|index| {
            self.add_inode(index);
            self.next_inode = self.next_free_inode();
            Some(index)
        })
    }

    #[inline]
    pub fn allocate_data_block(&mut self) -> Option<usize> {
        self.next_data_block.and_then(|index| {
            self.add_data_block(index);
            self.next_data_block = self.next_free_data_block();
            Some(index)
        })
    }

    #[inline]
    pub fn release_data_block(&mut self, index: usize) {
        self.data_bitmap.set(index - 1, false);
        self.next_data_block = self.next_free_data_block();
    }

    #[inline]
    pub fn release_inode(&mut self, index: usize) {
        self.inode_bitmap.set(index - 1, false);
        self.next_inode = self.next_free_inode();
    }

    #[inline]
    fn add_inode(&mut self, i: usize) {
        self.inode_bitmap.set(i - 1, true);
    }

    #[inline]
    fn add_data_block(&mut self, i: usize) {
        self.data_bitmap.set(i - 1, true);
    }

    #[inline]
    fn next_free_data_block(&self) -> Option<usize> {
        self.data_bitmap.iter().position(|bit| !*bit).map(|p| p + 1)
    }

    #[inline]
    fn next_free_inode(&self) -> Option<usize> {
        self.inode_bitmap
            .iter()
            .position(|bit| !*bit)
            .map(|p| p + 1)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Inode {
    pub mode: libc::mode_t,
    pub hard_links: u16,
    pub user_id: libc::uid_t,
    pub group_id: libc::gid_t,
    pub block_count: u32, // should be in 512 bytes blocks
    pub size: u64,
    pub created_at: u64,
    pub accessed_at: Option<i64>,
    pub modified_at: Option<i64>,
    pub changed_at: Option<i64>,
    pub direct_blocks: [u32; DIRECT_POINTERS as usize],
    pub indirect_block: u32,
    pub double_indirect_block: u32,
    pub checksum: u32,
}

impl Inode {
    pub fn new() -> Self {
        let mut inode = Self::default();
        let now = util::now();
        inode.created_at = now;
        inode.accessed_at = Some(now as i64);
        inode.modified_at = Some(now as i64);
        inode.changed_at = Some(now as i64);
        inode.hard_links = 1;
        inode
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

    pub fn deserialize_from<R: std::io::Read>(r: R) -> anyhow::Result<Self> {
        let mut inode: Self = bincode::deserialize_from(r)?;
        if !inode.verify_checksum() {
            return Err(anyhow!("Inode checksum verification failed"));
        }

        Ok(inode)
    }

    pub fn is_dir(&self) -> bool {
        (self.mode & libc::S_IFDIR) != 0
    }

    pub fn update_modified_at(&mut self) {
        let now = util::now();
        self.changed_at = Some(now as _);
        self.modified_at = Some(now as _);
    }

    pub fn update_accessed_at(&mut self) {
        self.accessed_at = Some(util::now() as _);
    }

    pub fn to_stat(&self, index: u32) -> FileStat {
        let mut stat = FileStat::new();
        stat.st_ino = index as _;
        stat.st_mode = self.mode;
        stat.st_nlink = self.hard_links;
        stat.st_atime = self.accessed_at.unwrap_or(0);
        stat.st_mtime = self.modified_at.unwrap_or(0);
        stat.st_ctime = self.changed_at.unwrap_or(0);
        stat.st_birthtime = self.created_at as _;
        stat.st_size = self.size as i64;
        stat.st_blocks = self.block_count as i64;
        stat.st_uid = self.user_id;
        stat.st_gid = self.group_id;

        stat
    }

    #[inline]
    pub fn direct_blocks(&self) -> Vec<u32> {
        self.direct_blocks
            .iter()
            .filter_map(|x| if *x != 0 { Some(*x) } else { None })
            .collect::<Vec<u32>>()
    }

    pub fn truncate(&mut self) -> Vec<u32> {
        self.update_modified_at();
        self.size = 0;
        self.block_count = 0;
        let blocks = self.direct_blocks();
        self.direct_blocks = [0u32; 12];
        blocks
    }

    pub fn find_direct_block(&self, index: usize) -> u32 {
        self.direct_blocks[index]
    }

    pub fn add_block(&mut self, block: u32, index: usize) -> anyhow::Result<()> {
        if index >= self.direct_blocks.len() {
            return Err(anyhow!("No space in direct blocks"));
        }
        self.direct_blocks[index] = block;
        Ok(())
    }

    pub fn adjust_size(&mut self, len: u64) {
        self.size = self.size.max(len);
        self.block_count = self.size as u32 / 512 + 1;
    }

    pub fn increment_size(&mut self, len: u64) {
        self.size += len;
        self.block_count = self.size as u32 / 512 + 1;
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

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Directory {
    pub entries: BTreeMap<OsString, u32>,
    checksum: u32,
}

impl Directory {
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
            return Err(anyhow!("Directory checksum verification failed"));
        }

        Ok(sb)
    }

    pub fn entry<P>(&self, path: P) -> fuse_rs::Result<u32>
    where
        P: AsRef<Path>,
    {
        self.entries
            .get(&path.as_ref().as_os_str().to_os_string())
            .ok_or(Errno::ENOENT)
            .map(|x| *x)
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
        let sb = Superblock::new(1024, 3, 0, 0);
        assert_eq!(sb.free_inodes, 8192 * 3);
        assert_eq!(sb.free_blocks, 8192 * 3);
        assert_eq!(sb.data_blocks_per_group, 1024 * 8);
    }

    #[test]
    fn superblock_checksum() -> anyhow::Result<()> {
        let mut sb = Superblock::new(1024, 3, 0, 0);
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
    fn inode_is_dir() {
        let mut inode = Inode::default();
        inode.mode = libc::S_IFREG | libc::S_IRWXO;
        assert!(!inode.is_dir());

        inode.mode |= libc::S_IFDIR;
        assert!(inode.is_dir());
    }

    #[test]
    fn inode_truncate() {
        let mut inode = Inode::new();
        inode.size = 512;
        inode.block_count = 1;
        inode.direct_blocks[0] = 23;
        assert!(!inode.direct_blocks.iter().all(|x| *x == 0));

        inode.truncate();
        assert_eq!(inode.size, 0);
        assert_eq!(inode.block_count, 0);
        assert!(inode.direct_blocks.iter().all(|x| *x == 0));
    }

    #[test]
    fn group_has_inode() {
        let mut bitmap = BitVec::<Lsb0, u8>::with_capacity(1024);
        bitmap.resize(1024, false);

        let mut group = Group::new(bitmap.clone(), bitmap);

        assert!(!group.has_inode(1));

        let index = group.allocate_inode().unwrap();
        assert_eq!(index, 1);
        assert!(group.has_inode(index));
        assert!(group.inode_bitmap[index - 1]);
        assert_eq!(group.next_inode, Some(index + 1));

        let index = group.allocate_inode().unwrap();
        assert_eq!(index, 2);
        assert!(group.has_inode(index));
        assert!(group.inode_bitmap[index - 1]);
        assert_eq!(group.next_inode, Some(index + 1));
    }

    #[test]
    fn group_has_data_block() {
        let mut bitmap = BitVec::<Lsb0, u8>::with_capacity(1024);
        bitmap.resize(1024, false);

        let mut group = Group::new(bitmap.clone(), bitmap);

        assert!(!group.has_data_block(1));

        let index = group.allocate_data_block().unwrap();
        assert_eq!(index, 1);
        assert!(group.has_data_block(index));
        assert!(group.data_bitmap[index - 1]);
        assert_eq!(group.next_data_block, Some(index + 1));

        let index = group.allocate_data_block().unwrap();
        assert_eq!(index, 2);
        assert!(group.has_data_block(index));
        assert!(group.data_bitmap[index - 1]);
        assert_eq!(group.next_data_block, Some(index + 1));
    }

    #[test]
    fn group_release_data_block() {
        let mut bitmap = BitVec::<Lsb0, u8>::with_capacity(1024);
        bitmap.resize(1024, false);

        let mut group = Group::new(bitmap.clone(), bitmap);

        assert_eq!(group.next_data_block.unwrap(), 1);
        for i in 0..3 {
            let index = group.allocate_data_block().unwrap();
            assert_eq!(index, i + 1);
        }
        for i in 0..3 {
            assert!(group.has_data_block(i + 1));
        }
        assert_eq!(group.next_data_block, Some(4));

        group.release_data_block(1);
        group.release_data_block(2);

        assert_eq!(group.next_data_block, Some(1));

        let index = group.allocate_data_block().unwrap();
        assert_eq!(index, 1);
        assert_eq!(group.next_data_block, Some(2));
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
            groups.push(Group::new(db, ib));
        }

        let buf = vec![0u8; SUPERBLOCK_SIZE as usize + block_group_size as usize * 3];
        let mut cursor = Cursor::new(buf);
        Group::serialize_into(&mut cursor, &groups)?;

        let deserialized = Group::deserialize_from(&mut cursor, 8, 3)?;
        for (i, g) in deserialized.into_iter().enumerate() {
            let (bitmap, next_data_block, next_inode) = if i & 1 == 0 {
                (0b10101010, 1, 2)
            } else {
                (0b01010101, 2, 1)
            };
            let vec = std::iter::repeat(bitmap).take(8).collect::<Vec<u8>>();
            assert_eq!(g.data_bitmap.into_vec(), vec);
            assert_eq!(g.next_data_block, Some(next_data_block));

            let vec = std::iter::repeat(!bitmap).take(8).collect::<Vec<u8>>();
            assert_eq!(g.inode_bitmap.into_vec(), vec);
            assert_eq!(g.next_inode, Some(next_inode));
        }

        Ok(())
    }

    #[test]
    fn directory_serialization() -> anyhow::Result<()> {
        let mut entries = BTreeMap::new();
        entries.insert(OsString::from("foo.txt"), 1);
        entries.insert(OsString::from("bar.txt"), 2);
        let mut dir = Directory {
            entries,
            checksum: 0,
        };

        let size = bincode::serialized_size(&dir)?;
        let buf = vec![0u8; size as _];
        let mut cursor = Cursor::new(buf);
        dir.serialize_into(&mut cursor)?;
        cursor.set_position(0);
        let deserialized = Directory::deserialize_from(cursor)?;

        assert_eq!(deserialized.entries.len(), 2);
        assert_ne!(deserialized.checksum, 0);
        for (i, (path, inode)) in deserialized.entries.iter().enumerate() {
            if i == 0 {
                assert_eq!(path, &OsString::from("bar.txt"));
                assert_eq!(*inode, 2);
            } else {
                assert_eq!(path, &OsString::from("foo.txt"));
                assert_eq!(*inode, 1);
            }
        }

        Ok(())
    }

    #[test]
    fn directory_entry() -> anyhow::Result<()> {
        let mut entries = BTreeMap::new();
        entries.insert(OsString::from("foo.txt"), 1);
        entries.insert(OsString::from("bar.txt"), 2);
        let dir = Directory {
            entries,
            checksum: 0,
        };

        assert_eq!(dir.entry("foo.txt")?, 1);
        assert_eq!(dir.entry("bar.txt")?, 2);
        assert_eq!(dir.entry("baz.txt").err(), Some(Errno::ENOENT));

        Ok(())
    }
}
