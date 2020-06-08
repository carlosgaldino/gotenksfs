use super::{
    types::{Group, Inode, Superblock},
    util, INODE_SIZE, ROOT_INODE, SUPERBLOCK_SIZE,
};
use fs::OpenOptions;
use io::{Cursor, SeekFrom};
use memmap::MmapMut;
use nix::{errno::Errno, sys::stat::SFlag};
use std::{
    fs,
    io::{self, prelude::*},
    mem,
    path::Path,
};

#[derive(Debug, Default)]
pub struct GotenksFS {
    pub sb: Option<Superblock>,
    pub mmap: Option<MmapMut>,
    pub groups: Option<Vec<Group>>,
}

impl GotenksFS {
    pub fn new<P>(image_path: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(image_path.as_ref())?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let mut cursor = Cursor::new(&mmap);
        let sb: Superblock = Superblock::deserialize_from(&mut cursor)?;
        let groups = Group::deserialize_from(&mut cursor, sb.block_size, sb.groups as usize)?;

        let mut fs = Self {
            sb: Some(sb),
            groups: Some(groups),
            mmap: Some(mmap),
        };

        fs.create_root()?;

        Ok(fs)
    }

    pub fn create_root(&mut self) -> anyhow::Result<()> {
        let group = self.groups_mut().get_mut(0).unwrap();
        if group.has_inode(ROOT_INODE as _) {
            return Ok(());
        }

        let mut inode = Inode::default();
        inode.mode = SFlag::S_IFDIR.bits() | 0o777;
        inode.hard_links = 2;
        inode.created_at = util::now();

        group.add_inode(ROOT_INODE as usize);
        self.save_inode(&mut inode, ROOT_INODE)
    }

    fn save_inode(&mut self, inode: &mut Inode, index: u32) -> anyhow::Result<()> {
        let offset = self.inode_seek_position(index);
        let buf = self.mmap_mut().as_mut();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(offset))?;

        Ok(inode.serialize_into(&mut cursor)?)
    }

    fn find_inode(&self, index: u32) -> fuse_rs::Result<Inode> {
        let (group_index, _bitmap_index) = self.inode_offsets(index);
        if !self
            .groups()
            .get(group_index as usize)
            .unwrap()
            .has_inode(index as usize)
        {
            return Err(Errno::ENOENT);
        }

        let offset = self.inode_seek_position(index);
        let buf = self.mmap();
        let mut cursor = Cursor::new(buf);
        cursor
            .seek(SeekFrom::Start(offset))
            .map_err(|_e| Errno::EIO)?;

        let inode = Inode::deserialize_from(cursor).map_err(|_e| Errno::EIO)?;
        Ok(inode)
    }

    // (group_block_index, bitmap_index)
    fn inode_offsets(&self, index: u32) -> (u32, u32) {
        let inodes_per_group = self.superblock().data_blocks_per_group;
        let inode_bg = (index - 1) / inodes_per_group;
        (inode_bg, (index - 1) & (inodes_per_group - 1))
    }

    fn inode_seek_position(&self, index: u32) -> u64 {
        let (group_index, bitmap_index) = self.inode_offsets(index);
        let block_size = self.superblock().block_size;
        let seek_pos = group_index * util::block_group_size(block_size)
            + 2 * block_size
            + bitmap_index * INODE_SIZE as u32
            + SUPERBLOCK_SIZE as u32;

        seek_pos as u64
    }

    fn groups(&self) -> &[Group] {
        self.groups.as_ref().unwrap()
    }

    fn groups_mut(&mut self) -> &mut [Group] {
        self.groups.as_mut().unwrap()
    }

    fn superblock(&self) -> &Superblock {
        self.sb.as_ref().unwrap()
    }

    fn superblock_mut(&mut self) -> &mut Superblock {
        self.sb.as_mut().unwrap()
    }

    fn mmap(&self) -> &MmapMut {
        self.mmap.as_ref().unwrap()
    }

    fn mmap_mut(&mut self) -> &mut MmapMut {
        self.mmap.as_mut().unwrap()
    }
}

impl fuse_rs::Filesystem for GotenksFS {
    fn metadata(&self, path: &Path) -> fuse_rs::Result<fuse_rs::fs::FileStat> {
        let mut stat = fuse_rs::fs::FileStat::new();
        match path.to_str().expect("path") {
            "/" => {
                let inode = self.find_inode(ROOT_INODE)?;
                stat.st_ino = ROOT_INODE as _;
                stat.st_mode = inode.mode;
                stat.st_nlink = inode.hard_links;
                stat.st_atime = inode.accessed_at.unwrap_or(0);
                stat.st_mtime = inode.modified_at.unwrap_or(0);
                stat.st_birthtime = inode.created_at as _;
            }
            _ => return Err(Errno::ENOENT),
        }
        Ok(stat)
    }

    fn statfs(&self, path: &Path) -> fuse_rs::Result<libc::statvfs> {
        if path == Path::new("/") {
            let sb = self.superblock();
            let stat = libc::statvfs {
                f_bsize: sb.block_size as u64,
                f_frsize: sb.block_size as u64,
                f_blocks: sb.block_count,
                f_bfree: sb.free_blocks,
                f_bavail: sb.free_blocks,
                f_files: sb.inode_count,
                f_ffree: sb.free_inodes,
                f_favail: sb.free_inodes,
                f_namemax: 255,
                f_fsid: 0, // ignored by fuse
                f_flag: 0, // ignored by fuse
            };

            Ok(stat)
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn init(&mut self, _connection_info: &mut fuse_rs::fs::ConnectionInfo) -> fuse_rs::Result<()> {
        let sb = self.superblock_mut();
        sb.update_last_mounted_at();
        sb.update_modified_at();

        Ok(())
    }

    fn destroy(&mut self) -> fuse_rs::Result<()> {
        let mut mmap = mem::replace(&mut self.mmap, None).unwrap();
        let buf = mmap.as_mut();
        let mut cursor = Cursor::new(buf);

        self.superblock_mut()
            .serialize_into(&mut cursor)
            .map_err(|_| Errno::EIO)?;

        Group::serialize_into(&mut cursor, self.groups()).map_err(|_| Errno::EIO)?;

        Ok(mmap.flush().map_err(|_| Errno::EIO)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gotenks::{types::Superblock, util, INODE_SIZE, ROOT_INODE},
        mkfs,
    };
    use fuse_rs::Filesystem;
    use std::path::PathBuf;

    #[test]
    fn inode_offsets() {
        let mut fs = GotenksFS::default();
        fs.sb = Some(Superblock::new(1024, 3));
        fs.superblock_mut().data_blocks_per_group = 1024 * 8;

        let (group_index, offset) = fs.inode_offsets(1);
        assert_eq!(group_index, 0);
        assert_eq!(offset, 0);

        let (group_index, offset) = fs.inode_offsets(1024 * 8);
        assert_eq!(group_index, 0);
        assert_eq!(offset, 8191);

        let (group_index, offset) = fs.inode_offsets(1024 * 8 - 1);
        assert_eq!(group_index, 0);
        assert_eq!(offset, 8190);

        let (group_index, offset) = fs.inode_offsets(2 * 1024 * 8 - 1);
        assert_eq!(group_index, 1);
        assert_eq!(offset, 8190);
    }

    #[test]
    fn inode_seek_position() {
        let mut fs = GotenksFS::default();
        fs.sb = Some(Superblock::new(1024, 3));
        fs.superblock_mut().data_blocks_per_group = 1024 * 8;

        let offset = fs.inode_seek_position(1);
        assert_eq!(3072, offset);

        let offset = fs.inode_seek_position(2);
        assert_eq!(3072 + INODE_SIZE, offset);

        let offset = fs.inode_seek_position(8192);
        assert_eq!(3072 + 8191 * INODE_SIZE, offset); // superblock + data bitmap + inode bitmap + 8191 inodes
    }

    #[test]
    fn new_fs() -> anyhow::Result<()> {
        let tmp_file = make_fs("new_fs")?;
        let mut fs = GotenksFS::new(&tmp_file)?;
        let inode = fs.find_inode(ROOT_INODE)?;

        assert_eq!(inode.mode, SFlag::S_IFDIR.bits() | 0o777);
        assert_eq!(inode.hard_links, 2);

        assert!(fs.groups().get(0).unwrap().has_inode(ROOT_INODE as _));

        assert_eq!(fs.superblock().groups, fs.groups().len() as u32);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn init_destroy() -> anyhow::Result<()> {
        let tmp_file = make_fs("init_destroy")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        assert_eq!(fs.superblock().last_mounted_at, None);

        fs.init(&mut fuse_rs::fs::ConnectionInfo::default())?;
        fs.destroy()?;

        let fs = GotenksFS::new(&tmp_file)?;

        assert_ne!(fs.superblock().last_mounted_at, None);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn metadata() -> anyhow::Result<()> {
        let tmp_file = make_fs("metadata")?;
        let fs = GotenksFS::new(&tmp_file)?;
        let inode = fs.metadata(Path::new("/"))?;

        assert_eq!(inode.st_ino, ROOT_INODE as u64);
        assert_eq!(inode.st_mode, SFlag::S_IFDIR.bits() | 0o777);
        assert_eq!(inode.st_nlink, 2);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    fn make_fs(name: &str) -> anyhow::Result<PathBuf> {
        let mut tmp_file = std::env::temp_dir();
        tmp_file.push(name);
        tmp_file.set_extension("img");
        if tmp_file.exists() {
            std::fs::remove_file(&tmp_file)?;
        }

        let block_size = 128;
        let block_group_size = util::block_group_size(block_size);
        mkfs::make(&tmp_file, block_group_size as u64, block_size)?;

        Ok(tmp_file)
    }
}
