use super::{
    types::{Directory, Group, Inode, Superblock},
    util, INODE_SIZE, ROOT_INODE, SUPERBLOCK_SIZE,
};
use anyhow::anyhow;
use fs::OpenOptions;
use fuse_rs::fs::FileStat;
use io::{Cursor, SeekFrom};
use memmap::MmapMut;
use nix::{
    errno::Errno,
    sys::stat::{Mode, SFlag},
};
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

        let mut inode = Inode::new();
        inode.mode = SFlag::S_IFDIR.bits() | 0o777;
        inode.hard_links = 2;

        let dir = Directory::default();

        let index = self
            .allocate_inode()
            .ok_or_else(|| anyhow!("No space left for inodes"))?;
        assert_eq!(index, ROOT_INODE);

        inode.add_block(
            self.allocate_data_block()
                .ok_or_else(|| anyhow!("No space left for data"))?,
            0,
        );
        self.save_inode(inode, index)?;
        self.save_dir(dir, index)
    }

    fn save_inode(&mut self, mut inode: Inode, index: u32) -> anyhow::Result<()> {
        let offset = self.inode_seek_position(index);
        let buf = self.mmap_mut().as_mut();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(offset))?;

        Ok(inode.serialize_into(&mut cursor)?)
    }

    fn save_dir(&mut self, mut dir: Directory, index: u32) -> anyhow::Result<()> {
        let mut inode = self.find_inode(index)?;
        inode.update_modified_at();
        self.save_inode(inode, index)?;

        let offset = self.data_block_seek_position(index);
        let buf = self.mmap_mut().as_mut();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(offset))?;

        Ok(dir.serialize_into(&mut cursor)?)
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

    fn find_inode_from_path<P>(&self, path: P) -> fuse_rs::Result<(Inode, u32)>
    where
        P: AsRef<Path>,
    {
        match path.as_ref().parent() {
            None => Ok((self.find_inode(ROOT_INODE)?, ROOT_INODE)),
            Some(parent) => {
                let (parent, _) = self.find_dir(parent)?;
                let index = parent.entry(
                    path.as_ref()
                        .file_name()
                        .ok_or(Errno::EINVAL)?
                        .to_os_string(),
                )?;
                Ok((self.find_inode(index)?, index))
            }
        }
    }

    fn find_dir<P>(&self, path: P) -> fuse_rs::Result<(Directory, u32)>
    where
        P: AsRef<Path>,
    {
        let mut current = self.find_dir_from_inode(ROOT_INODE)?;
        let mut index = ROOT_INODE;
        for c in path.as_ref().components().skip(1) {
            index = current.entry(c)?;
            current = self.find_dir_from_inode(index)?;
        }

        Ok((current, index))
    }

    fn find_dir_from_inode(&self, index: u32) -> fuse_rs::Result<Directory> {
        let inode = self.find_inode(index)?;
        if !inode.is_dir() {
            return Err(Errno::ENOTDIR);
        }

        // TODO: support more blocks
        let block = inode.direct_blocks[0];
        let (group_index, _) = self.data_block_offsets(index);
        if !self
            .groups()
            .get(group_index as usize)
            .unwrap()
            .has_data_block(block as usize)
        {
            return Err(Errno::ENOENT);
        }

        let mut cursor = Cursor::new(self.mmap().as_ref());
        cursor
            .seek(SeekFrom::Start(self.data_block_seek_position(block)))
            .map_err(|_| Errno::EIO)?;

        Directory::deserialize_from(cursor).map_err(|_| Errno::EIO)
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

    fn data_block_offsets(&self, index: u32) -> (u32, u32) {
        let data_blocks_per_group = self.superblock().data_blocks_per_group;
        let group_index = (index - 1) / data_blocks_per_group;
        let block_index = (index - 1) & (data_blocks_per_group - 1);

        (group_index, block_index)
    }

    fn data_block_seek_position(&self, index: u32) -> u64 {
        let (group_index, block_index) = self.data_block_offsets(index);
        let block_size = self.superblock().block_size;
        let seek_pos = group_index * util::block_group_size(block_size)
            + 2 * block_size
            + self.superblock().data_blocks_per_group * INODE_SIZE as u32
            + SUPERBLOCK_SIZE as u32
            + block_size * block_index;

        seek_pos as u64
    }

    fn allocate_inode(&mut self) -> Option<u32> {
        let mut groups = self
            .groups()
            .iter()
            .enumerate()
            .map(|(index, g)| (index, g.free_inodes()))
            .collect::<Vec<(usize, usize)>>();

        // TODO: handle when group has run out of space
        groups.sort_by(|a, b| a.1.cmp(&b.1));
        let (group_index, _) = groups.first().unwrap();
        self.superblock_mut().free_inodes -= 1;
        let group = self.groups_mut().get_mut(*group_index).unwrap();

        let index = group.allocate_inode()?;
        Some(index as u32 + *group_index as u32 * self.superblock().data_blocks_per_group)
    }

    fn allocate_data_block(&mut self) -> Option<u32> {
        let mut groups = self
            .groups()
            .iter()
            .enumerate()
            .map(|(index, g)| (index, g.free_inodes()))
            .collect::<Vec<(usize, usize)>>();

        // TODO: handle when group has run out of space
        groups.sort_by(|a, b| a.1.cmp(&b.1));
        let (group_index, _) = groups.first().unwrap();
        self.superblock_mut().free_blocks -= 1;
        let group = self.groups_mut().get_mut(*group_index).unwrap();

        let index = group.allocate_data_block()?;
        Some(index as u32 + *group_index as u32 * self.superblock().data_blocks_per_group)
    }

    fn release_data_blocks(&mut self, blocks: &[u32]) {
        for block in blocks {
            let (group_index, block_index) = self.data_block_offsets(*block);
            // TODO: release multiple blocks from the same group in a single call
            self.groups_mut()
                .get_mut(group_index as usize)
                .unwrap()
                .release_data_block(1 + block_index as usize);
        }
    }

    fn write_data(&mut self, data: &[u8], offset: u64, block_index: u32) -> anyhow::Result<usize> {
        let block_offset = self.data_block_seek_position(block_index);
        let buf = self.mmap_mut().as_mut();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(block_offset + offset))?;

        Ok(cursor.write(data)?)
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
    fn metadata(&self, path: &Path) -> fuse_rs::Result<FileStat> {
        let (inode, index) = self.find_inode_from_path(path)?;
        Ok(inode.to_stat(index))
    }

    fn read_dir(
        &mut self,
        path: &Path,
        _offset: u64,
        _file_info: fuse_rs::fs::FileInfo,
    ) -> fuse_rs::Result<Vec<fuse_rs::fs::DirEntry>> {
        // TODO: check permissions
        let (dir, _index) = self.find_dir(path)?;

        let mut entries = Vec::with_capacity(dir.entries.len());
        for (name, index) in dir.entries {
            let inode = self.find_inode(index)?;
            let stat = inode.to_stat(index);
            entries.push(fuse_rs::fs::DirEntry {
                name,
                metadata: Some(stat),
                offset: None,
            });
        }

        Ok(entries)
    }

    fn create(
        &mut self,
        path: &Path,
        permissions: Mode,
        file_info: &mut fuse_rs::fs::OpenFileInfo,
    ) -> fuse_rs::Result<()> {
        let index = self.allocate_inode().ok_or_else(|| Errno::ENOSPC)?;
        let mut inode = Inode::new();
        inode.mode = permissions.bits();
        inode.user_id = self.superblock().uid;
        inode.group_id = self.superblock().gid;

        let (mut parent, parent_index) = self.find_dir(path.parent().ok_or(Errno::EINVAL)?)?;
        parent.entries.insert(
            path.file_name()
                .map(|p| p.to_os_string())
                .ok_or(Errno::EINVAL)?,
            index,
        );

        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;
        self.save_dir(parent, parent_index)
            .map_err(|_| Errno::EIO)?;

        file_info.set_handle(index as u64);
        Ok(())
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

    fn open(
        &mut self,
        path: &Path,
        file_info: &mut fuse_rs::fs::OpenFileInfo,
    ) -> fuse_rs::Result<()> {
        // TODO: check permissions
        let (mut inode, index) = self.find_inode_from_path(path)?;
        inode.update_accessed_at();

        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;
        file_info.set_handle(index as u64);

        Ok(())
    }

    fn write(
        &mut self,
        _path: &Path,
        buf: &[u8],
        offset: u64,
        file_info: &mut fuse_rs::fs::WriteFileInfo,
    ) -> fuse_rs::Result<usize> {
        let index = file_info.handle().ok_or(Errno::EINVAL)? as u32;
        if index == 0 {
            return Err(Errno::EINVAL);
        }
        let mut total_wrote = 0;
        let mut inode = self.find_inode(index)?;
        let overwrite = inode.size > offset;
        let mut offset = offset;
        let blk_size = self.superblock().block_size;

        while total_wrote != buf.len() {
            let direct_block_index = offset / blk_size as u64;
            let (block_index, space_left) = match inode.next_block(offset, blk_size as u64) {
                None => {
                    let block_index = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
                    (block_index, blk_size)
                }
                Some(x) => x,
            };

            let max_write_len = buf.len().min(space_left as usize);
            let offset_in_block = if total_wrote != 0 {
                0
            } else {
                offset - direct_block_index * blk_size as u64
            };
            let wrote = self
                .write_data(
                    &buf[total_wrote..buf.len().min(max_write_len + total_wrote)],
                    offset_in_block,
                    block_index,
                )
                .map_err(|_| Errno::EIO)?;

            inode.add_block(block_index, direct_block_index as usize);
            total_wrote += wrote;
            offset += wrote as u64;
        }

        inode.update_modified_at();
        if overwrite {
            inode.adjust_size(total_wrote as u64);
        } else {
            inode.increment_size(total_wrote as u64);
        }
        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;
        self.mmap_mut().flush().map_err(|_| Errno::EIO)?;
        Ok(total_wrote)
    }

    fn ftruncate(
        &mut self,
        _path: &Path,
        _len: u64,
        file_info: fuse_rs::fs::FileInfo,
    ) -> fuse_rs::Result<()> {
        let index = file_info.handle().ok_or(Errno::EINVAL)? as u32;
        if index == 0 {
            return Err(Errno::EINVAL);
        }
        let mut inode = self.find_inode(index)?;

        // TODO: truncate using the length arg
        let blocks = inode.truncate();
        self.release_data_blocks(&blocks);
        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;

        Ok(())
    }

    fn fmetadata(
        &self,
        _path: &Path,
        file_info: fuse_rs::fs::FileInfo,
    ) -> fuse_rs::Result<FileStat> {
        let index = file_info.handle().ok_or(Errno::EINVAL)? as u32;
        if index == 0 {
            return Err(Errno::EINVAL);
        }
        let inode = self.find_inode(index)?;
        Ok(inode.to_stat(index))
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
    use fuse_rs::{fs::FileStat, Filesystem};
    use std::{ffi::OsString, path::PathBuf};

    const BLOCK_SIZE: u32 = 128;

    #[test]
    fn inode_offsets() {
        let mut fs = GotenksFS::default();
        fs.sb = Some(Superblock::new(1024, 3, 0, 0));
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
        fs.sb = Some(Superblock::new(1024, 3, 0, 0));
        fs.superblock_mut().data_blocks_per_group = 1024 * 8;

        let offset = fs.inode_seek_position(1);
        assert_eq!(3072, offset);

        let offset = fs.inode_seek_position(2);
        assert_eq!(3072 + INODE_SIZE, offset);

        let offset = fs.inode_seek_position(8192);
        assert_eq!(3072 + 8191 * INODE_SIZE, offset); // superblock + data bitmap + inode bitmap + 8191 inodes

        let offset = fs.inode_seek_position(8193);
        assert_eq!(3072 + 8192 * INODE_SIZE + 1024 * 1024 * 8 + 2048, offset); // superblock + data bitmap + inode bitmap + inode table + data blocks + data bitmap + inode bitmap
    }

    #[test]
    fn new_fs() -> anyhow::Result<()> {
        let tmp_file = make_fs("new_fs")?;
        let fs = GotenksFS::new(&tmp_file)?;
        let inode = fs.find_inode(ROOT_INODE)?;

        assert_eq!(inode.mode, SFlag::S_IFDIR.bits() | 0o777);
        assert_eq!(inode.hard_links, 2);

        assert!(fs.groups().get(0).unwrap().has_inode(ROOT_INODE as _));
        assert!(fs.groups().get(0).unwrap().has_data_block(ROOT_INODE as _));

        assert_eq!(fs.superblock().groups, fs.groups().len() as u32);
        assert_eq!(fs.superblock().free_inodes, BLOCK_SIZE * 8 - 1);
        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 1);

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
        assert_eq!(fs.superblock().free_inodes, BLOCK_SIZE * 8 - 1);
        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 1);

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
        assert_ne!(inode.st_mtime, 0);
        assert_ne!(inode.st_ctime, 0);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn data_block_seek_position() {
        let mut fs = GotenksFS::default();
        let block_size = 1024;
        fs.sb = Some(Superblock::new(block_size, 3, 0, 0));
        fs.superblock_mut().data_blocks_per_group = block_size * 8;

        let prefix = SUPERBLOCK_SIZE + 2 * block_size as u64 + block_size as u64 * INODE_SIZE * 8;
        let offset = fs.data_block_seek_position(1);
        assert_eq!(prefix, offset);

        let offset = fs.data_block_seek_position(2);
        assert_eq!(prefix + block_size as u64, offset);

        let offset = fs.data_block_seek_position(8192);
        assert_eq!(prefix + 8191 * block_size as u64, offset);

        let offset = fs.data_block_seek_position(8193);
        assert_eq!(
            2 * prefix - SUPERBLOCK_SIZE + (block_size * block_size) as u64 * 8,
            offset
        );
    }

    #[test]
    fn save_dir() -> anyhow::Result<()> {
        let tmp_file = make_fs("save_dir")?;
        let mut fs = GotenksFS::new(&tmp_file)?;
        let dir = fs.find_dir_from_inode(ROOT_INODE)?;

        assert_eq!(dir.entries.len(), 0);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn find_dir() -> anyhow::Result<()> {
        let tmp_file = make_fs("find_dir")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        assert_eq!(fs.find_dir("/not-a-dir").err(), Some(Errno::ENOENT));

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn read_dir() -> anyhow::Result<()> {
        let tmp_file = make_fs("read_dir")?;
        let mut fs = GotenksFS::new(&tmp_file)?;
        let inode = fs.find_inode(ROOT_INODE)?;

        assert_ne!(inode.accessed_at, None);

        let file_info = fuse_rs::fs::FileInfo::default();
        let entries = fs.read_dir(Path::new("/"), 0, file_info)?;
        assert_eq!(entries.len(), 0);

        fs.create(
            Path::new("/foo.txt"),
            nix::sys::stat::Mode::S_IRWXO,
            &mut fuse_rs::fs::OpenFileInfo::default(),
        )?;
        fs.create(
            Path::new("/bar.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut fuse_rs::fs::OpenFileInfo::default(),
        )?;

        assert_eq!(fs.superblock().free_inodes, BLOCK_SIZE * 8 - 3);

        let file_info = fuse_rs::fs::FileInfo::default();
        let entries = fs.read_dir(Path::new("/"), 0, file_info)?;
        assert_eq!(entries.len(), 2);

        let bar = entries.first().unwrap();
        let mut stat = FileStat::default();
        let mode = nix::sys::stat::Mode::S_IRWXU.bits();
        stat.st_mode = mode;
        stat.st_ino = 3;
        assert_eq!(bar.name, OsString::from("bar.txt"));
        assert_eq!(bar.metadata.as_ref().unwrap().st_ino, 3);
        assert_eq!(bar.metadata.as_ref().unwrap().st_mode, mode);

        let foo = entries.last().unwrap();
        let mut stat = FileStat::default();
        let mode = nix::sys::stat::Mode::S_IRWXO.bits();
        stat.st_mode = mode;
        stat.st_ino = 2;
        assert_eq!(foo.name, OsString::from("foo.txt"));
        assert_eq!(foo.metadata.as_ref().unwrap().st_ino, 2);
        assert_eq!(foo.metadata.as_ref().unwrap().st_mode, mode);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn open() -> anyhow::Result<()> {
        let tmp_file = make_fs("open")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        let mut file_info = fuse_rs::fs::OpenFileInfo::default();
        assert_eq!(
            fs.open(Path::new("/hello.txt"), &mut file_info).err(),
            Some(Errno::ENOENT)
        );

        fs.create(
            Path::new("/bar.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut file_info,
        )?;

        fs.open(Path::new("/bar.txt"), &mut file_info)?;

        assert_eq!(file_info.handle(), Some(2));

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn write() -> anyhow::Result<()> {
        let tmp_file = make_fs("write")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        let mut open_fi = fuse_rs::fs::OpenFileInfo::default();
        fs.create(
            Path::new("/bar.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut open_fi,
        )?;

        fs.open(Path::new("/bar.txt"), &mut open_fi)?;
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(open_fi.handle().unwrap());

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3).take(125).collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, 125);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 125);
        assert_eq!(stat.st_blocks, 1);

        // Overwriting with larger buffer
        let buf = std::iter::repeat(4).take(126).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, 126);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 126);
        assert_eq!(stat.st_blocks, 1); // 126 / 512 + 1

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);

        let modified_at = inode.modified_at;
        let changed_at = inode.changed_at;

        // Overwriting with shorter buffer
        let buf = std::iter::repeat(5).take(120).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, 120);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 126);
        assert_eq!(stat.st_blocks, 1); // 126 / 512 + 1

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);

        // Appending
        let buf = std::iter::repeat(7).take(125).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 126, &mut write_file_info)?;
        assert_eq!(wrote, 125);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 251);
        assert_eq!(stat.st_blocks, 1); // 251 / 512 + 1

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);
        assert_eq!(inode.direct_blocks[1], 3);

        // Appending again
        let buf = std::iter::repeat(8).take(125).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 251, &mut write_file_info)?;
        assert_eq!(wrote, 125);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 376);
        assert_eq!(stat.st_blocks, 1); // 376 / 512 + 1

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);
        assert_eq!(inode.direct_blocks[1], 3);
        assert_eq!(inode.direct_blocks[2], 4);

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Overwriting in the middle
        let buf = std::iter::repeat(9).take(125).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 126, &mut write_file_info)?;
        assert_eq!(wrote, 125);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 376);
        assert_eq!(stat.st_blocks, 1); // 376 / 512 + 1

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);
        assert_eq!(inode.direct_blocks[1], 3);
        assert_eq!(inode.direct_blocks[2], 4);

        assert_ne!(inode.modified_at, modified_at);
        assert_ne!(inode.changed_at, changed_at);

        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 4);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn append_only() -> anyhow::Result<()> {
        let tmp_file = make_fs("append_only")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        let mut open_fi = fuse_rs::fs::OpenFileInfo::default();
        fs.create(
            Path::new("/bar.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut open_fi,
        )?;

        fs.open(Path::new("/bar.txt"), &mut open_fi)?;
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(open_fi.handle().unwrap());

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3)
            .take(2 * BLOCK_SIZE as usize)
            .collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, buf.len());

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, buf.len() as _);
        assert_eq!(stat.st_blocks, 1);

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);
        assert_eq!(inode.direct_blocks[1], 3);

        let buf = std::iter::repeat(4)
            .take(BLOCK_SIZE as _)
            .collect::<Vec<u8>>();

        let wrote = fs.write(
            Path::new("/ignored.txt"),
            &buf,
            2 * BLOCK_SIZE as u64,
            &mut write_file_info,
        )?;
        assert_eq!(wrote, BLOCK_SIZE as _);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, BLOCK_SIZE as i64 * 3);
        assert_eq!(stat.st_blocks, 1);

        let inode = fs.find_inode(2)?;
        assert_eq!(inode.direct_blocks[0], 2);
        assert_eq!(inode.direct_blocks[1], 3);
        assert_eq!(inode.direct_blocks[2], 4);

        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 4);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    fn make_fs(name: &str) -> anyhow::Result<PathBuf> {
        let mut tmp_file = std::env::temp_dir();
        tmp_file.push(name);
        tmp_file.set_extension("img");
        if tmp_file.exists() {
            std::fs::remove_file(&tmp_file)?;
        }

        let block_group_size = util::block_group_size(BLOCK_SIZE);
        mkfs::make(&tmp_file, block_group_size as u64, BLOCK_SIZE)?;

        Ok(tmp_file)
    }
}
