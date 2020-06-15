use super::{
    types::{Directory, Group, Inode, Superblock},
    util, INODE_SIZE, ROOT_INODE, SUPERBLOCK_SIZE,
};
use fs::OpenOptions;
use fuse_rs::fs::FileStat;
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

        let mut inode = Inode::new();
        inode.mode = SFlag::S_IFDIR.bits() | 0o777;
        inode.hard_links = 2;

        let dir = Directory::default();

        group.allocate_inode();
        inode.direct_blocks[0] = group.allocate_data_block() as u32;
        self.superblock_mut().free_inodes -= 1;
        self.superblock_mut().free_blocks -= 1;
        self.save_inode(inode, ROOT_INODE)?;
        self.save_dir(dir, ROOT_INODE)
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

    fn allocate_inode(&mut self) -> u32 {
        let mut groups = self
            .groups()
            .iter()
            .enumerate()
            .map(|(index, g)| (index, g.free_inodes()))
            .collect::<Vec<(usize, usize)>>();

        // TODO: handle when group has run out of space
        groups.sort_by(|a, b| a.1.cmp(&b.1));
        let (group_index, _) = groups.first().unwrap();
        let group = self.groups_mut().get_mut(*group_index).unwrap();

        let index = group.allocate_inode();
        index as u32 + *group_index as u32 * self.superblock().data_blocks_per_group
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
        match path.parent() {
            None => Ok(self.find_inode(ROOT_INODE)?.to_stat(ROOT_INODE)),
            Some(parent) => {
                let (parent, _) = self.find_dir(parent)?;
                let index = parent.entry(path.file_name().ok_or(Errno::EINVAL)?.to_os_string())?;
                Ok(self.find_inode(index)?.to_stat(index))
            }
        }
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
        permissions: nix::sys::stat::Mode,
        file_info: &mut fuse_rs::fs::OpenFileInfo,
    ) -> fuse_rs::Result<()> {
        let index = self.allocate_inode();
        let mut inode = Inode::new();
        inode.mode = permissions.bits();

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
        fs.sb = Some(Superblock::new(block_size, 3));
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

        assert_eq!(inode.accessed_at, None);

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

        let file_info = fuse_rs::fs::FileInfo::default();
        let entries = fs.read_dir(Path::new("/"), 0, file_info)?;
        assert_eq!(entries.len(), 2);

        let bar = entries.first().unwrap();
        let mut stat = FileStat::default();
        stat.st_mode = nix::sys::stat::Mode::S_IRWXU.bits();
        stat.st_ino = 3;
        assert_eq!(bar.name, OsString::from("bar.txt"));
        assert_eq!(bar.metadata, Some(stat));

        let foo = entries.last().unwrap();
        let mut stat = FileStat::default();
        stat.st_mode = nix::sys::stat::Mode::S_IRWXO.bits();
        stat.st_ino = 2;
        assert_eq!(foo.name, OsString::from("foo.txt"));
        assert_eq!(foo.metadata, Some(stat));

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
