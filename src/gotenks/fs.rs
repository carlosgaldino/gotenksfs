use super::{
    types::{Group, Inode, Superblock},
    util, INODE_SIZE, ROOT_INODE, SUPERBLOCK_SIZE,
};
use nix::{errno::Errno, sys::stat::SFlag};
use std::{
    fs,
    io::{self, prelude::*},
    path::{Path, PathBuf},
};

#[derive(Debug, Default)]
pub struct GotenksFS {
    pub sb: Option<Superblock>,
    pub image: Option<PathBuf>,
    pub groups: Option<Vec<Group>>,
}

impl GotenksFS {
    pub fn create_root(&mut self) -> anyhow::Result<()> {
        let group = self.groups.as_mut().unwrap().get_mut(0).unwrap();
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
        let file = fs::OpenOptions::new()
            .write(true)
            .open(self.image.as_ref().unwrap())?;
        let mut writer = io::BufWriter::new(file);
        writer.seek(io::SeekFrom::Start(self.inode_seek_position(index)))?;
        inode.serialize_into(&mut writer)?;

        Ok(writer.flush()?)
    }

    fn find_inode(&self, index: u32) -> fuse_rs::Result<Inode> {
        let (group_index, bitmap_index) = self.inode_offsets(index);
        if !self
            .groups
            .as_ref()
            .unwrap()
            .get(group_index as usize)
            .unwrap()
            .has_inode(bitmap_index as usize)
        {
            return Err(Errno::ENOENT);
        }

        let file = fs::File::open(self.image.as_ref().unwrap()).map_err(|_e| Errno::EIO)?;
        let mut reader = io::BufReader::new(file);
        reader
            .seek(io::SeekFrom::Start(self.inode_seek_position(index)))
            .map_err(|_e| Errno::EIO)?;

        let inode = Inode::deserialize_from(reader).map_err(|_e| Errno::EIO)?;
        Ok(inode)
    }

    // (block_group_index, bitmap_index)
    fn inode_offsets(&self, index: u32) -> (u32, u32) {
        let inodes_per_group = self.sb.as_ref().unwrap().data_blocks_per_group;
        let inode_bg = (index - 1) / inodes_per_group;
        (inode_bg, index - 1 & (inodes_per_group - 1))
    }

    fn inode_seek_position(&self, index: u32) -> u64 {
        let (group_index, bitmap_index) = self.inode_offsets(index);
        let block_size = self.sb.as_ref().unwrap().block_size;
        let seek_pos = group_index * util::block_group_size(block_size)
            + 2 * block_size
            + bitmap_index * INODE_SIZE as u32
            + SUPERBLOCK_SIZE as u32;

        seek_pos as u64
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
            let sb = self.sb.as_ref().unwrap();
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
        if let Some(sb) = self.sb.as_mut() {
            sb.update_last_mounted_at();
            sb.update_modified_at();
        };

        Ok(())
    }

    fn destroy(&mut self) -> fuse_rs::Result<()> {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(self.image.as_ref().unwrap())
            .or_else(|e| {
                e.raw_os_error()
                    .map_or_else(|| Err(Errno::EINVAL), |e| Err(Errno::from_i32(e)))
            })?;
        let mut writer = io::BufWriter::new(file);

        self.sb
            .as_mut()
            .unwrap()
            .serialize_into(&mut writer)
            .or_else(|_| Err(Errno::EIO))?;

        Group::serialize_into(&mut writer, self.groups.as_ref().unwrap())
            .or_else(|_| Err(Errno::EIO))
    }
}
