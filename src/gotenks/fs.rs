use super::{
    types::{Directory, Group, Inode, Superblock},
    util, DIRECT_POINTERS, INODE_SIZE, ROOT_INODE, SUPERBLOCK_SIZE,
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
        )?;
        self.save_inode(inode, index)?;
        self.save_dir(dir, index)
    }

    #[inline]
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

    #[inline]
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

    fn find_data_block(
        &mut self,
        inode: &mut Inode,
        offset: u64,
        read: bool,
    ) -> fuse_rs::Result<(u32, u32)> {
        let blk_size = self.superblock().block_size as u64;
        let index = offset / blk_size;

        let pointers_per_block = blk_size / mem::size_of::<u32>() as u64;

        let block = if index < DIRECT_POINTERS {
            inode.find_direct_block(index as usize)
        } else if index < (pointers_per_block + DIRECT_POINTERS) {
            self.find_indirect(
                inode.indirect_block,
                index - DIRECT_POINTERS,
                offset,
                pointers_per_block,
            )
            .map_err(|_| Errno::EIO)?
        } else if index
            < (pointers_per_block * pointers_per_block + pointers_per_block + DIRECT_POINTERS)
        {
            self.find_indirect(
                inode.double_indirect_block,
                index - DIRECT_POINTERS,
                offset,
                pointers_per_block,
            )
            .map_err(|_| Errno::EIO)?
        } else {
            return Err(Errno::ENOSPC);
        };

        if block != 0 {
            return Ok((block, ((index + 1) * blk_size - offset) as u32));
        }

        if read {
            return Err(Errno::EINVAL);
        }

        let mut block = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
        if index < DIRECT_POINTERS {
            inode
                .add_block(block, index as usize)
                .map_err(|_| Errno::ENOSPC)?;
        } else if index < (pointers_per_block + DIRECT_POINTERS) {
            if inode.indirect_block == 0 {
                inode.indirect_block = block;
                self.write_data(&vec![0u8; blk_size as usize], 0, block)
                    .map_err(|_| Errno::EIO)?;
                block = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
            }

            self.save_indirect(
                inode.indirect_block,
                block,
                index - DIRECT_POINTERS,
                pointers_per_block,
            )
            .map_err(|_| Errno::EIO)?;
        } else if index
            < (pointers_per_block * pointers_per_block + pointers_per_block + DIRECT_POINTERS)
        {
            if inode.double_indirect_block == 0 {
                inode.double_indirect_block = block;
                self.write_data(&vec![0u8; blk_size as usize], 0, block)
                    .map_err(|_| Errno::EIO)?;
                block = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
            }

            let indirect_offset = (index - DIRECT_POINTERS) / pointers_per_block - 1;
            let indirect_block = match self
                .find_indirect(
                    inode.double_indirect_block,
                    indirect_offset,
                    0,
                    pointers_per_block,
                )
                .map_err(|_| Errno::EIO)?
            {
                0 => {
                    let indirect_block = block;
                    self.save_indirect(
                        inode.double_indirect_block,
                        block,
                        indirect_offset,
                        pointers_per_block,
                    )
                    .map_err(|_| Errno::EIO)?;
                    self.write_data(&vec![0u8; blk_size as usize], 0, block)
                        .map_err(|_| Errno::EIO)?;
                    block = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
                    indirect_block
                }
                indirect_block => indirect_block,
            };

            self.save_indirect(
                indirect_block,
                block,
                (index - DIRECT_POINTERS) & (pointers_per_block - 1),
                pointers_per_block,
            )
            .map_err(|_| Errno::EIO)?;
        } else {
            return Err(Errno::ENOSPC);
        }

        Ok((block, blk_size as u32))
    }

    fn find_indirect(
        &self,
        pointer: u32,
        index: u64,
        offset: u64,
        pointers_per_block: u64,
    ) -> anyhow::Result<u32> {
        if pointer == 0 {
            return Ok(pointer);
        }

        let off = if index < pointers_per_block {
            index & (pointers_per_block - 1)
        } else {
            index / pointers_per_block - 1
        };

        let block = self.read_u32(off, pointer)?;

        if block == 0 || index < pointers_per_block {
            return Ok(block);
        }

        self.find_indirect(
            block,
            index & (pointers_per_block - 1),
            offset,
            pointers_per_block,
        )
    }

    fn save_indirect(
        &mut self,
        pointer: u32,
        block: u32,
        index: u64,
        pointers_per_block: u64,
    ) -> anyhow::Result<()> {
        assert_ne!(pointer, 0);
        let offset = index & (pointers_per_block - 1);

        if index < pointers_per_block {
            self.write_data(&block.to_le_bytes(), offset * 4, pointer)
                .map(|_| ())
        } else {
            let indirect_offset = index / pointers_per_block - 1;
            let new_pointer = self.read_u32(indirect_offset, pointer)?;
            self.save_indirect(new_pointer, block, offset, pointers_per_block)
        }
    }

    // (group_block_index, bitmap_index)
    #[inline]
    fn inode_offsets(&self, index: u32) -> (u64, u64) {
        let inodes_per_group = self.superblock().data_blocks_per_group as u64;
        let inode_bg = (index as u64 - 1) / inodes_per_group;
        let bitmap_index = (index as u64 - 1) & (inodes_per_group - 1);
        (inode_bg, bitmap_index)
    }

    #[inline]
    fn inode_seek_position(&self, index: u32) -> u64 {
        let (group_index, bitmap_index) = self.inode_offsets(index);
        let block_size = self.superblock().block_size;
        group_index * util::block_group_size(block_size)
            + 2 * block_size as u64
            + bitmap_index * INODE_SIZE
            + SUPERBLOCK_SIZE
    }

    #[inline]
    fn data_block_offsets(&self, index: u32) -> (u64, u64) {
        let data_blocks_per_group = self.superblock().data_blocks_per_group as u64;
        let group_index = (index as u64 - 1) / data_blocks_per_group;
        let block_index = (index as u64 - 1) & (data_blocks_per_group - 1);

        (group_index, block_index)
    }

    #[inline]
    fn data_block_seek_position(&self, index: u32) -> u64 {
        let (group_index, block_index) = self.data_block_offsets(index);

        let block_size = self.superblock().block_size;
        group_index * util::block_group_size(block_size)
            + 2 * block_size as u64
            + self.superblock().data_blocks_per_group as u64 * INODE_SIZE
            + SUPERBLOCK_SIZE
            + block_size as u64 * block_index
    }

    fn allocate_inode(&mut self) -> Option<u32> {
        // TODO: handle when group has run out of space
        let group_index = self.groups().iter().position(|g| g.free_inodes() > 0)?;
        self.superblock_mut().free_inodes -= 1;
        let group = self.groups_mut().get_mut(group_index).unwrap();

        let index = group.allocate_inode()?;
        Some(index as u32 + group_index as u32 * self.superblock().data_blocks_per_group)
    }

    fn allocate_data_block(&mut self) -> Option<u32> {
        // TODO: handle when group has run out of space
        let group_index = self
            .groups()
            .iter()
            .position(|g| g.free_data_blocks() > 0)?;

        self.superblock_mut().free_blocks -= 1;
        let group = self.groups_mut().get_mut(group_index).unwrap();

        let index = group.allocate_data_block()?;
        Some(index as u32 + group_index as u32 * self.superblock().data_blocks_per_group)
    }

    #[inline]
    fn release_data_blocks(&mut self, blocks: &[u32]) {
        for block in blocks {
            let (group_index, block_index) = self.data_block_offsets(*block);
            // TODO: release multiple blocks from the same group in a single call
            self.groups_mut()
                .get_mut(group_index as usize)
                .unwrap()
                .release_data_block(1 + block_index as usize);
        }
        self.superblock_mut().free_blocks += blocks.len() as u32;
    }

    #[inline]
    fn release_inode(&mut self, index: u32) {
        let (group_index, _) = self.inode_offsets(index);
        self.groups_mut()
            .get_mut(group_index as usize)
            .unwrap()
            .release_inode(index as usize);
        self.superblock_mut().free_inodes += 1;
    }

    fn release_indirect_block(&mut self, block: u32) -> anyhow::Result<()> {
        let blocks = self.read_indirect_block(block)?;
        self.release_data_blocks(&blocks);
        Ok(())
    }

    fn release_double_indirect_block(&mut self, block: u32) -> anyhow::Result<()> {
        let pointers_per_block = self.superblock().block_size as usize / 4;
        let indirect_blocks = self.read_indirect_block(block)?;
        let mut blocks = Vec::with_capacity(indirect_blocks.len() * pointers_per_block);
        for b in indirect_blocks.iter().filter(|x| **x != 0) {
            blocks.append(&mut self.read_indirect_block(*b)?);
        }

        self.release_data_blocks(&indirect_blocks);
        self.release_data_blocks(&blocks);

        Ok(())
    }

    #[inline]
    fn write_data(&mut self, data: &[u8], offset: u64, block_index: u32) -> anyhow::Result<usize> {
        let block_offset = self.data_block_seek_position(block_index);

        let buf = self.mmap_mut().as_mut();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(block_offset + offset))?;
        Ok(cursor.write(data)?)
    }

    #[inline]
    fn read_data(&self, data: &mut [u8], offset: u64, block_index: u32) -> anyhow::Result<usize> {
        let block_offset = self.data_block_seek_position(block_index);
        let buf = self.mmap().as_ref();
        let mut cursor = Cursor::new(buf);
        cursor.seek(SeekFrom::Start(block_offset + offset))?;

        cursor.read_exact(data)?;

        Ok(data.len())
    }

    #[inline]
    fn read_u32(&self, offset: u64, block_index: u32) -> anyhow::Result<u32> {
        let mut data = [0u8; 4];
        self.read_data(&mut data, offset * 4, block_index)?;
        Ok(u32::from_le_bytes(data))
    }

    fn read_indirect_block(&mut self, block: u32) -> anyhow::Result<Vec<u32>> {
        let pointers_per_block = self.superblock().block_size as usize / 4;
        let mut vec = Vec::with_capacity(pointers_per_block);
        for i in 0..pointers_per_block {
            let b = self.read_u32(i as u64, block)?;
            if b != 0 {
                vec.push(b);
            }
        }

        Ok(vec)
    }

    #[inline]
    fn groups(&self) -> &[Group] {
        self.groups.as_ref().unwrap()
    }

    #[inline]
    fn groups_mut(&mut self) -> &mut [Group] {
        self.groups.as_mut().unwrap()
    }

    #[inline]
    fn superblock(&self) -> &Superblock {
        self.sb.as_ref().unwrap()
    }

    #[inline]
    fn superblock_mut(&mut self) -> &mut Superblock {
        self.sb.as_mut().unwrap()
    }

    #[inline]
    fn mmap(&self) -> &MmapMut {
        self.mmap.as_ref().unwrap()
    }

    #[inline]
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
            let (block_index, space_left) = self.find_data_block(&mut inode, offset, false)?;

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
        Ok(total_wrote)
    }

    fn read(
        &mut self,
        _path: &Path,
        buf: &mut [u8],
        offset: u64,
        file_info: fuse_rs::fs::FileInfo,
    ) -> fuse_rs::Result<usize> {
        let index = file_info.handle().ok_or(Errno::EINVAL)? as u32;
        if index == 0 {
            return Err(Errno::EINVAL);
        }
        let mut inode = self.find_inode(index)?;
        let mut total_read: usize = 0;
        let mut offset = offset;
        let blk_size = self.superblock().block_size;

        let should_read = buf.len().min(inode.size as usize);
        while total_read != should_read as usize {
            let direct_block_index = offset / blk_size as u64;
            let (block_index, space_left) = self.find_data_block(&mut inode, offset, true)?;

            let max_read_len = buf.len().min(space_left as usize);
            let max_read_len = buf.len().min(max_read_len + total_read);
            let offset_in_block = if total_read != 0 {
                0
            } else {
                offset - direct_block_index * blk_size as u64
            };

            let read = self
                .read_data(
                    &mut buf[total_read..max_read_len],
                    offset_in_block,
                    block_index,
                )
                .map_err(|_| Errno::EIO)?;

            total_read += read;
            offset += read as u64;
        }

        inode.update_accessed_at();
        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;

        Ok(total_read)
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

    fn set_permissions(&mut self, path: &Path, mode: Mode) -> fuse_rs::Result<()> {
        let (mut inode, index) = self.find_inode_from_path(path)?;
        inode.mode |= mode.bits();
        self.save_inode(inode, index).map_err(|_| Errno::EIO)
    }

    fn remove_file(&mut self, path: &Path) -> fuse_rs::Result<()> {
        let (mut parent, parent_index) = self.find_dir(path.parent().ok_or(Errno::EINVAL)?)?;
        match parent
            .entries
            .remove(path.file_name().ok_or(Errno::EINVAL)?)
        {
            None => Err(Errno::ENOENT),
            Some(index) => {
                // TODO: handle when links > 1
                let inode = self.find_inode(index)?;
                self.release_data_blocks(&inode.direct_blocks());
                if inode.indirect_block != 0 {
                    self.release_indirect_block(inode.indirect_block)
                        .map_err(|_| Errno::EIO)?;
                }
                if inode.double_indirect_block != 0 {
                    self.release_double_indirect_block(inode.double_indirect_block)
                        .map_err(|_| Errno::EIO)?;
                }
                self.save_dir(parent, parent_index)
                    .map_err(|_| Errno::EIO)?;
                self.release_inode(index);
                Ok(())
            }
        }
    }

    fn create_dir(&mut self, path: &Path, mode: Mode) -> fuse_rs::Result<()> {
        let index = self.allocate_inode().ok_or_else(|| Errno::ENOSPC)?;
        let (mut parent, parent_index) = self.find_dir(path.parent().ok_or(Errno::EINVAL)?)?;
        parent.entries.insert(
            path.file_name()
                .map(|p| p.to_os_string())
                .ok_or(Errno::EINVAL)?,
            index,
        );

        let mut inode = Inode::new();
        inode.mode = SFlag::S_IFDIR.bits() | mode.bits();
        inode.hard_links = 2;
        inode.user_id = self.superblock().uid;
        inode.group_id = self.superblock().gid;

        let data_block_index = self.allocate_data_block().ok_or_else(|| Errno::ENOSPC)?;
        let dir = Directory::default();

        inode
            .add_block(data_block_index, 0)
            .map_err(|_| Errno::EIO)?;

        self.save_inode(inode, index).map_err(|_| Errno::EIO)?;
        self.save_dir(dir, data_block_index)
            .map_err(|_| Errno::EIO)?;
        self.save_dir(parent, parent_index)
            .map_err(|_| Errno::EIO)?;

        Ok(())
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
        fs.superblock_mut().data_blocks_per_group = block_size as u32 * 8;

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
        let fs = GotenksFS::new(&tmp_file)?;
        let dir = fs.find_dir_from_inode(ROOT_INODE)?;

        assert_eq!(dir.entries.len(), 0);

        Ok(std::fs::remove_file(&tmp_file)?)
    }

    #[test]
    fn find_dir() -> anyhow::Result<()> {
        let tmp_file = make_fs("find_dir")?;
        let fs = GotenksFS::new(&tmp_file)?;

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
        let handle = open_fi.handle().unwrap();

        fs.open(Path::new("/bar.txt"), &mut open_fi)?;
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(handle);

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3).take(125).collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, 125);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 125);
        assert_eq!(stat.st_blocks, 1);

        assert_eq!(read(&mut fs, 125, 0, handle)?, buf);

        // Overwriting with larger buffer
        let buf = std::iter::repeat(4).take(126).collect::<Vec<u8>>();
        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, 126);

        let stat = fs.metadata(Path::new("/bar.txt"))?;
        assert_eq!(stat.st_size, 126);
        assert_eq!(stat.st_blocks, 1); // 126 / 512 + 1

        assert_eq!(read(&mut fs, 126, 0, handle)?, buf);

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

        assert_eq!(read(&mut fs, 120, 0, handle)?, buf);
        assert_eq!(
            read(&mut fs, 6, 120, handle)?,
            std::iter::repeat(4).take(6).collect::<Vec<u8>>()
        );

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

        assert_eq!(
            read(&mut fs, 120, 0, handle)?,
            std::iter::repeat(5).take(120).collect::<Vec<u8>>()
        );
        assert_eq!(
            read(&mut fs, 6, 120, handle)?,
            std::iter::repeat(4).take(6).collect::<Vec<u8>>()
        );
        assert_eq!(read(&mut fs, 125, 126, handle)?, buf);

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

        assert_eq!(
            read(&mut fs, 120, 0, handle)?,
            std::iter::repeat(5).take(120).collect::<Vec<u8>>()
        );
        assert_eq!(
            read(&mut fs, 6, 120, handle)?,
            std::iter::repeat(4).take(6).collect::<Vec<u8>>()
        );
        assert_eq!(
            read(&mut fs, 125, 126, handle)?,
            std::iter::repeat(7).take(125).collect::<Vec<u8>>()
        );
        assert_eq!(read(&mut fs, 125, 251, handle)?, buf);

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

        assert_eq!(
            read(&mut fs, 120, 0, handle)?,
            std::iter::repeat(5).take(120).collect::<Vec<u8>>()
        );
        assert_eq!(
            read(&mut fs, 6, 120, handle)?,
            std::iter::repeat(4).take(6).collect::<Vec<u8>>()
        );
        assert_eq!(read(&mut fs, 125, 126, handle)?, buf);
        assert_eq!(
            read(&mut fs, 125, 251, handle)?,
            std::iter::repeat(8).take(125).collect::<Vec<u8>>()
        );

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
        let handle = open_fi.handle().unwrap();
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(handle);

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3)
            .take(2 * BLOCK_SIZE as usize)
            .collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, buf.len());
        assert_eq!(read(&mut fs, 2 * BLOCK_SIZE as usize, 0, handle)?, buf);

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
        assert_eq!(
            read(&mut fs, BLOCK_SIZE as usize, 2 * BLOCK_SIZE as u64, handle)?,
            buf
        );

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

    #[test]
    fn remove_file() -> anyhow::Result<()> {
        let tmp_file = make_fs("remove_file")?;
        let mut fs = GotenksFS::new(&tmp_file)?;

        let mut open_fi = fuse_rs::fs::OpenFileInfo::default();
        fs.create(
            Path::new("/bar.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut open_fi,
        )?;

        fs.open(Path::new("/bar.txt"), &mut open_fi)?;
        let handle = open_fi.handle().unwrap();
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(handle);

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3)
            .take(2 * BLOCK_SIZE as usize)
            .collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, buf.len());
        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 3);

        let (inode, index) = fs.find_inode_from_path(Path::new("/bar.txt"))?;
        let blocks = vec![2u32, 3u32];
        assert_eq!(blocks, inode.direct_blocks());
        assert_eq!(index, 2);

        fs.remove_file(Path::new("/bar.txt"))?;

        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 1);
        assert_eq!(
            Errno::ENOENT,
            fs.metadata(Path::new("/bar.txt")).unwrap_err()
        );

        let entries = fs.read_dir(Path::new("/"), 0, fuse_rs::fs::FileInfo::default())?;
        assert_eq!(entries.len(), 0);

        let mut open_fi = fuse_rs::fs::OpenFileInfo::default();
        fs.create(
            Path::new("/baz.txt"),
            nix::sys::stat::Mode::S_IRWXU,
            &mut open_fi,
        )?;

        fs.open(Path::new("/baz.txt"), &mut open_fi)?;
        let handle = open_fi.handle().unwrap();
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(handle);

        let mut write_file_info = fuse_rs::fs::WriteFileInfo::from_file_info(file_info);
        let buf = std::iter::repeat(3)
            .take(2 * BLOCK_SIZE as usize)
            .collect::<Vec<u8>>();

        let wrote = fs.write(Path::new("/ignored.txt"), &buf, 0, &mut write_file_info)?;
        assert_eq!(wrote, buf.len());
        assert_eq!(fs.superblock().free_blocks, BLOCK_SIZE * 8 - 3);

        // Check that it reuses previously freed blocks
        let (inode, index) = fs.find_inode_from_path(Path::new("/baz.txt"))?;
        let blocks = vec![2u32, 3u32];
        assert_eq!(blocks, inode.direct_blocks());
        assert_eq!(index, 2);

        let entries = fs.read_dir(Path::new("/"), 0, fuse_rs::fs::FileInfo::default())?;
        assert_eq!(entries.len(), 1);

        let bar = entries.first().unwrap();
        assert_eq!(bar.name, OsString::from("baz.txt"));

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
        mkfs::make(&tmp_file, block_group_size, BLOCK_SIZE)?;

        Ok(tmp_file)
    }

    fn read(
        fs: &mut dyn fuse_rs::Filesystem,
        len: usize,
        offset: u64,
        handle: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(len);
        unsafe { buf.set_len(len) };
        let mut file_info = fuse_rs::fs::FileInfo::default();
        file_info.set_handle(handle);

        fs.read(Path::new("/ignored.txt"), &mut buf, offset, file_info)?;

        Ok(buf)
    }
}
