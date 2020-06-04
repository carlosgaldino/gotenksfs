use crate::fs;
use bitvec::prelude::*;
use nix::{errno::Errno, sys::stat::SFlag};
use std::{
    ffi::OsString,
    fs as std_fs,
    io::{self, prelude::*, SeekFrom},
    path::{Path, PathBuf},
};
use std_fs::File;

static mut FS: GotenksFS = GotenksFS {
    sb: None,
    image: None,
    groups: None,
};

pub(crate) fn mount<P>(image_path: P, mountpoint: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let file = std_fs::File::open(image_path.as_ref())?;
    let reader = io::BufReader::new(&file);
    let mut sb: fs::Superblock = bincode::deserialize_from(reader)?;

    if !sb.verify_checksum() {
        return Err(anyhow!("Superblock checksum verification failed"));
    }

    let groups = load_bitmaps(&sb, file)?;
    let mut fs = GotenksFS {
        sb: Some(sb),
        image: Some(PathBuf::from(image_path.as_ref())),
        groups: Some(groups),
    };

    fs.create_root()?;

    unsafe {
        FS = fs;
    }

    let opts = vec![
        // OsString::from("-h"),
        OsString::from("-s"),
        OsString::from("-f"),
        // OsString::from("-d"),
        OsString::from("-o"),
        OsString::from("volname=gotenksfs"),
    ];

    match fuse_rs::mount(
        OsString::from("GotenksFS"),
        mountpoint,
        unsafe { &mut FS },
        opts,
    ) {
        Ok(_) => Ok(()),
        Err(err) => Err(anyhow!(format!("{:?}", err))),
    }
}

#[derive(Debug)]
struct Group {
    data_bitmap: BitVec<Lsb0, u8>,
    inode_bitmap: BitVec<Lsb0, u8>,
}

impl Group {
    fn has_inode(&self, i: usize) -> bool {
        let b = self.inode_bitmap.get(i - 1).unwrap_or(&false);
        b == &true
    }

    fn add_inode(&mut self, i: usize) {
        self.inode_bitmap.set(i - 1, true);
    }
}

#[derive(Debug, Default)]
struct GotenksFS {
    sb: Option<fs::Superblock>,
    image: Option<PathBuf>,
    groups: Option<Vec<Group>>,
}

impl GotenksFS {
    fn create_root(&mut self) -> anyhow::Result<()> {
        let group = self.groups.as_mut().unwrap().get_mut(0).unwrap();
        if group.has_inode(fs::ROOT_INODE as _) {
            return Ok(());
        }

        let mut inode = fs::Inode::default();
        inode.mode = SFlag::S_IFDIR.bits() | 0o777;
        inode.hard_links = 2;
        inode.created_at = fs::now();

        group.add_inode(fs::ROOT_INODE as usize);
        self.save_inode(inode, fs::ROOT_INODE)
    }

    fn save_inode(&mut self, inode: fs::Inode, index: u32) -> anyhow::Result<()> {
        let file = std_fs::OpenOptions::new()
            .write(true)
            .open(self.image.as_ref().unwrap())?;
        let mut writer = io::BufWriter::new(file);
        writer.seek(SeekFrom::Start(self.inode_seek_position(index)))?;

        writer.write_all(&inode.serialize()?)?;

        Ok(writer.flush()?)
    }

    fn find_inode(&self, index: u32) -> fuse_rs::Result<fs::Inode> {
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

        let file = std_fs::File::open(self.image.as_ref().unwrap()).map_err(|_e| Errno::EIO)?;
        let mut reader = io::BufReader::new(file);
        reader
            .seek(SeekFrom::Start(self.inode_seek_position(index)))
            .map_err(|_e| Errno::EIO)?;

        let inode: fs::Inode = bincode::deserialize_from(reader).map_err(|_e| Errno::EIO)?;
        Ok(inode)
    }

    // (block_group_index, bitmap_index)
    fn inode_offsets(&self, index: u32) -> (u32, u32) {
        let inodes_per_group = self.sb.as_ref().unwrap().data_blocks_per_group;
        let inode_bg = (index - 1) / inodes_per_group;
        (inode_bg, index & (inodes_per_group - 1))
    }

    fn inode_seek_position(&self, index: u32) -> u64 {
        let (group_index, bitmap_index) = self.inode_offsets(index);
        let block_size = self.sb.as_ref().unwrap().block_size;
        let seek_pos = group_index * fs::block_group_size(block_size)
            + block_size
            + bitmap_index * fs::inode_size()
            + fs::SUPERBLOCK_SIZE as u32;

        seek_pos as u64
    }
}

fn load_bitmaps(sb: &fs::Superblock, f: File) -> anyhow::Result<Vec<Group>> {
    let mut groups = Vec::with_capacity(sb.groups as _);
    let mut reader = io::BufReader::new(f);
    reader.seek(SeekFrom::Start(fs::SUPERBLOCK_SIZE))?;
    let mut buf = Vec::with_capacity(sb.block_size as usize);
    unsafe {
        buf.set_len(sb.block_size as _);
    }
    for i in 0..sb.groups {
        if i > 0 {
            reader.seek(SeekFrom::Current(
                (fs::block_group_size(sb.block_size) - 2 * sb.block_size) as _, // minus the bitmaps
            ))?;
        }

        reader.read_exact(&mut buf)?;
        let data_bitmap = BitVec::<Lsb0, u8>::from_slice(&mut buf);
        reader.read_exact(&mut buf)?;
        let inode_bitmap = BitVec::<Lsb0, u8>::from_slice(&mut buf);
        groups.push(Group {
            data_bitmap,
            inode_bitmap,
        });
    }

    Ok(groups)
}

fn save_bitmaps<W>(groups: &[Group], blk_size: u32, w: &mut W) -> anyhow::Result<()>
where
    W: io::Write + io::Seek,
{
    for (i, g) in groups.iter().enumerate() {
        let offset = fs::block_group_size(blk_size) as u64 * i as u64 + fs::SUPERBLOCK_SIZE;
        w.seek(SeekFrom::Start(offset))?;
        w.write_all(g.data_bitmap.as_slice())?;
        w.write_all(g.inode_bitmap.as_slice())?;
    }

    Ok(w.flush()?)
}

impl fuse_rs::Filesystem for GotenksFS {
    fn metadata(&self, path: &Path) -> fuse_rs::Result<fuse_rs::fs::FileStat> {
        let mut stat = fuse_rs::fs::FileStat::new();
        match path.to_str().expect("path") {
            "/" => {
                let inode = self.find_inode(fs::ROOT_INODE)?;
                stat.st_ino = fs::ROOT_INODE as _;
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
            Ok(libc::statvfs {
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
            })
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
        let file = std_fs::OpenOptions::new()
            .write(true)
            .open(self.image.as_ref().unwrap())
            .or_else(|e| {
                e.raw_os_error()
                    .map_or_else(|| Err(Errno::EINVAL), |e| Err(Errno::from_i32(e)))
            })?;
        let mut writer = io::BufWriter::new(file);
        self.sb.as_mut().unwrap().checksum();
        save_bitmaps(
            self.groups.as_ref().unwrap(),
            self.sb.as_ref().unwrap().block_size,
            &mut writer,
        )
        .or_else(|_| Err(Errno::EIO))?;
        bincode::serialize_into(writer, self.sb.as_ref().unwrap()).or_else(|_| Err(Errno::EIO))
    }
}
