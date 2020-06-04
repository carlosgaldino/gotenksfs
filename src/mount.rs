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
    unsafe {
        FS = GotenksFS {
            sb: Some(sb),
            image: Some(PathBuf::from(image_path.as_ref())),
            groups: Some(groups),
        };
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

#[derive(Debug, Default)]
struct GotenksFS {
    sb: Option<fs::Superblock>,
    image: Option<PathBuf>,
    groups: Option<Vec<Group>>,
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
                (fs::block_group_size(sb.block_size) - 2 * sb.block_size as u64) as _, // minus the bitmaps
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

impl fuse_rs::Filesystem for GotenksFS {
    fn metadata(&self, path: &Path) -> fuse_rs::Result<fuse_rs::fs::FileStat> {
        let mut stat = fuse_rs::fs::FileStat::new();
        match path.to_str().expect("path") {
            "/" => {
                let sb = self.sb.as_ref().unwrap();
                let now = fs::now();
                stat.st_mode = SFlag::S_IFDIR.bits() | 0o777;
                stat.st_nlink = 2;
                stat.st_ino = 1;
                stat.st_atime = now as _;
                stat.st_mtime = sb.modified_at.unwrap() as _;
                stat.st_birthtime = sb.created_at as _;
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
        let writer = match std_fs::OpenOptions::new()
            .write(true)
            .open(self.image.as_ref().unwrap())
        {
            Ok(f) => f,
            Err(err) => {
                return Err(err
                    .raw_os_error()
                    .map_or_else(|| Errno::EINVAL, |e| Errno::from_i32(e)))
            }
        };
        self.sb.as_mut().unwrap().checksum();
        match bincode::serialize_into(writer, self.sb.as_ref().unwrap()) {
            Ok(_) => Ok(()),
            Err(_) => Err(Errno::EIO),
        }
    }
}
