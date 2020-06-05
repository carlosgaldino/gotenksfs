use crate::gotenks::{
    fs::GotenksFS,
    types::{Group, Superblock},
};
use anyhow::anyhow;
use io::BufReader;
use std::{
    ffi::OsString,
    fs::File,
    io,
    path::{Path, PathBuf},
};

static mut FS: GotenksFS = GotenksFS {
    sb: None,
    image: None,
    groups: None,
};

pub fn mount<P>(image_path: P, mountpoint: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let file = File::open(image_path.as_ref())?;
    let sb: Superblock = Superblock::deserialize_from(BufReader::new(&file))?;

    let groups = Group::deserialize_from(BufReader::new(&file), sb.block_size, sb.groups as usize)?;
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
