use crate::fs;

use fs::gotenks::GotenksFS;
use std::{
    ffi::OsString,
    fs as std_fs, io,
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
    let file = std_fs::File::open(image_path.as_ref())?;
    let reader = io::BufReader::new(&file);
    let mut sb: fs::types::Superblock = bincode::deserialize_from(reader)?;

    if !sb.verify_checksum() {
        return Err(anyhow!("Superblock checksum verification failed"));
    }

    let groups = fs::gotenks::load_bitmaps(&sb, file)?;
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
