use crate::gotenks::fs::GotenksFS;
use anyhow::anyhow;
use std::{ffi::OsString, path::Path};

static mut FS: GotenksFS = GotenksFS {
    sb: None,
    mmap: None,
    groups: None,
};

pub fn mount<P>(image_path: P, mountpoint: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    unsafe {
        FS = GotenksFS::new(image_path)?;
    }

    let opts = vec![
        // OsString::from("-h"),
        // OsString::from("-s"),
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
