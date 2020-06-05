pub mod fs;
pub mod types;
pub mod util;

const GOTENKS_MAGIC: u32 = 0x64627a;
const ROOT_INODE: u32 = 1;
pub const SUPERBLOCK_SIZE: u64 = 1024;
