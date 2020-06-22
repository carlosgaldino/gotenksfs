pub mod fs;
pub mod types;
pub mod util;

const GOTENKS_MAGIC: u32 = 0x64627a;
const ROOT_INODE: u32 = 1;
const INODE_SIZE: u64 = 128;
pub const SUPERBLOCK_SIZE: u64 = 1024;
pub const DIRECT_POINTERS: u64 = 12;
