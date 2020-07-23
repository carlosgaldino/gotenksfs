# GotenksFS

GotenksFS is a file system built for learning purposes. The disk is represented
as a regular file which is created using the `mkfs` command provided by this
binary. A `mount` command is also available for mounting the file system using
FUSE.

For a more in-depth overview of this project, read the blog post: [Writing a
file system from scratch in
Rust](https://blog.carlosgaldino.com/writing-a-file-system-from-scratch-in-rust.html).

## Layout

The first 1024 bytes of the "disk" hold the superblock with the metadata about
the file system. When `mkfs` is executed, the "disk" is divided into fixed-size
blocks. With the exception of the superblock, all data is written in these
blocks. The size can be configured to be either 1 KiB, 2 KiB, or 4 KiB.

Blocks are grouped in _block groups_. The first two blocks in each block group
are used for the data and inode bitmaps. Following that, there is the
appropriate number of blocks for storing the inode table. As an example, inodes
have a size of 128 bytes which means that for a block size of 4 KiB there will
32768 inodes in the bitmap which will require 1024 blocks for the inode table.
After the inode table, the remaining blocks are used for user data. In this
example, 32768 blocks taking 128 MiB to be exact.

Each inode has 12 direct pointers. The system supports larger files by using
double indirect pointers. Considering blocks of 4 KiB, this means the maximum
size a file can have is 4 GiB. The file system could theoretically be up to 16
TiB in size.

<figure>
    <img src="https://blog.carlosgaldino.com/public/images/gotenksfs_block_group.svg" alt="" style="max-width: 100%;">
</figure>

## Example

First we need to create the disk image:

```bash
$ ./gotenksfs mkfs disk.img -s "10 GiB" -b 4096
```

Then mount it:

```bash
$ ./gotenksfs mount disk.img gotenks
```

The following image shows the file system in action.

<figure>
    <img src="https://blog.carlosgaldino.com/public/images/cp.gif" alt="" style="max-width: 100%;">
</figure>

## Usage

```
gotenksfs
A file system on top of your file system

USAGE:
    gotenksfs [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help     Prints this message or the help of the given subcommand(s)
    mkfs     Create a new file system
    mount    Mount a file system
```
