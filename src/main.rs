use byte_unit::Byte;

mod gotenks;
mod mkfs;
mod mount;

fn main() -> anyhow::Result<()> {
    let matches = clap::App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            clap::App::new("mkfs")
                .about("Create a new file system")
                .arg("<file> 'Location of the new file system image'")
                .arg(
                    clap::Arg::with_name("block-size")
                        .short('b')
                        .long("block-size")
                        .takes_value(true)
                        .about("Specify the block size in bytes.")
                        .possible_values(&["1024", "2048", "4096"])
                        .default_value("4096"),
                )
                .arg(
                    clap::Arg::with_name("size")
                        .short('s')
                        .long("size")
                        .takes_value(true)
                        .about("Specify the total size of the file system. The final size might be bigger than the provided value in order to have space for the file system structures.").required(true),
                ),
        ).subcommand(
            clap::App::new("mount")
                .about("Mount a file system")
                .arg("<image> 'Location of the file system image'")
                .arg("<mountpoint> 'Mountpoint'")
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("mkfs") {
        let blk_size = matches
            .value_of("block-size")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let file_name = matches.value_of("file").unwrap();
        let file_size = matches.value_of("size").unwrap();

        let file_size = match Byte::from_str(file_size) {
            Ok(size) => size.get_bytes(),
            Err(err) => return Err(err.into()),
        };

        mkfs::make(file_name, file_size, blk_size)?;
    }

    if let Some(matches) = matches.subcommand_matches("mount") {
        let image = matches.value_of("image").unwrap();
        let mountpoint = matches.value_of("mountpoint").unwrap();

        mount::mount(image, mountpoint)?;
    }

    Ok(())
}
