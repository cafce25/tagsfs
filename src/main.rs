use std::path::PathBuf;

use anyhow::anyhow;
use clap::Parser;
use tagsfs::TagsFs;

#[derive(Parser)]
/// Commandline option
struct Options {
    #[clap()]
    /// Database with the tags and possibly further option
    database: PathBuf,
    #[clap(short, long)]
    /// where to mount the TagFS to.
    mountpoint: Option<PathBuf>,
    #[clap(short, long, parse(from_occurrences))]
    /// Verbosity of logging (specify multiple times for higher level)
    verbose: usize,
    #[clap(short, long)]
    /// Don't log anything
    quiet: bool,
}

fn main() -> anyhow::Result<()> {
    let opt = Options::parse();
    stderrlog::new()
        .module(module_path!())
        .quiet(opt.quiet)
        .verbosity(opt.verbose)
        .init()
        .unwrap();
    let fs = TagsFs::new(opt.database, Some("tryout/files".into()))?;
    let mountpoint = opt
        .mountpoint
        .ok_or_else(|| anyhow!("no mountpoint specified"))
        .or_else(|_| fs.db.mountpoint())
        ?;
    // fuser::mount2(fs, mountpoint, &[MountOption::AllowRoot, MountOption::AutoUnmount])?;
    fuser::mount2(fs, mountpoint, &[])?;
    Ok(())
}
