#![allow(unused_imports, unused_variables, dead_code)]
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashSet},
    ffi::{CStr, CString, OsStr, OsString},
    fs::{self, File, FileType},
    hash::Hash,
    io::{Read, Seek, SeekFrom, Write},
    mem,
    os::unix::prelude::{MetadataExt, OsStrExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use clap::Parser;
use fuser::{FileAttr, MountOption, ReplyEntry, Request, TimeOrNow};
use itertools::Itertools as _;
use libc::{c_int, EINVAL, ENODATA, ENODEV, ENOENT, ENOSYS, EPERM};
use log::{debug, info, trace, warn};
use rand::thread_rng;

use crate::error::{Error, Result};
use crate::TagsFsDb;

pub struct TagsFs {
    pub db: TagsFsDb,
    pub source: PathBuf,
}

impl TagsFs {
    pub fn new<P: AsRef<Path>>(database: P, source: Option<PathBuf>) -> Result<Self> {
        let db = TagsFsDb::new(database)?;
        let source = match source {
            Some(source) => source,
            None => db.source()?,
        };
        Ok(Self { db, source })
    }

    fn find_file<S: AsRef<Path>>(&self, name: S) -> Result<PathBuf> {
        Ok(self.source.join(name).canonicalize()?)
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr) -> Result<FileAttr> {
        let tags = match self.db.entry(parent) {
            Ok(Entry::Tags(tags)) => tags,
            Ok(Entry::File(_)) | Err(_) => {
                return Err(Error::StdC(EINVAL));
            }
        };
        // is it a file?
        if let Ok(path) = self.source.join(name).canonicalize() {
            let ino = self.db.inode(&Entry::from(path.as_ref()))?;
            let file_tags = self.db.file_tags(name.to_string_lossy())?;
            return if tags.is_subset(&file_tags) {
                Ok(file_attr_of_file(ino, path))
            } else {
                Err(Error::StdC(ENOENT))
            };
        }
        // is it a tag?
        for row in self.db.sub_tags(&tags)? {
            if row == name.to_string_lossy() {
                let mut tags = tags.clone();
                tags.insert(row);
                let ino = self.db.inode_or_create(&Entry::Tags(tags))?;
                return Ok(file_attr_of_file(ino, &self.source));
            }
        }
        Err(Error::StdC(ENOENT))
    }
}

impl fuser::Filesystem for TagsFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), c_int> {
        trace!("init");
        let root_entry = Entry::Tags(BTreeSet::new());
        // TODO properly create db if root isn't in it
        let root_ino = self.db.inode(&root_entry).unwrap();
        assert_eq!(root_ino, fuser::FUSE_ROOT_ID);
        Ok(())
    }

    fn destroy(&mut self) {
        trace!("destroy");
    }

    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup {parent} {name:?}");
        match self.lookup(req, parent, name) {
            Ok(attr) => reply.entry(&Duration::from_secs(0), &attr, 0),
            Err(Error::StdC(errno)) => reply.error(errno),
            Err(_) => reply.error(ENODEV),
        }
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {
        trace!("forget");
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        trace!("getattr(_req, {ino}, reply)");
        match self.db.entry(ino) {
            Ok(Entry::File(name)) => {
                if let Ok(path) = self.find_file(name) {
                    reply.attr(&Duration::from_secs(0), &file_attr_of_file(ino, path));
                } else {
                    reply.error(ENOENT);
                }
            }
            Ok(Entry::Tags(_)) => {
                reply.attr(
                    &Duration::from_secs(0),
                    &file_attr_of_file(ino, &self.source),
                );
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        trace!("setattr");
        // currently only allow setting attributes of files since all tags show the attributes of
        // the source directory
        let path = if let Ok(Entry::File(name)) = self.db.entry(ino) {
            if let Ok(path) = self.find_file(name) {
                path
            } else {
                reply.error(EINVAL);
                return;
            }
        } else {
            reply.error(EINVAL);
            return;
        };
        eprintln!("{path:?}");
        let c_path = unsafe {
            CString::from_vec_unchecked(AsRef::<OsStr>::as_ref(&path).as_bytes().to_vec())
        };
        let mut attr = file_attr_of_file(ino, &path);

        if let Some(mode) = mode {
            let perm = PermissionsExt::from_mode(mode);
            fs::set_permissions(path, perm).unwrap();
        }

        let uid = uid.unwrap_or(attr.uid);
        let gid = gid.unwrap_or(attr.gid);
        if uid != attr.uid || gid != attr.gid {
            let err = unsafe { libc::chown(c_path.as_ptr(), uid, gid) };
            if err != 0 {
                reply.error(err);
                return;
            }
            attr.gid = gid;
            attr.uid = uid;
        }

        if let Some(size) = size {
            if size != attr.size {
                let err = unsafe { libc::truncate(c_path.as_ptr(), size as i64) };
                if err != 0 {
                    reply.error(err);
                    return;
                }
                attr.size = size;
            }
        }
        let atime = match atime {
            Some(TimeOrNow::SpecificTime(atime)) => atime,
            Some(TimeOrNow::Now) => SystemTime::now(),
            None => attr.atime,
        };
        let mtime = match mtime {
            Some(TimeOrNow::SpecificTime(mtime)) => mtime,
            Some(TimeOrNow::Now) => SystemTime::now(),
            None => attr.mtime,
        };
        if atime != attr.atime || mtime != attr.mtime {
            let atime = atime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
            let mtime = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap();
            let times = [
                libc::timespec {
                    tv_sec: atime.as_secs() as i64,
                    tv_nsec: atime.subsec_nanos() as i64,
                },
                libc::timespec {
                    tv_sec: mtime.as_secs() as i64,
                    tv_nsec: mtime.subsec_nanos() as i64,
                },
            ];

            let err = unsafe { libc::utimensat(0, c_path.as_ptr(), times.as_ptr(), 0) };
            if err != 0 {
                let error = unsafe { CStr::from_ptr(libc::strerror(*libc::__errno_location())) };
                debug!("error in utimensat: {error:?}");
                reply.error(err);
                return;
            }
        }
        // ctime: Option<SystemTime>, can't change it other than setting to now
        // crtime: Option<SystemTime>, macos only we don't care
        // chgtime: Option<SystemTime>, ?!?
        // bkuptime: Option<SystemTime>, ?!?
        // flags: Option<u32>,
        reply.attr(&Duration::from_secs(0), &attr);
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyData) {
        debug!("[Not Implemented] readlink(ino: {:#x?})", ino);
        reply.error(ENOSYS);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] mknod(parent: {:#x?}, name: {:?}, mode: {:o}, \
            umask: {:#x?}, rdev: {})",
            parent, name, mode, umask, rdev
        );
        reply.error(ENOSYS);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        trace!(
            "mkdir(parent: {:#x?}, name: {:?}, mode: {:o}, umask: {:#x?})",
            parent,
            name,
            mode,
            umask
        );
        let ino = self.db.create_tag(name.to_string_lossy()).unwrap();
        // TODO return actual inode of new tagset
        reply.entry(
            &Duration::from_secs(0),
            &file_attr_of_file(ino, &self.source),
            0,
        );
    }

    /// Delete all tags of `parent` from the file `name`
    /// Note: since we don't differentiate the order of tags there is no "last" tag we could remove
    /// here
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        trace!("unlink(parent: {:#x?}, name: {:?})", parent, name,);
        let tags = match self.db.entry(parent) {
            Ok(Entry::Tags(tags)) => tags,
            _ => {
                reply.error(EINVAL);
                return;
            }
        };
        if tags.is_empty() {
            fs::remove_file(self.find_file(name).unwrap()).unwrap();
        } else {
            self.db
                .remove_tags_from_file(&tags, name.to_string_lossy())
                .unwrap();
        }
        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        trace!("rmdir(parent: {:#x?}, name: {:?})", parent, name);
        self.db
            .delete_tags(&BTreeSet::from([name.to_string_lossy().to_string()]))
            .unwrap();
        reply.ok();
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] symlink(parent: {:#x?}, name: {:?}, link: {:?})",
            parent, name, link,
        );
        reply.error(EPERM);
    }

    /// change set of tags, it will ignore the newname since that would require a change on the
    /// underlying file system
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        trace!(
            "rename(parent: {:#x?}, name: {:?}, newparent: {:#x?}, \
            newname: {:?}, flags: {})",
            parent,
            name,
            newparent,
            newname,
            flags,
        );
        let tags = match self.db.entry(parent) {
            Ok(Entry::Tags(tags)) => tags,
            _ => {
                reply.error(EINVAL);
                return;
            }
        };
        let newtags = match self.db.entry(newparent) {
            Ok(Entry::Tags(tags)) => tags,
            _ => {
                reply.error(EINVAL);
                return;
            }
        };

        self.db
            .remove_tags_from_file(
                tags.iter().filter(|t| !newtags.contains(*t)),
                name.to_string_lossy(),
            )
            .unwrap();

        self.db
            .add_tags_to_file(
                newtags.iter().filter(|t| !tags.contains(*t)),
                name.to_string_lossy(),
            )
            .unwrap();
        reply.ok();
    }

    /// all links have the same file name since they share the name of the backing file so we
    /// ignore newname
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        trace!(
            "link(ino: {:#x?}, newparent: {:#x?}, newname: {:?})",
            ino,
            newparent,
            newname
        );
        let name = match self.db.entry(ino) {
            Ok(Entry::File(name)) => name,
            _ => {
                reply.error(EINVAL);
                return;
            }
        };
        let tags = match self.db.entry(newparent) {
            Ok(Entry::Tags(tags)) => tags,
            _ => {
                reply.error(EINVAL);
                return;
            }
        };
        self.db
            .add_tags_to_file(tags, name.to_string_lossy())
            .unwrap();
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        trace!("open(req, {_ino}, {_flags}, reply)");
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        trace!("read {ino}");
        match self.db.entry(ino) {
            Ok(Entry::File(name)) => {
                let path = self.find_file(name).unwrap();
                let mut data = vec![0; size as usize];
                let mut file = fs::File::open(path).unwrap();
                file.seek(SeekFrom::Start(offset as u64)).unwrap();
                let read = file.read(&mut data).unwrap();
                reply.data(&data[..read])
            }
            Ok(_) => reply.error(ENODATA),
            Err(_) => reply.error(ENOENT),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        trace!(
            "write(ino: {ino:#x?}, fh: {fh}, offset: {offset}, data.len(): {}, \
            write_flags: {write_flags:#x?}, flags: {flags:#x?}, lock_owner: {lock_owner:?})",
            data.len(),
        );
        let path = match self.db.entry(ino) {
            Ok(Entry::File(name)) => self.source.join(name).canonicalize().unwrap(),
            _ => {
                reply.error(EINVAL);
                return;
            }
        };
        let mut file = File::options().write(true).append(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        match file.write(data) {
            Ok(size) => reply.written(size as u32),
            Err(_) => reply.error(EINVAL),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] flush(ino: {:#x?}, fh: {}, lock_owner: {:?})",
            ino, fh, lock_owner
        );
        reply.error(ENOSYS);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        trace!("release(req, {_ino}, {_fh}, {_flags}, {_lock_owner:?}, {_flush}, reply)");
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fsync(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        trace!("opendir {ino}");
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        trace!("readdir {ino} {fh} {offset}");
        let entry = self.db.entry(ino);
        let tags = match entry {
            Ok(Entry::File(_)) => {
                reply.error(EINVAL);
                return;
            }
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
            Ok(Entry::Tags(tags)) => tags.clone(),
        };
        let mut cur = 0;
        for file in std::fs::read_dir(&self.source).unwrap() {
            cur += 1;
            if cur <= offset {
                continue;
            }
            let file = file.unwrap();
            let path = file.path().canonicalize().unwrap();
            let file_tags = self
                .db
                .file_tags(file.file_name().to_string_lossy())
                .unwrap();
            if !tags.is_subset(&file_tags) {
                continue;
            }
            let entry = Entry::from(path.as_ref());
            let f_ino = if let Ok(ino) = entry.inode(&self.db) {
                ino
            } else {
                entry.create(&self.db).unwrap()
            };
            if file.file_type().unwrap().is_file() {
                if reply.add(f_ino, cur, fuser::FileType::RegularFile, file.file_name()) {
                    reply.ok();
                    return;
                }
            }
        }
        for tag in self.db.sub_tags(&tags).unwrap() {
            cur += 1;
            if cur <= offset {
                continue;
            }
            let entry = Entry::Tags(BTreeSet::from([tag.clone()]));
            let ino = if let Ok(ino) = self.db.inode(&entry) {
                ino
            } else {
                entry.create(&self.db).unwrap()
            };
            if reply.add(ino, cur, fuser::FileType::Directory, tag) {
                reply.ok();
                return;
            }
        }
        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        debug!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        trace!("releasedir(req, {_ino}, {_fh}, {_flags:o}, reply)");
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fsyncdir(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        trace!("statfs(_req, {_ino}, reply)");
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        _value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] setxattr(ino: {:#x?}, name: {:?}, flags: {:#x?}, position: {})",
            ino, name, flags, position
        );
        reply.error(ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        debug!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
        reply.error(ENOSYS);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: fuser::ReplyXattr) {
        debug!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino, size
        );
        reply.error(ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino, name
        );
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(ENOSYS);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        trace!(
            "create(parent: {parent:#x?}, name: {name:?}, mode: {mode:o}, \
            umask: {umask:#x?}, flags: {flags:#x?})",
        );
        let source_path = self.source.join(name);
        if source_path.is_file() {
            reply.error(libc::EEXIST);
            return;
        }

        let c_path =
            unsafe { CString::from_vec_unchecked(source_path.as_os_str().as_bytes().to_vec()) };
        let new_fd = unsafe { libc::creat(c_path.as_ptr(), mode & !umask) };
        if new_fd == 0 {
            reply.error(EPERM);
            return;
        }

        let err = unsafe { libc::close(new_fd) };
        if err != 0 {
            reply.error(err);
            return;
        }
        let ino = self
            .db
            .inode_or_create(&Entry::from(source_path.as_ref()))
            .unwrap();
        let attr = file_attr_of_file(ino, &source_path);
        trace!("{ino} {attr:?}");
        let tags = match self.db.entry(parent) {
            Ok(Entry::Tags(tags)) => tags,
            _ => BTreeSet::new(),
        };
        trace!("{tags:?}");
        self.db
            .add_tags_to_file(tags, name.to_string_lossy())
            .unwrap();

        reply.created(&Duration::from_secs(0), &attr, 0, 0, 0);
        trace!("finished create");
    }

    fn getlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        debug!(
            "[Not Implemented] getlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {})",
            ino, fh, lock_owner, start, end, typ, pid
        );
        reply.error(ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] setlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {}, sleep: {})",
            ino, fh, lock_owner, start, end, typ, pid, sleep
        );
        reply.error(ENOSYS);
    }

    fn bmap(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        blocksize: u32,
        idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        debug!(
            "[Not Implemented] bmap(ino: {:#x?}, blocksize: {}, idx: {})",
            ino, blocksize, idx,
        );
        reply.error(ENOSYS);
    }

    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        debug!(
            "[Not Implemented] ioctl(ino: {:#x?}, fh: {}, flags: {}, cmd: {}, \
            in_data.len(): {}, out_size: {})",
            ino,
            fh,
            flags,
            cmd,
            in_data.len(),
            out_size,
        );
        reply.error(ENOSYS);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fallocate(ino: {:#x?}, fh: {}, offset: {}, \
            length: {}, mode: {:o})",
            ino, fh, offset, length, mode
        );
        reply.error(ENOSYS);
    }

    fn lseek(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        debug!(
            "[Not Implemented] lseek(ino: {:#x?}, fh: {}, offset: {}, whence: {})",
            ino, fh, offset, whence
        );
        reply.error(ENOSYS);
    }

    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        debug!(
            "[Not Implemented] copy_file_range(ino_in: {:#x?}, fh_in: {}, \
            offset_in: {}, ino_out: {:#x?}, fh_out: {}, offset_out: {}, \
            len: {}, flags: {})",
            ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags
        );
        reply.error(ENOSYS);
    }
}

fn file_attr_of_file<P: AsRef<Path>>(ino: u64, path: P) -> FileAttr {
    let metadata = std::fs::metadata(path).unwrap();
    let ctime = SystemTime::UNIX_EPOCH + Duration::from_nanos(metadata.ctime_nsec() as u64);
    FileAttr {
        ino,
        size: metadata.size(),
        blocks: metadata.blocks(),

        atime: metadata.accessed().unwrap(),
        mtime: metadata.modified().unwrap(),
        ctime,
        crtime: metadata.created().unwrap(),

        kind: match metadata.file_type() {
            t if t.is_dir() => fuser::FileType::Directory,
            t if t.is_file() => fuser::FileType::RegularFile,
            _ => fuser::FileType::Directory,
        },
        perm: metadata.permissions().mode() as u16,
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        blksize: metadata.blksize() as u32,
        flags: 0,
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum Entry {
    File(OsString),
    Tags(BTreeSet<String>),
}

impl Entry {
    fn file_type(&self) -> fuser::FileType {
        match self {
            Entry::File(_) => fuser::FileType::RegularFile,
            Entry::Tags(_) => fuser::FileType::Directory,
        }
    }

    fn fetch(conn: &TagsFsDb, ino: u64) -> anyhow::Result<Self> {
        Ok(conn.entry(ino)?)
    }

    fn inode(&self, conn: &TagsFsDb) -> Result<u64> {
        let (discriminant, data) = self.discrimimant_data();
        conn.inode(self)
    }

    fn inode_or_create(&self, conn: &TagsFsDb) -> u64 {
        if let Ok(ino) = self.inode(conn) {
            ino
        } else {
            // we checked above this isn't already in
            self.create(conn).unwrap()
        }
    }

    fn create(&self, conn: &TagsFsDb) -> Result<u64> {
        let (discriminant, data) = self.discrimimant_data();
        conn.create_inode(self)
    }

    pub(crate) fn discrimimant_data(&self) -> (&str, Cow<str>) {
        match self {
            Entry::File(name) => ("file", name.to_string_lossy()),
            Entry::Tags(tags) => ("tags", Cow::Owned(tags.iter().sorted().join("/"))),
        }
    }
}

impl From<&Path> for Entry {
    fn from(p: &Path) -> Self {
        Entry::File(p.file_name().unwrap().to_os_string())
    }
}

