use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use rusqlite::{named_params, params, types::ValueRef, Connection, ToSql};

use crate::{
    error::{Error, Result},
    filesystem::Entry,
    Tag,
};

pub struct TagsFsDb {
    conn: Connection,
}

impl TagsFsDb {
    pub fn new<P>(p: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            conn: Connection::open(p)?,
        })
    }

    pub fn mountpoint(&self) -> Result<PathBuf> {
        Ok(self.conn
            .prepare("SELECT value FROM config WHERE key = 'mountpoint'")?
            .query_row([], |r| r.get::<_, String>(0))?.into())
    }

    pub fn sub_tags(&self, tags: &BTreeSet<Tag>) -> Result<Vec<Tag>> {
        let mut stmt = self.conn.prepare_cached(
            format!(
                "SELECT tag FROM tags WHERE tag NOT IN ({})",
                vec!["?"; tags.len()].join(", "),
            )
            .as_str(),
        )?;
        let sub_tags = stmt
            .query_map(rusqlite::params_from_iter(tags.iter()), |row| {
                row.get::<_, Tag>(0)
            })?
            .collect::<std::result::Result<_, _>>()?;
        Ok(sub_tags)
    }

    pub fn file_tags(&self, filename: impl ToSql) -> Result<BTreeSet<String>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT DISTINCT tag \
                 FROM file_tags \
                 JOIN tags \
                 ON file_tags.tag_id = tags.id \
                 WHERE file = ?",
        )?;
        let tags = stmt
            .query_map([filename], |row| row.get("tag"))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(tags)
    }

    pub fn remove_tags_from_file<I, It>(&self, tags: I, file: impl ToSql) -> Result<()>
    where
        I: IntoIterator<Item = It>,
        It: ToSql,
    {
        let mut stmt = self
            .conn
            .prepare_cached("DELETE FROM file_tags WHERE tag_id = ? AND file = ?")?;
        for tag in tags {
            let tag_id: u64 =
                self.conn
                    .query_row("SELECT id FROM tags WHERE tag = ?", [tag], |r| r.get(0))?;
            stmt.execute(params![tag_id, file])?;
        }
        Ok(())
    }

    pub fn add_tags_to_file<I, It>(&self, tags: I, file: impl ToSql) -> Result<()>
    where
        I: IntoIterator<Item = It>,
        It: ToSql,
    {
        for tag in tags {
            let tag_id = self.tag_id(&tag).or_else(|_| self.create_tag(&tag))?;
            self.conn
                .prepare_cached("INSERT INTO file_tags (file, tag_id) VALUES (?, ?)")?
                .insert(params![file, tag_id])?;
        }
        Ok(())
    }

    pub fn delete_tags(&self, tags: &BTreeSet<Tag>) -> Result<()> {
        for tag in tags {
            let tag_id: u64 =
                self.conn
                    .query_row("SELECT id FROM tags WHERE tag = ?", [tag], |r| r.get(0))?;
            self.conn
                .prepare_cached("DELETE FROM tags WHERE id = ?")?
                .execute([tag_id])?;
            self.conn
                .prepare_cached("DELETE FROM file_tags WHERE tag_id = ?")?
                .execute([tag_id])?;
        }
        Ok(())
    }

    pub fn create_inode(&self, entry: &Entry) -> Result<u64> {
        let (discriminant, data) = entry.discrimimant_data();
        Ok(self
            .conn
            .prepare_cached(
                "INSERT INTO inodes (discriminant, data) VALUES (:discriminant, :data);",
            )?
            .insert(named_params! {
                ":discriminant": discriminant,
                ":data": data,
            })? as u64)
    }

    pub fn inode(&self, entry: &Entry) -> Result<u64> {
        let (discriminant, data) = entry.discrimimant_data();
        let mut stmt = self.conn.prepare_cached(
            "SELECT * FROM inodes WHERE discriminant = :discriminant AND data = :data",
        )?;
        let ino = stmt.query_row(
            named_params! {
                ":discriminant": discriminant,
                ":data": data,
            },
            |row| row.get("id"),
        )?;
        Ok(ino)
    }

    pub fn entry(&self, ino: u64) -> Result<Entry> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT * FROM inodes WHERE id = ?")?;
        let entry = stmt.query_row([ino], |row| {
            let data: String = row.get("data")?;
            Ok(match row.get_ref("discriminant")? {
                ValueRef::Text(b"tags") => Ok(Entry::Tags(
                    data.split('/')
                        .filter(|x| *x != "")
                        .map(String::from)
                        .collect(),
                )),
                ValueRef::Text(b"file") => Ok(Entry::File(data.into())),
                _ => Err(Error::InvalidEntryDiscriminant),
            })
        })??;
        Ok(entry)
    }

    pub fn source(&self) -> Result<PathBuf> {
        Ok(self
            .conn
            .prepare("SELECT value FROM options WHERE key = 'source'")?
            .query_row([], |row| row.get::<_, String>(0))
            .map(PathBuf::from)?)
    }

    pub fn inode_or_create(&self, entry: &Entry) -> Result<u64> {
        self.inode(entry).or_else(|_| self.create_inode(entry))
    }

    pub fn create_tag(&self, tag: impl ToSql) -> Result<u64> {
        Ok(self
            .conn
            .prepare_cached("INSERT INTO tags (tag) VALUES (?)")?
            .insert([tag])? as u64)
    }

    pub fn tag_id(&self, tag: impl ToSql) -> Result<u64> {
        Ok(self
            .conn
            .prepare_cached("SELECT id FROM tags WHERE tag = ?")?
            .query_row([tag], |r| r.get(0))?)
    }
}
