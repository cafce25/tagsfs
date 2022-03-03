pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error")]
    Database(#[from] rusqlite::Error),
    #[error("database contains invalid discriminant")]
    InvalidEntryDiscriminant,
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("file system error")]
    StdC(i32),
}
