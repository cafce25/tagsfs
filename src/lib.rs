pub mod filesystem;
pub use filesystem::TagsFs;

pub mod database;
pub use database::TagsFsDb;

pub mod error;

pub type Tag = String;
