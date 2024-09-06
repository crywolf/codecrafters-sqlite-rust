mod db_info;
mod schema;

use std::path::Path;
use std::{fs::File, path::PathBuf};

use anyhow::{Context, Result};

use db_info::DBInfo;

#[derive(Debug)]
#[allow(dead_code)]
pub struct DB {
    path: PathBuf,
    db_info: DBInfo,
}

impl DB {
    pub fn new(file: impl AsRef<Path>) -> Result<Self> {
        let db = File::open(&file).context("open DB file")?;
        let db_info = DBInfo::new(db)?;
        Ok(Self {
            path: PathBuf::from(file.as_ref()),
            db_info,
        })
    }

    pub fn page_size(&self) -> u16 {
        self.db_info.page_size
    }

    pub fn text_encoding(&self) -> u32 {
        self.db_info.text_encoding
    }

    pub fn reserved_bytes(&self) -> u8 {
        self.db_info.reserved_bytes
    }

    pub fn n_pages(&self) -> u32 {
        self.db_info.n_pages
    }

    pub fn n_freelist_pages(&self) -> u32 {
        self.db_info.n_freelist_pages
    }

    pub fn schema_format(&self) -> u32 {
        self.db_info.schema_format
    }

    pub fn table_names(&self) -> Vec<String> {
        self.db_info
            .schemas(schema::SchemaType::Table)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn index_names(&self) -> Vec<String> {
        self.db_info
            .schemas(schema::SchemaType::Index)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn view_names(&self) -> Vec<String> {
        self.db_info
            .schemas(schema::SchemaType::View)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn trigger_names(&self) -> Vec<String> {
        self.db_info
            .schemas(schema::SchemaType::Trigger)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn schemas(&self) -> Vec<String> {
        self.db_info.schemas.iter().map(|s| s.sql.clone()).collect()
    }
}
