mod db_info;
mod page;
mod schema;
mod sql;

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use db_info::DBInfo;
use schema::{Schema, SchemaType};

#[derive(Debug)]
pub struct DB {
    file: File,
    db_info: DBInfo,
}

impl DB {
    pub fn new(file: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(&file).context("open DB file")?;
        let db_info = DBInfo::new(file.try_clone()?)?;
        Ok(Self { file, db_info })
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
            .schemas(SchemaType::Table)
            .iter()
            .filter(|&s| s.tbl_name != "sqlite_sequence") // sqlite_sequence is an internal object
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn index_names(&self) -> Vec<String> {
        self.db_info
            .schemas(SchemaType::Index)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn view_names(&self) -> Vec<String> {
        self.db_info
            .schemas(SchemaType::View)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn trigger_names(&self) -> Vec<String> {
        self.db_info
            .schemas(SchemaType::Trigger)
            .iter()
            .map(|&s| s.tbl_name.clone())
            .collect()
    }

    pub fn schemas_sql(&self) -> Vec<String> {
        self.db_info.schemas.iter().map(|s| s.sql.clone()).collect()
    }

    pub(crate) fn schema(&self, tbl_name: &str) -> Option<Schema> {
        self.db_info
            .schemas
            .iter()
            .find(|&s| s.tbl_name == tbl_name)
            .cloned()
    }

    pub fn execute(&self, sql: &str) -> Result<String> {
        let cmd = sql::parse(sql)?;

        let res = cmd.execute(self)?;

        Ok(res)
    }
}
