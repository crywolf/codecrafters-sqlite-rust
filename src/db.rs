mod db_info;
mod page;
mod pager;
mod schema;
mod sql;

use std::fs::File;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use db_info::DBInfo;
use pager::Pager;
use schema::{Schema, SchemaType};

#[derive(Debug)]
pub struct DB {
    db_info: DBInfo,
    pager: Pager<File>,
}

impl DB {
    pub fn new(file: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(&file).context("open DB file")?;
        let (pager, db_info) = Pager::new(file)?;
        Ok(Self { db_info, pager })
    }

    pub fn page_size(&self) -> u16 {
        self.db_info.page_size
    }

    pub fn read_format(&self) -> u8 {
        self.db_info.read_format
    }

    pub fn write_format(&self) -> u8 {
        self.db_info.write_format
    }

    pub fn text_encoding(&self) -> u32 {
        self.db_info.text_encoding
    }

    pub fn reserved_bytes(&self) -> u8 {
        self.db_info.reserved_bytes
    }

    pub fn file_change_counter(&self) -> u32 {
        self.db_info.file_change_counter
    }

    pub fn n_pages(&self) -> u32 {
        self.db_info.n_pages
    }

    pub fn n_freelist_pages(&self) -> u32 {
        self.db_info.n_freelist_pages
    }

    pub fn schema_cookie(&self) -> u32 {
        self.db_info.schema_cookie
    }

    pub fn schema_format(&self) -> u32 {
        self.db_info.schema_format
    }
    pub fn default_cache_size(&self) -> u32 {
        self.db_info.default_cache_size
    }

    pub fn sqlite_version_number(&self) -> u32 {
        self.db_info.sqlite_version_number
    }

    pub fn application_id(&self) -> u32 {
        self.db_info.application_id
    }

    pub fn table_names(&self, include_internal: bool) -> Vec<String> {
        self.db_info
            .schemas(SchemaType::Table)
            .iter()
            .filter(|&s| {
                if include_internal {
                    true
                } else {
                    s.tbl_name != "sqlite_sequence"
                }
            }) // sqlite_sequence is an internal object
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

    pub fn execute(&mut self, sql: &str) -> Result<Vec<Vec<String>>> {
        let cmd = sql::parse_command(sql)?;
        let res = cmd.execute(self)?;
        Ok(res)
    }

    pub(crate) fn root_page_num(&self, table: &str) -> Result<u64> {
        let schema = self
            .schema(table)
            .ok_or_else(|| anyhow::anyhow!("No schema found for {}", table))?;

        Ok(schema.rootpage)
    }

    pub(crate) fn table(&self, name: &str) -> Result<Table> {
        /* To extract data for a single column, you'll need to know the order of that column in the sequence.
        You'll need to parse the table's CREATE TABLE statement to do this. */
        let schema = self
            .schema(name)
            .ok_or(anyhow!("table {} does not exist", name))?;

        let cmd = sql::parse_command(&schema.sql).context("parse schema")?;

        let (columns, primary_key_column_index) = match cmd {
            sql::Command::Create {
                columns,
                primary_key,
                ..
            } => (columns, primary_key),
            _ => bail!("Schema is broken"),
        };

        Ok(Table {
            name: name.to_string(),
            columns,
            primary_key_column_index,
        })
    }

    pub(crate) fn schema(&self, table: &str) -> Option<Schema> {
        self.db_info
            .schemas
            .iter()
            .find(|&s| s.tbl_name == table)
            .cloned()
    }
}

pub(crate) struct Table {
    name: String,
    columns: Vec<String>,
    primary_key_column_index: u16,
}
