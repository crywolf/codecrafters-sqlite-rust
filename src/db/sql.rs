use std::io::{Seek, SeekFrom};

use anyhow::{bail, Context, Result};

use crate::db::page::Page;

use super::DB;

pub(crate) fn parse(sql: &str) -> Result<SelectCmd> {
    if let Some(table) = sql.to_lowercase().split_whitespace().last() {
        Ok(SelectCmd {
            columns: vec!["count(*)".to_string()], // TODO
            table: table.to_string(),
        })
    } else {
        bail!(
            "Invalid command: {}; unable to determine the table name!",
            sql
        );
    }
}

#[allow(dead_code)]
pub(crate) struct SelectCmd {
    pub columns: Vec<String>,
    pub table: String,
}

impl SelectCmd {
    pub fn execute(&self, db: &DB) -> Result<String> {
        let schema = db
            .schema(&self.table)
            .ok_or_else(|| anyhow::anyhow!("No schema found for {}", self.table))?;

        let page_start = (schema.rootpage - 1) * db.page_size() as u64;

        let mut db_file = &db.file;
        db_file
            .seek(SeekFrom::Start(page_start))
            .context("seek offset in the DB file")?;

        let page = Page::new(db_file)?;

        Ok(count(&page)?.to_string())
    }
}

pub(crate) fn count(page: &Page) -> Result<u64> {
    // "SELECT COUNT(*) FROM apples"
    Ok(page.n_cells as u64)
}
