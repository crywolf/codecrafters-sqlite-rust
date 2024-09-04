use anyhow::{bail, Result};

use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;

            // The first 100 bytes of the database file comprise the database file header.
            // https://www.sqlite.org/fileformat.html#the_database_header
            let mut file_header = [0; 100];
            file.read_exact(&mut file_header)?;

            anyhow::ensure!(
                file_header.starts_with(b"SQLite format 3"),
                "Incorrect database format"
            );

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([file_header[16], file_header[17]]);
            println!("database page size: {}", page_size);

            // The sqlite_schema page is always page 1, and it always begins at offset 0. The file header is a part of the page.
            // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
            // https://www.sqlite.org/fileformat.html#b_tree_pages

            // In this challenge, you can assume that the sqlite_schema table is small enough to fit entirely on a single page.
            let mut page_header = [0; 8];
            file.read_exact(&mut page_header)?;

            let page_type = page_header[0];
            anyhow::ensure!(
                page_type == 13, // A value of 13 (0x0d) means the page is a leaf table b-tree page.
                "First page shoud be a leaf table b-tree page"
            );

            // In this challenge, you can assume that databases only contain tables—no indexes, views, or triggers.
            // So, each row in sqlite_schema represents a table in the database.
            // As a result, you can get the total number of tables in the database by getting the number of rows in sqlite_schema.
            // The sqlite_schema page stores the rows of the sqlite_schema table in chunks of data called "cells." Each cell stores a single row.
            // The two-byte integer at offset 3 gives the number of cells on the page.
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);
            println!("number of tables: {}", n_cells);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
