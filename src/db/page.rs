use std::io::Read;

use anyhow::{Context, Result};

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Page {
    pub page_type: u8,
    pub freeblocks: u16,
    pub n_cells: u16,
    pub content_start: u16,
    pub fragmented: u8,
}

impl Page {
    pub(crate) fn new(mut db_file: impl Read) -> Result<Self> {
        let mut page_header = [0; 8];
        db_file
            .read_exact(&mut page_header)
            .context("read page header (8 bytes)")?;

        let page_type = page_header[0];
        anyhow::ensure!(
            page_type == 13, // A value of 13 (0x0d) means the page is a leaf table b-tree page.
            "First page shoud be a leaf table b-tree page"
        );

        // Start of the first freeblock on the page, or is zero if there are no freeblocks.
        let freeblocks = u16::from_be_bytes([page_header[1], page_header[2]]);

        // The page stores the rows of the table in chunks of data called "cells." Each cell stores a single row.
        // The two-byte integer at offset 3 gives the number of cells on the page.
        let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

        // Start of the cell content area
        let content_start = u16::from_be_bytes([page_header[5], page_header[6]]);

        // Number of fragmented free bytes within the cell content area
        let fragmented = page_header[7];

        Ok(Self {
            page_type,
            freeblocks,
            n_cells,
            content_start,
            fragmented,
        })
    }
}
