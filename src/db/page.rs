use std::io::{Read, Seek};

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes, BytesMut};

use crate::db::schema::{varint, ColumnType};

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Page {
    data_size: u16,
    page_type: u8,
    freeblocks: u16,
    pub n_cells: u16,
    content_start: u16,
    fragmented: u8,
    pub primary_key_column: u16,
    cell_pointers: Vec<u8>,
    pub table_columns: Vec<String>,
    pub cells: Vec<Cell>,
}

impl Page {
    pub(crate) fn new(
        mut db_file: impl Read + Seek,
        page_size: u16,
        table_columns: Vec<String>,
        primary_key_column: u16,
    ) -> Result<Self> {
        let mut page_header = [0; 8];
        db_file
            .read_exact(&mut page_header)
            .context("read page header (first 8 bytes)")?;

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

        // Size of the data area contained in this page
        let data_size = page_size - content_start;

        // Number of fragmented free bytes within the cell content area
        let fragmented = page_header[7];

        // The cell pointer array of a b-tree page immediately follows the b-tree page header.
        // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        // The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        let mut cell_pointers = vec![0; 2 * n_cells as usize];
        db_file
            .read_exact(&mut cell_pointers)
            .context("read cell pointers")?;

        let mut previous_offset = page_size;

        db_file
            .seek_relative((page_size as usize - page_header.len() - cell_pointers.len()) as i64)
            .context("move to the end of the page")?;

        let mut cells = Vec::new();

        for i_cell in 0..n_cells as usize {
            let offset =
                u16::from_be_bytes([cell_pointers[i_cell * 2], cell_pointers[i_cell * 2 + 1]]);

            let cell_size = (previous_offset - offset) as usize;

            db_file
                .seek_relative(-(cell_size as i64))
                .context("move to the beginning of the cell")?;

            let mut cell = BytesMut::zeroed(cell_size);

            db_file
                .read_exact(&mut cell)
                .context("read the cell content")?;

            // Size of the record (varint)
            let payload_size =
                varint(&mut cell).with_context(|| format!("get int from varint {:?}", cell))?;

            // rowid (varint)
            let row_id = varint(&mut cell)
                .with_context(|| format!("get int from varint {:?}", cell))?
                as u64;

            // discard unused cell content
            // cell now contains only valid record content
            cell.resize(payload_size as usize, 0);

            let payload = Bytes::from(cell);
            let c = Cell::new(row_id, payload).context("get a cell")?;

            cells.push(c);

            previous_offset = offset + cell_size as u16;
        }

        Ok(Self {
            data_size,
            page_type,
            freeblocks,
            n_cells,
            content_start,
            fragmented,
            primary_key_column,
            cell_pointers,
            table_columns,
            cells,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Cell {
    row_id: u64,
    record: Record,
}

impl Cell {
    pub fn new(row_id: u64, mut payload: Bytes) -> Result<Self> {
        // Size of the record header (varint)
        let header_size = varint(&mut payload)
            .with_context(|| format!("get record header_size from varint for row id {}", row_id))?;

        let header_size = header_size as usize - 1; // minus first byte (as it is the header size)

        let mut offset: usize = 0;
        let mut columns = Vec::new();

        let payload_length = payload.remaining();

        // Read the record header consuming the header bytes
        while payload.remaining() > payload_length - header_size {
            let column_type =
                super::schema::column_type(&mut payload).context("get column type")?;

            let val_len = match column_type {
                ColumnType::Text(v) => v,
                ColumnType::Int(v) => v,
                ColumnType::Blob(v) => v,
                ColumnType::Float(v) => v,
                ColumnType::Null(v) => v,
            };

            let col = RecordColumn {
                offset,
                typ: column_type,
                val_len,
            };

            columns.push(col);

            offset += val_len as usize;
        }

        // What left in payload is recod body
        let record = Record {
            row_id,
            body: payload,
            columns,
        };

        Ok(Self { row_id, record })
    }

    /// Returns content of the column
    pub(crate) fn column(&self, column_index: u16, primary_key_column: u16) -> Result<String> {
        let col = self
            .record
            .columns
            .get(column_index as usize)
            .ok_or(anyhow!("column index {column_index} out of range"))?;

        let val = if column_index == primary_key_column {
            Ok(self.row_id.to_string())
        } else {
            String::from_utf8(self.record.body[col.offset..][..col.val_len as usize].to_vec())
        };

        let s = match col.typ {
            ColumnType::Text(_) => val,
            ColumnType::Int(_) => val,
            ColumnType::Null(_) => Ok("null".to_string()),
            _ => unimplemented!(),
        }
        .context("transforming column value to string")?;

        Ok(s)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Record {
    row_id: u64,
    body: Bytes,
    columns: Vec<RecordColumn>,
}

#[derive(Debug)]
struct RecordColumn {
    offset: usize,
    typ: ColumnType,
    val_len: u64,
}
