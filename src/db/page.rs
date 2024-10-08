use std::io::{Read, Seek};

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};

use crate::db::schema::{varint, ColumnType};

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum PageType {
    TableInterior,
    TableLeaf,
    IndexInterior,
    IndexLeaf,
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::IndexInterior), // Index B-Tree Interior Cell (header 0x02)
            5 => Ok(Self::TableInterior), // Table B-Tree Interior Cell (header 0x05)
            10 => Ok(Self::IndexLeaf),    // Index B-Tree Leaf Cell (header 0x0a)
            13 => Ok(Self::TableLeaf),    // ATable B-Tree Leaf Cell (header 0x0d):
            _ => Err(anyhow!("Unknown page type: {}", value)),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Page {
    data_size: u16,
    pub page_type: PageType,
    freeblocks: u16,
    pub n_cells: u16,
    content_start: u16,
    fragmented: u8,
    /// Only in Interior Pages: The page number of the "root" of the subtree
    /// that contains records with keys greater than the largest key in the page.
    pub rightmost_pointer: Option<u32>,
    cell_pointers: Vec<u8>,
    pub cells: Vec<Cell>,
}

impl Page {
    pub(crate) fn load(db_file: &mut (impl Read + Seek), page_size: u16) -> Result<Self> {
        // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
        // https://www.sqlite.org/fileformat.html#b_tree_pages
        let mut page_header = [0; 8];
        db_file
            .read_exact(&mut page_header)
            .context("read page header (first 8 bytes)")?;

        let page_type = page_header[0].try_into()?;

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

        // The four-byte page number at offset 8 is the right-most pointer.
        // This value appears in the header of interior b-tree pages only and is omitted from all other pages.
        let mut rightmost_pointer = None;
        let mut extra_header = [0; 4];

        if page_type == PageType::TableInterior || page_type == PageType::IndexInterior {
            db_file
                .read_exact(&mut extra_header)
                .context("read page header (extra 4 bytes in interior pages)")?;
            rightmost_pointer = Some(u32::from_be_bytes(extra_header));
        }

        let page_header_len = match page_type {
            PageType::TableInterior | PageType::IndexInterior => page_header.len() + 4,
            _ => page_header.len(),
        };

        // The cell pointer array of a b-tree page immediately follows the b-tree page header.
        // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        // The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        let mut cell_pointers = vec![0; 2 * n_cells as usize];
        db_file
            .read_exact(&mut cell_pointers)
            .context("read cell pointers")?;

        let mut previous_offset = page_size;

        db_file
            .seek_relative((page_size as usize - page_header_len - cell_pointers.len()) as i64)
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

            let cell: Cell = match page_type {
                PageType::TableLeaf => {
                    // Size of the record (varint)
                    let payload_size = varint(&mut cell)
                        .with_context(|| format!("get int from varint {:?}", cell))?;

                    // rowid (varint)
                    let row_id = varint(&mut cell)
                        .with_context(|| format!("get int from varint {:?}", cell))?
                        as u64;

                    // discard unused cell content
                    // cell now contains only valid record content
                    cell.resize(payload_size as usize, 0);

                    let payload = Bytes::from(cell);
                    TableLeafCell::new(row_id, payload)
                        .context("get a TableLeafCell")?
                        .into()
                }
                PageType::TableInterior => {
                    let left_child_page = cell.get_u32();
                    let key = varint(&mut cell)? as u64;
                    TableInteriorCell::new(left_child_page, key).into()
                }
                PageType::IndexLeaf => {
                    // Size of the payload (varint)
                    let payload_size = varint(&mut cell)
                        .with_context(|| format!("get int from varint {:?}", cell))?;

                    // discard unused cell content
                    // cell now contains only valid payload content
                    cell.resize(payload_size as usize, 0);

                    let payload = Bytes::from(cell);
                    IndexLeafCell::new(payload)
                        .context("get an IndexLeafCell")?
                        .into()
                }
                PageType::IndexInterior => {
                    let left_child_page = cell.get_u32();
                    // Size of the payload (varint)
                    let payload_size = varint(&mut cell)
                        .with_context(|| format!("get int from varint {:?}", cell))?;

                    // discard unused cell content
                    // cell now contains only valid payload content
                    cell.resize(payload_size as usize, 0);

                    let payload = Bytes::from(cell);

                    IndexInteriorCell::new(left_child_page, payload)
                        .context("get an IndexInteriorCell")?
                        .into()
                }
            };

            cells.push(cell);

            previous_offset = offset + cell_size as u16;
        }

        Ok(Self {
            data_size,
            page_type,
            freeblocks,
            n_cells,
            content_start,
            fragmented,
            rightmost_pointer,
            cell_pointers,
            cells,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Cell {
    TableLeaf(TableLeafCell),
    TableInterior(TableInteriorCell),
    IndexLeaf(IndexLeafCell),
    IndexInterior(IndexInteriorCell),
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct TableInteriorCell {
    /// The page number of the "root" page of a subtree that contains records with keys lower or equal to key ie. row_id (primary key).
    left_child_page: u32,
    /// Cells in interior pages are logically ordered by key in ascending order.
    row_id: u64,
}

impl TableInteriorCell {
    fn new(left_child_page: u32, row_id: u64) -> Self {
        Self {
            left_child_page,
            row_id,
        }
    }

    /// Returns the page number of the "root" page of a subtree that contains records with keys lower or equal to key ie. row_id (primary key).
    pub(crate) fn left_child_page(&self) -> u32 {
        self.left_child_page
    }

    pub(crate) fn row_id(&self) -> u64 {
        self.row_id
    }
}

impl From<TableInteriorCell> for Cell {
    fn from(cell: TableInteriorCell) -> Self {
        Cell::TableInterior(cell)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableLeafCell {
    row_id: u64,
    record: Record,
}

impl From<TableLeafCell> for Cell {
    fn from(cell: TableLeafCell) -> Self {
        Cell::TableLeaf(cell)
    }
}

impl TableLeafCell {
    fn new(row_id: u64, payload: Bytes) -> Result<Self> {
        let record = Record::parse(payload).context("parse record payload")?;
        Ok(Self { row_id, record })
    }

    /// Returns content of the column
    pub(crate) fn column(
        &self,
        column_index: u16,
        primary_key_column: u16,
    ) -> Result<ColumnContent> {
        if column_index == primary_key_column {
            return Ok(ColumnContent::Int(self.row_id as i64));
        }

        self.record.column(column_index)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Record {
    body: Bytes,
    columns: Vec<RecordColumn>,
}

impl Record {
    fn parse(mut payload: Bytes) -> Result<Self> {
        // Size of the record header (varint)
        let header_size = varint(&mut payload).context("get record header_size from varint")?;

        let header_size = header_size as usize - 1; // minus first byte (as it is the header size)

        let mut offset: usize = 0;
        let mut columns = Vec::new();

        let payload_length = payload.remaining();

        // Read the record header consuming the header bytes
        while payload.remaining() > payload_length - header_size {
            let column_type =
                super::schema::column_type(&mut payload).context("get column type")?;

            let column_length = column_type.column_bytes_lenght();

            let col = RecordColumn {
                offset,
                typ: column_type,
            };

            columns.push(col);

            offset += column_length as usize;
        }

        // What's left in payload is record body
        Ok(Self {
            body: payload,
            columns,
        })
    }

    /// Returns content of the record column
    fn column(&self, column_index: u16) -> Result<ColumnContent> {
        let col = self
            .columns
            .get(column_index as usize)
            .ok_or(anyhow!("column index {column_index} out of range"))?;

        let column_type = &col.typ;
        let column_length = column_type.column_bytes_lenght();

        let column_bytes = &self.body[col.offset..][..column_length as usize];

        let content = match column_type {
            ColumnType::Text(_) => {
                ColumnContent::Text(String::from_utf8_lossy(column_bytes).to_string())
            }
            ColumnType::Int(int_len) => match int_len {
                1 => ColumnContent::Int(i8::from_be_bytes(column_bytes.try_into()?) as i64),
                2 => ColumnContent::Int(i16::from_be_bytes(column_bytes.try_into()?) as i64),
                3 => {
                    let mut alligned = vec![0u8; 1];
                    alligned.extend_from_slice(column_bytes);
                    ColumnContent::Int(i32::from_be_bytes(alligned.as_slice().try_into()?) as i64)
                }
                4 => ColumnContent::Int(i32::from_be_bytes(column_bytes.try_into()?) as i64),
                6 => {
                    let mut alligned = vec![0u8; 2];
                    alligned.extend_from_slice(column_bytes);
                    ColumnContent::Int(i64::from_be_bytes(alligned.as_slice().try_into()?) as i64)
                }

                8 => ColumnContent::Int(i64::from_be_bytes(column_bytes.try_into()?) as i64),
                _ => bail!("Invalid INT column length: {:?} bytes", int_len),
            },
            ColumnType::Int0(_) => ColumnContent::Int(0),
            ColumnType::Int1(_) => ColumnContent::Int(1),
            ColumnType::Null(_) => ColumnContent::Null,
            _ => unimplemented!("column type: {:?}", column_type),
        };

        Ok(content)
    }
}

#[derive(Debug, Clone)]
pub enum ColumnContent {
    Text(String),
    Int(i64),
    Null,
}

impl std::fmt::Display for ColumnContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnContent::Text(s) => write!(f, "{}", s),
            ColumnContent::Int(i) => write!(f, "{}", i),
            ColumnContent::Null => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RecordColumn {
    offset: usize,
    typ: ColumnType,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct IndexLeafCell {
    record: Record,
}

impl IndexLeafCell {
    fn new(payload: Bytes) -> Result<Self> {
        let record = Record::parse(payload).context("parse record payload")?;
        Ok(Self { record })
    }

    /// Returns content of the index column
    pub(crate) fn key(&self) -> Result<ColumnContent> {
        self.record.column(0)
    }

    /// Returns content of the row_id
    pub(crate) fn row_id(&self) -> Result<ColumnContent> {
        self.record.column(1)
    }
}

impl From<IndexLeafCell> for Cell {
    fn from(cell: IndexLeafCell) -> Self {
        Cell::IndexLeaf(cell)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct IndexInteriorCell {
    /// The page number of the "root" page of a subtree that contains records with keys lower or equal to key (ie. indexed column).
    left_child_page: u32,
    record: Record,
}

impl IndexInteriorCell {
    fn new(left_child_page: u32, payload: Bytes) -> Result<Self> {
        let record = Record::parse(payload).context("parse record payload")?;
        Ok(Self {
            left_child_page,
            record,
        })
    }

    /// Returns page number of the "root" page of a subtree that contains records with keys lower or equal to key (ie. indexed column).
    pub(crate) fn left_child_page(&self) -> u32 {
        self.left_child_page
    }

    /// Returns content of the index key (ie. indexed column). Cells in interior pages are logically ordered by key in ascending order.
    pub(crate) fn key(&self) -> Result<ColumnContent> {
        self.record.column(0)
    }

    /// Returns content of the row_id
    pub(crate) fn row_id(&self) -> Result<ColumnContent> {
        self.record.column(1)
    }
}

impl From<IndexInteriorCell> for Cell {
    fn from(cell: IndexInteriorCell) -> Self {
        Cell::IndexInterior(cell)
    }
}
