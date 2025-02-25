use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};

use std::io::prelude::*;

use super::schema::{Schema, SchemaType};
use crate::db::schema::{varint, ColumnType};

#[derive(Debug)]
pub(crate) struct DBInfo {
    pub page_size: u16,
    pub read_format: u8,
    pub write_format: u8,
    pub reserved_bytes: u8,
    pub file_change_counter: u32,
    pub n_pages: u32,
    pub n_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub text_encoding: u32,
    pub default_cache_size: u32,
    pub application_id: u32,
    pub sqlite_version_number: u32,
    pub schemas: Vec<Schema>,
}

impl DBInfo {
    pub(crate) fn new<T>(mut db_file: T) -> Result<(Self, T)>
    where
        T: Read + Seek,
    {
        // The first 100 bytes of the database file comprise the database file header.
        // https://www.sqlite.org/fileformat.html#the_database_header
        let mut file_header = [0; 100];
        db_file.read_exact(&mut file_header)?;

        anyhow::ensure!(
            file_header.starts_with(b"SQLite format 3"),
            "Incorrect database format"
        );

        // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
        let page_size = u16::from_be_bytes([file_header[16], file_header[17]]);

        // File format write version. 1 for legacy; 2 for WAL.
        let read_format = file_header[18];

        // File format read version. 1 for legacy; 2 for WAL.
        let write_format = file_header[19];

        // Bytes of unused "reserved" space at the end of each page. Usually 0.
        let reserved_bytes = file_header[20];

        //  File change counter.
        let file_change_counter = u32::from_be_bytes(file_header[24..28].try_into()?);

        // Size of the database file in pages. The "in-header database size".
        let n_pages = u32::from_be_bytes(file_header[28..32].try_into()?);

        // Page number of the first freelist trunk page.
        //let n_freelist_pages = u32::from_be_bytes(file_header[32..36].try_into()?);
        let n_freelist_pages = u32::from_be_bytes(file_header[36..40].try_into()?);

        // The schema cookie.
        let schema_cookie = u32::from_be_bytes(file_header[40..44].try_into()?);

        // The schema format number. Supported schema formats are 1, 2, 3, and 4.
        let schema_format = u32::from_be_bytes(file_header[44..48].try_into()?);

        let default_cache_size = u32::from_be_bytes(file_header[48..52].try_into()?);

        // The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
        let text_encoding = u32::from_be_bytes(file_header[56..60].try_into()?);
        anyhow::ensure!(text_encoding == 1, "Only UTF-8 encoding is supported");

        let application_id = u32::from_be_bytes(file_header[68..72].try_into()?);

        let sqlite_version_number = u32::from_be_bytes(file_header[96..100].try_into()?);

        let mut db_info = Self {
            page_size,
            read_format,
            write_format,
            reserved_bytes,
            file_change_counter,
            n_pages,
            n_freelist_pages,
            schema_cookie,
            schema_format,
            default_cache_size,
            text_encoding,
            application_id,
            sqlite_version_number,
            schemas: Vec::new(),
        };

        // The sqlite_schema page is always page 1, and it always begins at offset 0. The file header is a part of the page.
        // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
        // https://www.sqlite.org/fileformat.html#b_tree_pages

        // In this challenge, you can assume that the sqlite_schema table is small enough to fit entirely on a single page.
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
        let _freeblocks = u16::from_be_bytes([page_header[1], page_header[2]]);

        // In this challenge, you can assume that databases only contain tables — no indexes, views, or triggers.
        // So, each row in sqlite_schema represents a table in the database.
        // As a result, you can get the total number of tables in the database by getting the number of rows in sqlite_schema.
        // The sqlite_schema page stores the rows of the sqlite_schema table in chunks of data called "cells." Each cell stores a single row.
        // The two-byte integer at offset 3 gives the number of cells on the page.
        let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as usize;

        // Start of the cell content area
        let _content_start = u16::from_be_bytes([page_header[5], page_header[6]]);

        // Number of fragmented free bytes within the cell content area
        let _fragmented = page_header[7];

        // The cell pointer array of a b-tree page immediately follows the b-tree page header.
        // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        // The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        let mut cell_pointers = vec![0; n_cells * 2];
        db_file
            .read_exact(&mut cell_pointers)
            .context("read cell pointers")?;

        let mut previous_offset = page_size;

        db_file
            .seek_relative(
                (page_size as usize - file_header.len() - page_header.len() - cell_pointers.len())
                    as i64,
            )
            .context("move to the end of the page")?;

        for i_cell in 0..n_cells {
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
            let _payload_size =
                varint(&mut cell).with_context(|| format!("get int from varint {:?}", cell))?;

            // rowid (varint)
            let _row_id =
                varint(&mut cell).with_context(|| format!("get int from varint {:?}", cell))?;

            // Size of the record header (varint)
            let header_size =
                varint(&mut cell).with_context(|| format!("get int from varint {:?}", cell))?;

            let mut record_header = Bytes::copy_from_slice(&cell[..header_size as usize - 1]); // minus first byte (as it is the header size)

            let mut schema = Schema::default();

            let mut col_i = 1;
            let mut previous_len: usize = 0;
            while !record_header.is_empty() {
                anyhow::ensure!(col_i < 6, "schema definition can have only 5 colums");

                let column_type =
                    super::schema::column_type(&mut record_header).context("get column type")?;

                let mut s = String::new();
                let mut num: u64 = 0;

                let val_len = match column_type {
                    ColumnType::Int(val_len) => {
                        let bytes_slice =
                            &cell[header_size as usize - 1 + previous_len..][..val_len as usize];
                        let mut bytes = [0; 8];
                        bytes[8 - (val_len as usize)..].copy_from_slice(bytes_slice);
                        num = u64::from_be_bytes(bytes);
                        val_len
                    }
                    ColumnType::Text(val_len) => {
                        s = String::from_utf8(
                            cell[header_size as usize - 1 + previous_len..][..val_len as usize]
                                .to_vec(),
                        )?;
                        val_len
                    }
                    _ => bail!("Invalid column type in schema definition"),
                };

                previous_len += val_len as usize;

                match col_i {
                    1 => schema.typ = s,
                    2 => schema.name = s,
                    3 => schema.tbl_name = s,
                    4 => schema.rootpage = num,
                    5 => schema.sql = s,
                    _ => {}
                }

                col_i += 1;
            }

            db_info.schemas.push(schema);

            previous_offset = offset + cell_size as u16;
        }

        Ok((db_info, db_file))
    }

    pub(crate) fn schemas(&self, schema_type: SchemaType) -> Vec<&Schema> {
        self.schemas
            .iter()
            .filter(|&s| {
                SchemaType::from_str(&s.typ)
                    .map_err(|e| eprintln!("{e:?}"))
                    .ok()
                    == Some(schema_type)
            })
            .collect()
    }
}
