use anyhow::{bail, Context, Result};
use bytes::buf::BufMut;
use bytes::{Bytes, BytesMut};

use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;

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
        ".tables" => {
            let mut file = File::open(&args[1])?;

            // Database file header
            let mut file_header = [0; 100];
            file.read_exact(&mut file_header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([file_header[16], file_header[17]]);
            //println!("database page size: {}", page_size);

            // Bytes of unused "reserved" space at the end of each page. Usually 0.
            let _reserved_space = file_header[20];
            // println!("reserved page space: {}", reserved_space);

            let page_encoding = u32::from_be_bytes([
                file_header[56],
                file_header[57],
                file_header[58],
                file_header[59],
            ]);
            anyhow::ensure!(page_encoding == 1, "Page encoding is not UTF-8");

            let mut page_header = [0; 8];
            file.read_exact(&mut page_header)?;

            let page_type = page_header[0];
            anyhow::ensure!(
                page_type == 13, // A value of 13 (0x0d) means the page is a leaf table b-tree page.
                "First page shoud be a leaf table b-tree page"
            );

            // Start of the first freeblock on the page, or is zero if there are no freeblocks.
            let _freeblocks = u16::from_be_bytes([page_header[1], page_header[2]]);
            // println!("freeblocks: {}", freeblocks);

            // The number of cells on the page
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as usize;
            //println!("number of cells  (~ rows): {}", n_cells);

            // Start of the cell content area
            let _content_start = u16::from_be_bytes([page_header[5], page_header[6]]);
            //println!("start of the cell content area: {}", content_start);

            // Number of fragmented free bytes within the cell content area
            let _fragmented = page_header[7];
            // println!(
            //     "number of fragmented free bytes within the cell content area: {}",
            //     fragmented
            // );

            // The cell pointer array of a b-tree page immediately follows the b-tree page header.
            // Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
            // The cell pointer array consists of K 2-byte integer offsets to the cell contents.
            let mut cell_pointers = vec![0; n_cells * 2];
            file.read_exact(&mut cell_pointers)?;

            let mut schemas = Vec::new();

            let mut previous_offset = page_size;

            for i_cell in 0..n_cells {
                let offset =
                    u16::from_be_bytes([cell_pointers[i_cell * 2], cell_pointers[i_cell * 2 + 1]]);

                //let mut cell = vec![0; (previous_offset - offset) as usize];
                let cell_size = (previous_offset - offset) as usize;
                let mut cell = BytesMut::with_capacity(cell_size);
                cell.put_bytes(0, cell_size);

                file.read_exact_at(&mut cell, offset as u64)?;

                let _payload_size = cell[0]; // Size of the record
                let _row_id = cell[1]; // rowid
                let header_size = cell[2]; // Size of record header (varint)

                let mut record_header =
                    Bytes::copy_from_slice(&cell[3..3 + header_size as usize - 1]); // minus first byte (it is the header size)

                let mut schema = Schema::new_empty();

                let mut col_i = 1;
                let mut previous_len: usize = 0;
                while !record_header.is_empty() {
                    anyhow::ensure!(col_i < 6, "schema definition can have only 5 colums");

                    let (col_type, val_len) =
                        column_type(&mut record_header).context("get column type")?;
                    let mut s = String::new();
                    let mut num: u64 = 0;
                    if col_type == "int" {
                        // rootpage integer
                        num = cell[2 + header_size as usize + previous_len..][..val_len as usize][0]
                            as u64;
                    } else {
                        s = String::from_utf8(
                            cell[2 + header_size as usize + previous_len..][..val_len as usize]
                                .to_vec(),
                        )?;
                    }
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

                schemas.push(schema);

                previous_offset = offset;
            }

            for schema in schemas {
                print!("{}   ", schema.tbl_name);
            }
            println!();
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

#[derive(Debug, Default)]
struct Schema {
    /*
    CREATE TABLE sqlite_schema(
        type text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
      );
      */
    typ: String,
    name: String,
    tbl_name: String,
    rootpage: u64,
    sql: String,
}

impl Schema {
    fn new_empty() -> Self {
        Self::default()
    }
}

fn column_type<T>(mut v: T) -> Result<(String, u64)>
where
    T: bytes::Buf + std::fmt::Debug,
{
    let v = varint(&mut v).with_context(|| format!("get int from varint {:?}", v))?;
    let (col_type, len) = if v > 12 {
        if v % 2 == 0 {
            ("blob", (v - 12) / 2) // BLOB: v = (n*2) + 12 => n = (v - 12) / 2
        } else {
            ("text", (v - 13) / 2) // TEXT: v = (n*2) + 13 => n = (v - 13) / 2
        }
    } else if v < 7 {
        if v == 5 {
            ("int", 6)
        } else if v == 6 {
            ("int", 8)
        } else {
            ("int", v)
        }
    } else if v == 7 {
        ("float", 1)
    } else if v == 0 {
        ("null", 0)
    } else {
        bail!("invalid column serial type: {}", v)
    };

    Ok((col_type.to_string(), len))
}

fn varint<T>(mut buf: T) -> Result<u64>
where
    T: bytes::Buf,
{
    if buf.remaining() == 0 {
        bail!("buffer is empty")
    }

    let buf_len = buf.remaining();

    let mut b0 = buf.get_u8() as u64;
    let mut res = b0 & 0b0111_1111;
    let mut n_bytes = 1;

    while b0 & 0b1000_0000 != 0 && n_bytes <= 8 {
        // highest bit in first byte is one, get another byte

        if buf.remaining() == 0 {
            if buf_len >= 8 {
                bail!("invalid varint")
            }
            bail!("buffer is too short ({} bytes) or invalid varint", buf_len)
        }

        let b1 = buf.get_u8() as u64;
        if buf.remaining() == 0 && b1 & 0b1000_0000 != 0 {
            // last byte still starts with 1

            if buf_len >= 8 {
                bail!("invalid varint")
            }
            bail!("buffer is too short ({} bytes) or invalid varint", buf_len)
        }

        res <<= 7;
        res += b1 & 0b0111_1111;

        n_bytes += 1;

        b0 = b1;
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::varint;

    #[test]
    fn varint_empty_buf() {
        let mut buf = &[][..];
        assert_eq!(buf.len(), 0);
        let r = varint(&mut buf);
        assert!(r.is_err());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap_err().to_string(), "buffer is empty");
    }

    #[test]
    fn varint_1_byte() {
        let mut buf = &[0b01101000][..];
        assert_eq!(buf.len(), 1);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap(), 104);

        let mut buf = &[0b01101000, 0b01101000][..];
        assert_eq!(buf.len(), 2);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 1);
        assert_eq!(r.unwrap(), 104);
    }

    #[test]
    fn varint_2_bytes() {
        let mut buf: &[u8] = &[0x81, 0x47][..];
        assert_eq!(buf.len(), 2);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap(), 199);

        let mut buf = Bytes::from_static(&[0b10000111, 0b01101000]);
        assert_eq!(buf.len(), 2);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap(), 1000);
    }

    #[test]
    fn varint_3_bytes() {
        let mut buf: &[u8] = &[0x81, 0x47, 0x12][..];
        assert_eq!(buf.len(), 3);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 1);
        assert_eq!(r.unwrap(), 199);

        let mut buf = Bytes::from_static(&[0b10000111, 0b01101000, 0b01101000]);
        assert_eq!(buf.len(), 3);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 1);
        assert_eq!(r.unwrap(), 1000);

        let mut buf = Bytes::from_static(&[0b10000111, 0b11101000, 0b01101000]);
        assert_eq!(buf.len(), 3);
        let r = varint(&mut buf);
        assert!(r.is_ok());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap(), 128104);
    }

    #[test]
    fn varint_invalid() {
        let mut buf = Bytes::from_static(&[137, 137]);
        assert_eq!(buf.len(), 2);
        let r = varint(&mut buf);
        assert!(r.is_err());
        assert_eq!(buf.len(), 0);
        assert_eq!(
            r.unwrap_err().to_string(),
            "buffer is too short (2 bytes) or invalid varint"
        );

        let mut buf = Bytes::from_static(&[137, 137, 137, 137, 137, 137, 137, 137]);
        assert_eq!(buf.len(), 8);
        let r = varint(&mut buf);
        assert!(r.is_err());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap_err().to_string(), "invalid varint");

        let mut buf = Bytes::from_static(&[137, 137, 137, 137, 137, 137, 137, 137, 137]);
        assert_eq!(buf.len(), 9);
        let r = varint(&mut buf);
        assert!(r.is_err());
        assert_eq!(buf.len(), 0);
        assert_eq!(r.unwrap_err().to_string(), "invalid varint");
    }
}
