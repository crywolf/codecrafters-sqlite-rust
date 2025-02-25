use anyhow::{bail, Context, Result};

#[derive(Debug, Default, Clone)]
pub(crate) struct Schema {
    /*
    CREATE TABLE sqlite_schema(
        type text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
      );
      */
    pub typ: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u64,
    pub sql: String,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum SchemaType {
    Table,
    Index,
    View,
    Trigger,
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Table => f.write_str("table"),
            SchemaType::Index => f.write_str("index"),
            SchemaType::View => f.write_str("view"),
            SchemaType::Trigger => f.write_str("trigger"),
        }
    }
}

impl SchemaType {
    pub fn from_str(s: &str) -> Result<SchemaType> {
        match s {
            "table" => Ok(SchemaType::Table),
            "index" => Ok(SchemaType::Index),
            "view" => Ok(SchemaType::View),
            "trigger" => Ok(SchemaType::Trigger),
            _ => bail!("unknown schema type"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ColumnType {
    Text(u64),
    Int(u64),
    Int0(u64),
    Int1(u64),
    Blob(u64),
    Float(u64),
    Null(u64),
}

impl ColumnType {
    pub(crate) fn column_bytes_lenght(&self) -> u64 {
        let &val_len = match self {
            ColumnType::Text(v) => v,
            ColumnType::Int(v) => v,
            ColumnType::Int0(v) => v,
            ColumnType::Int1(v) => v,
            ColumnType::Blob(v) => v,
            ColumnType::Float(v) => v,
            ColumnType::Null(v) => v,
        };

        val_len
    }
}
pub(crate) fn column_type<T>(mut v: T) -> Result<ColumnType>
where
    T: bytes::Buf + std::fmt::Debug,
{
    let v = varint(&mut v).with_context(|| format!("get int from varint {:?}", v))? as u64;
    let column_type = if v == 0 {
        ColumnType::Null(0)
    } else if v < 5 {
        ColumnType::Int(v)
    } else if v == 5 {
        ColumnType::Int(6)
    } else if v == 6 {
        ColumnType::Int(8)
    } else if v == 7 {
        ColumnType::Float(8)
    } else if v == 8 {
        ColumnType::Int0(0)
    } else if v == 9 {
        ColumnType::Int1(0)
    } else if v > 12 {
        if v % 2 == 0 {
            ColumnType::Blob((v - 12) / 2) // BLOB: v = (n*2) + 12 => n = (v - 12) / 2
        } else {
            ColumnType::Text((v - 13) / 2) // TEXT: v = (n*2) + 13 => n = (v - 13) / 2
        }
    } else {
        bail!("invalid column serial type: {}", v)
    };

    Ok(column_type)
}

pub(crate) fn varint<T>(mut buf: T) -> Result<i64>
where
    T: bytes::Buf,
{
    if buf.remaining() == 0 {
        bail!("buffer is empty")
    }

    let buf_len = buf.remaining();

    let mut b0 = buf.get_u8() as i64;
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

        let b1 = buf.get_u8() as i64;
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
