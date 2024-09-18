mod parser;

use anyhow::{bail, Context, Result};
use nom::branch::alt;

use crate::db::page::Page;
use parser::*;

use super::DB;

pub(crate) fn parse_command(sql: &str) -> Result<Command> {
    let parsed = match alt((parse_select, parse_create_table))(sql) {
        Ok((_, parsed)) => parsed,
        Err(err) => bail!("Error parsing SQL: {:?}", err),
    };

    if let ParsedCommand::Create(primary_key) = parsed.command {
        return Ok(Command::Create {
            columns: parsed.columns,
            table: parsed.table,
            primary_key,
        });
    }

    if parsed.columns.contains(&"count(*)".to_string()) {
        return Ok(Command::Count {
            table: parsed.table,
        });
    }

    Ok(Command::Select {
        columns: parsed.columns,
        table: parsed.table,
    })
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Create {
        columns: Vec<String>,
        table: String,
        primary_key: u16, // primary key column index
    },
    Select {
        columns: Vec<String>,
        table: String,
    },
    Count {
        table: String,
    },
}

impl Command {
    pub fn table(&self) -> &str {
        match &self {
            Command::Select { table, .. } => table,
            Command::Count { table } => table,
            Command::Create { table, .. } => table,
        }
    }

    pub fn execute(&self, db: &mut DB) -> Result<Vec<Vec<String>>> {
        let page = db
            .get_page(self.table())
            .with_context(|| format!("get page for table {}", self.table()))?;

        match self {
            Command::Select { columns, .. } => Ok(Self::select_columns(page, columns)?),
            Command::Count { .. } => Ok(Self::count(page)?),
            Command::Create { .. } => unimplemented!("CREATE TABLE command is not implemented"),
        }
    }

    fn select_columns(mut page: Page, column_names: &[String]) -> Result<Vec<Vec<String>>> {
        // SELECT name FROM apples"
        // SELECT id, name FROM apples"

        let mut col_indices = Vec::new();

        for col in column_names {
            let Some(col_index) = page.table_columns.iter().position(|c| c == col) else {
                bail!("Error: column '{col}' is not in the table")
            };
            col_indices.push(col_index as u16);
        }

        let mut result = Vec::new();

        for c in page.cells.iter_mut() {
            let mut row = Vec::new();
            for index in col_indices.iter() {
                let s = c.column(*index, page.primary_key_column)?;
                row.push(s);
            }
            result.push(row);
        }

        Ok(result)
    }

    fn count(page: Page) -> Result<Vec<Vec<String>>> {
        // "SELECT COUNT(*) FROM apples"
        Ok(vec![vec![page.n_cells.to_string()]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_count_uppercase() {
        let sql = "SELECT COUNT(*) FROM oranges";
        let c = parse_command(sql);
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn parse_count_lowercase() {
        let sql = "select count(*) from oranges";
        let c = parse_command(sql);
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn parse_select_column_uppercase() {
        let sql = "SELECT NAME FROM ORANGES";
        let c = parse_command(sql);
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["name".to_string()],
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn parse_select_column_lowercase() {
        let sql = "select name, color from apples";
        let c = parse_command(sql);
        assert!(c.is_ok());
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["name".to_string(), "color".to_string()],
                table: "apples".to_string()
            }
        );
    }
}
