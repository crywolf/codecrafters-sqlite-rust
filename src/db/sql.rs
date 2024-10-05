mod parser;

use anyhow::{anyhow, bail, Context, Result};
use nom::branch::alt;

use super::pager::Tree;
use crate::db::DB;
use parser::*;

pub(crate) fn parse_command(sql: &str) -> Result<Command> {
    let parsed = match alt((parse_select, parse_create_table))(sql) {
        Ok((_, parsed)) => parsed,
        Err(err) => bail!("Error parsing SQL: {:?}", err),
    };

    let columns = parsed.columns;
    let table = parsed.table;

    let command = match parsed.command {
        ParsedCommand::Count => Command::Count {
            table,
            column: columns
                .first()
                .ok_or(anyhow!("Count command is missing column field"))?
                .to_string(),
        },
        ParsedCommand::Select => {
            let where_cond = if let Some(cond) = parsed.where_cond {
                Some(Condition {
                    column: cond.column,
                    value: cond.value,
                })
            } else {
                None
            };
            Command::Select {
                columns,
                table,
                where_cond,
            }
        }
        ParsedCommand::Create(pk) => Command::Create {
            columns,
            table,
            primary_key: pk,
        },
    };

    Ok(command)
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Create {
        columns: Vec<String>,
        table: String,
        /// primary key column index
        primary_key: u16,
    },
    Select {
        columns: Vec<String>,
        table: String,
        where_cond: Option<Condition>, // WHERE color = 'Yellow'
    },
    Count {
        column: String,
        table: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct Condition {
    column: String,
    value: String,
}

impl Command {
    pub fn table(&self) -> &str {
        match &self {
            Command::Select { table, .. } => table,
            Command::Count { table, .. } => table,
            Command::Create { table, .. } => table,
        }
    }

    pub fn execute(&self, db: &mut DB) -> Result<Vec<Vec<String>>> {
        match self {
            Command::Select {
                columns,
                where_cond,
                ..
            } => Ok(Self::select_columns(db, columns, where_cond, self.table())?),
            Command::Count { .. } => Ok(Self::count(db, self.table())?),
            Command::Create { .. } => unimplemented!("CREATE TABLE command is not implemented"),
        }
    }

    fn select_columns(
        db: &mut DB,
        column_names: &[String],
        cond: &Option<Condition>,
        table: &str,
    ) -> Result<Vec<Vec<String>>> {
        // SELECT name FROM apples"
        // SELECT id, name FROM apples"
        // SELECT * FROM apples"

        let table = db
            .table(table)
            .with_context(|| format!("get schema columns for table {}", table))?;

        let mut col_indices = Vec::new();

        if column_names.len() == 1 && column_names.contains(&"*".to_string()) {
            // SELECT * FROM ... (all columns)
            col_indices = table
                .columns
                .iter()
                .enumerate()
                .map(|(i, _)| i as u16)
                .collect();
        } else {
            // Select only specified columns
            for col in column_names {
                let Some(col_index) = table.columns.iter().position(|c| c == col) else {
                    bail!("Error: column '{col}' is not in the table")
                };
                col_indices.push(col_index as u16);
            }
        }

        let mut result = Vec::new();

        // WHERE condition
        let mut cond_column_index = None;
        let mut cond_column_value = String::new();
        if let Some(cond) = cond {
            if let Some(col_cond) = table.columns.iter().position(|c| *c == cond.column) {
                cond_column_index = Some(col_cond as u16);
                cond_column_value = cond.value.to_lowercase();
            }
        }

        let root_page = db.root_page_num(&table.name)?;
        let mut tree = Tree::new(&mut db.pager);

        for cell in tree.cells(root_page)? {
            if let Some(cond_column_index) = cond_column_index {
                let record_val = cell.column(cond_column_index, table.primary_key_column_index)?;
                if cond_column_value != record_val.to_lowercase() {
                    continue;
                }
            }

            let mut row = Vec::new();
            for index in col_indices.iter() {
                let s = cell.column(*index, table.primary_key_column_index)?;
                row.push(s);
            }
            result.push(row);
        }

        Ok(result)
    }

    fn count(db: &mut DB, table: &str) -> Result<Vec<Vec<String>>> {
        // "SELECT COUNT(*) FROM apples"

        // TODO where clause

        let table = db
            .table(table)
            .with_context(|| format!("get schema columns for table {}", table))?;

        let root_page = db.root_page_num(&table.name)?;
        let mut tree = Tree::new(&mut db.pager);

        Ok(vec![vec![tree.cells(root_page)?.len().to_string()]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_count_uppercase() {
        let sql = "SELECT COUNT(*) FROM oranges";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                column: "*".to_string(),
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn test_parse_count_lowercase() {
        let sql = "select count(*) from oranges";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                column: "*".to_string(),
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn test_parse_count_column() {
        let sql = "select count(name) from oranges";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                column: "name".to_string(),
                table: "oranges".to_string()
            }
        );
    }

    #[test]
    fn test_parse_select_column_uppercase() {
        let sql = "SELECT NAME FROM ORANGES";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["name".to_string()],
                table: "oranges".to_string(),
                where_cond: None,
            }
        );
    }

    #[test]
    fn test_parse_select_column_lowercase() {
        let sql = "select name, color from apples";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["name".to_string(), "color".to_string()],
                table: "apples".to_string(),
                where_cond: None,
            }
        );
    }

    #[test]
    fn test_parse_select_column_asterix() {
        let sql = "select * from apples";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["*".to_string()],
                table: "apples".to_string(),
                where_cond: None,
            }
        );
    }

    #[test]
    fn test_parse_select_column_with_where_condition() {
        let sql = "select name, color from apples where color = 'Yellow'";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Select {
                columns: vec!["name".to_string(), "color".to_string()],
                table: "apples".to_string(),
                where_cond: Some(Condition {
                    column: "color".to_string(),
                    value: "Yellow".to_string(),
                }),
            }
        );
    }
}
