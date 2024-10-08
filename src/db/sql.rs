mod parser;

use anyhow::{anyhow, bail, Context, Result};
use nom::branch::alt;

use super::{
    pager::{self, Tree},
    schema::SchemaType,
};
use crate::db::DB;
use parser::*;

pub(crate) fn parse_command(sql: &str) -> Result<Command> {
    let parsed = match alt((parse_select, parse_create_table, parse_create_index))(sql) {
        Ok((_, parsed)) => parsed,
        Err(err) => bail!("Error parsing SQL: {:?}", err),
    };

    let columns = parsed.columns;
    let table = parsed.table;

    let where_cond = if let Some(cond) = parsed.where_cond {
        Some(Condition {
            column: cond.column,
            value: cond.value,
        })
    } else {
        None
    };

    let command = match parsed.command {
        ParsedCommand::Count => Command::Count {
            table,
            column: columns
                .first()
                .ok_or(anyhow!("Count command is missing column field"))?
                .to_string(),
            where_cond,
        },
        ParsedCommand::Select => Command::Select {
            columns,
            table,
            where_cond,
        },
        ParsedCommand::CreateTable(pk) => Command::CreateTable {
            columns,
            table,
            primary_key: pk,
        },
        ParsedCommand::CreateIndex => Command::CreateIndex { columns, table },
    };

    Ok(command)
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    CreateTable {
        columns: Vec<String>,
        table: String,
        /// primary key column index
        primary_key: u16,
    },
    CreateIndex {
        columns: Vec<String>,
        table: String,
    },
    Select {
        columns: Vec<String>,
        table: String,
        where_cond: Option<Condition>, // WHERE color = 'Yellow'
    },
    Count {
        column: String,
        table: String,
        where_cond: Option<Condition>, // WHERE color = 'Yellow'
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
            Command::CreateTable { table, .. } => table,
            Command::CreateIndex { table, .. } => table,
        }
    }

    pub fn execute(&self, db: &mut DB) -> Result<Vec<Vec<String>>> {
        match self {
            Command::Select {
                columns,
                where_cond,
                ..
            } => Ok(Self::select_columns(db, columns, where_cond, self.table())?),
            Command::Count { where_cond, .. } => Ok(Self::count(db, self.table(), where_cond)?),
            Command::CreateTable { .. } => {
                unimplemented!("CREATE TABLE command is not implemented")
            }
            Command::CreateIndex { .. } => {
                unimplemented!("CREATE INDEX command is not implemented")
            }
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

        let tbl_name = table;
        let table = db
            .table(tbl_name)
            .with_context(|| format!("get schema columns for table {}", tbl_name))?;

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
            for col_name in column_names {
                let Some(col_index) = table.columns.iter().position(|c| c == col_name) else {
                    bail!("Error: column '{col_name}' is not in the table")
                };
                col_indices.push(col_index as u16);
            }
        }

        // WHERE condition
        let mut filter = None;
        if let Some(cond) = cond {
            if let Some(cond_col_i) = table.columns.iter().position(|c| *c == cond.column) {
                let mut index_root_page = None;

                // get index schema if index exists
                let index = db.index_on_column(tbl_name, &cond.column);

                if let Some(index_schema) = index {
                    index_root_page = Some(index_schema.rootpage);
                }

                filter = Some(pager::CellFilter::new(
                    index_root_page,
                    cond_col_i as u16,
                    cond.value.to_lowercase(),
                    table.primary_key_column_index,
                ));
            }
        }

        let table_root_page = db.root_page_num(&table.name, SchemaType::Table)?;

        let mut tree = Tree::new(&mut db.pager);

        let mut result = Vec::new();
        for cell in tree.cells(table_root_page, filter)? {
            let mut row = Vec::new();
            for index in col_indices.iter() {
                let s = cell.column(*index, table.primary_key_column_index)?;
                row.push(s.to_string());
            }
            result.push(row);
        }

        Ok(result)
    }

    fn count(db: &mut DB, table: &str, cond: &Option<Condition>) -> Result<Vec<Vec<String>>> {
        // "SELECT COUNT(*) FROM apples"

        let tbl_name = table;
        let table = db
            .table(tbl_name)
            .with_context(|| format!("get schema columns for table {}", tbl_name))?;

        // WHERE condition
        let mut filter = None;
        if let Some(cond) = cond {
            if let Some(cond_col_i) = table.columns.iter().position(|c| *c == cond.column) {
                let mut index_root_page = None;

                // get index schema if index exists
                let index = db.index_on_column(tbl_name, &cond.column);

                if let Some(index_schema) = index {
                    index_root_page = Some(index_schema.rootpage);
                }

                filter = Some(pager::CellFilter::new(
                    index_root_page,
                    cond_col_i as u16,
                    cond.value.to_lowercase(),
                    table.primary_key_column_index,
                ));
            }
        }

        let table_root_page = db.root_page_num(&table.name, SchemaType::Table)?;
        let mut tree = Tree::new(&mut db.pager);

        Ok(vec![vec![tree
            .cells(table_root_page, filter)?
            .len()
            .to_string()]])
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
                table: "oranges".to_string(),
                where_cond: None,
            }
        );
    }

    #[test]
    fn test_parse_count_lowercase() {
        let sql = "select count(*) from oranges where color = 'Yellow'";
        let c = parse_command(sql);
        let c = c.unwrap();
        assert_eq!(
            c,
            Command::Count {
                column: "*".to_string(),
                table: "oranges".to_string(),
                where_cond: Some(Condition {
                    column: "color".to_string(),
                    value: "Yellow".to_string(),
                }),
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
                table: "oranges".to_string(),
                where_cond: None,
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

#[test]
fn test_parse_create_index() {
    let sql = "CREATE INDEX idx_companies_country\n\ton companies (country)";
    let c = parse_command(sql);
    let c = c.unwrap();
    assert_eq!(
        c,
        Command::CreateIndex {
            columns: vec!["country".to_string()],
            table: "companies".to_string()
        }
    );
}
