use anyhow::{bail, Context, Result};

use codecrafters_sqlite::db::DB;

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
            let db = DB::new(&args[1]).context("open DB")?;

            println!("database page size:  {}", db.page_size());
            println!("reserved bytes:      {}", db.reserved_bytes());
            println!("database page count: {}", db.n_pages());
            println!("freelist page count: {}", db.n_freelist_pages());
            println!("schema format:       {}", db.schema_format());

            println!("text encoding:       {} (utf-8)", db.text_encoding());

            println!("number of tables:    {}", db.table_names().len());
            println!("number of indexes:   {}", db.index_names().len());
            println!("number of triggers:  {}", db.trigger_names().len());
            println!("number of views:     {}", db.view_names().len());
        }
        ".tables" => {
            let db = DB::new(&args[1]).context("open DB")?;

            for name in db.table_names() {
                print!("{}   ", name);
            }
            println!();
        }
        ".schema" => {
            let db = DB::new(&args[1]).context("open DB")?;

            for sql in db.schemas_sql() {
                println!("{};", sql);
            }
        }
        cmd => {
            let sql = cmd.trim();
            if sql.to_uppercase().starts_with(".") {
                bail!("Invalid command: {}!", sql);
            }
            if !sql.to_uppercase().starts_with("SELECT") {
                bail!("Invalid SQL command: {}; only SELECT is supported!", sql);
            }

            let db = DB::new(&args[1]).context("open DB")?;
            let res = db.execute(sql)?;
            println!("{}", res);
        }
    }

    Ok(())
}
