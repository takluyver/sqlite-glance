use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::process;
use std::rc::Rc;

use anyhow;
use clap::{Arg, Command, value_parser};
use comfy_table::presets::UTF8_FULL;
use comfy_table;
use crossterm::tty::IsTty;
use rusqlite;
use rusqlite::{Connection, OpenFlags};
use rusqlite::types::Value;
use yansi::{Condition, Paint};

mod table;
use table::{
    Table,
    get_table_names,
    get_view_names,
};

fn fmt_col_names(names: &[String]) -> String {
    let mut res = String::new();
    let mut iter = names.iter();
    if let Some(s) = iter.next() {
        res.push_str(&format!("{}", s.cyan()));
        for s in iter {
            res.push_str(&format!(", {}", s.cyan()));
        }
    }
    res
}

fn show_in_pager(text: &str) -> std::io::Result<()> {
    let mut pager_proc = process::Command::new("less")
            .arg("-SR")
            .stdin(process::Stdio::piped())
            .spawn()?;

    pager_proc.stdin.take().unwrap().write_all(text.as_bytes())?;
    pager_proc.wait()?;
    Ok(())
}

fn inspect_table(conn: Rc<Connection>, table: &str) -> anyhow::Result<()> {
    let count: usize = conn.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name=?", [table], |r| r.get(0)
    )?;
    if count == 0 {
        anyhow::bail!("No such table: {}", table);
    }

    let mut stmt = conn.prepare(&format!("SELECT * FROM {} LIMIT 12", table))?;
    let ncols = stmt.column_count();

    let mut table = comfy_table::Table::new();
    table.load_preset(UTF8_FULL).set_header(stmt.column_names());

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let mut row_vec = Vec::new();
        for i in 0..ncols {
            let val: Value = row.get(i)?;
            row_vec.push(match val {
                Value::Null => "".to_string(),
                Value::Integer(i) => i.to_string(),
                Value::Real(f) => f.to_string(),
                Value::Text(s) => s,
                Value::Blob(_) => "<blob>".to_string(),
            });
        }
        table.add_row(row_vec);
    }
    if std::io::stdout().is_tty() {
        // Crude way to figure out how much space the table takes
        let table_s = format!("{}", table);
        let tbl_height = table_s.lines().count();
        let tbl_width = table_s.lines().nth(0).unwrap().chars().count();

        let (term_cols, term_rows) = crossterm::terminal::size()?;
        if (tbl_width > term_cols.into()) || (tbl_height > term_rows.into()) {
            show_in_pager(&table_s)?;
        } else {
            println!("{}", table_s);
        }
    } else {
        println!("{}", table);
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("sqlite-glance")
                    .version(env!("CARGO_PKG_VERSION"))
                    .arg(Arg::new("path")
                            .required(true)
                            .help("SQLite file to inspect")
                            .value_parser(value_parser!(PathBuf))
                        )
                    .arg(Arg::new("table")
                            .required(false)
                            .help("Table or view to inspect")
                        )
                    .get_matches();

    yansi::whenever(Condition::TTY_AND_COLOR);

    let path = matches.get_one::<PathBuf>("path").unwrap();
    let conn = Rc::new(Connection::open_with_flags(path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    )?);

    if let Some(table) = matches.get_one::<String>("table") {
        return inspect_table(conn,  table);
    }

    let filename = PathBuf::from(path.file_name().unwrap());
    let table_names = get_table_names(&conn)?;
    println!("{} — {} tables", filename.display().bold(), table_names.len());
    println!();

    for tbl in table_names {
        let table = Table::new(&tbl, Rc::clone(&conn));

        let mut cols_unique = HashSet::new();  // Columns to label UNIQUE
        let mut cols_w_index = HashSet::new(); // 1-column indices, not unique
        let mut pk_cols = Vec::new();         // Columns in the primary key
        let mut other_indexes = Vec::new();  // Indexes we'll list
        for ix in table.indices_info()? {
            let cols = ix.column_names(&conn)?;
            if ix.origin == "pk" {
                pk_cols = cols
            } else if cols.len() == 1 {
                if ix.unique {
                    cols_unique.insert(cols.get(0).unwrap().to_string());
                } else {
                    cols_w_index.insert(cols.get(0).unwrap().to_string());
                }
            } else {
                other_indexes.push((ix, cols))
            }
        }
        let nrows = table.count_rows()?;

        println!("{} table ({} rows):", tbl.bright_green().bold(), nrows);

        // Columns info
        for col_info in table.columns_info()? {
            print!("  {}", col_info.name.cyan());
            if !col_info.dtype.is_empty() {
                print!(" {}", col_info.dtype);
            }
            if col_info.notnull {
                print!(" NOT NULL")
            }
            if col_info.pk > 0 && pk_cols.len() == 1 {
                print!(" PRIMARY KEY");
            } else if cols_unique.contains(&col_info.name) {
                print!(" UNIQUE")
            } else if cols_w_index.contains(&col_info.name) {
                print!(" indexed")
            }
            println!();
        }
        if pk_cols.len() > 1 {
            println!("PRIMARY KEY ({})", fmt_col_names(&pk_cols));
        }
        
        if !other_indexes.is_empty() {
            println!("Indexes:");
            for (ix, cols) in other_indexes {
                print!("  {} ({})", ix.name, fmt_col_names(&cols));
                if ix.unique {
                    print!(" UNIQUE")
                }
                println!()
            }
        }
        println!();
    }

    // List views
    for name in get_view_names(&conn)? {
        // Views and tables are similar enough for this to work
        let view = Table::new(&name, Rc::clone(&conn));

        println!("{} view ({} rows):",
                 name.bright_green().bold(), view.count_rows()?);

        for col_info in view.columns_info()? {
            println!("  {}", col_info.name.cyan());
        }
    }
    
    Ok(())
}
