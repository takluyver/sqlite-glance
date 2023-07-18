use std::collections::HashSet;
use std::path::PathBuf;
use std::rc::Rc;
use clap::{Arg, Command, value_parser};
use rusqlite::{Connection, Result, OpenFlags};
use yansi::{Condition, Paint};

mod table;
use table::{
    Table,
    get_table_names,
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

fn main() -> Result<()> {
    let matches = Command::new("sqlite-glance")
                    .version(env!("CARGO_PKG_VERSION"))
                    .arg(Arg::new("path")
                            .required(true)
                            .help("SQLite file to inspect")
                            .value_parser(value_parser!(PathBuf))
                        )
                    .get_matches();

    yansi::whenever(Condition::TTY_AND_COLOR);

    let path = matches.get_one::<PathBuf>("path").unwrap();
    let conn = Rc::new(Connection::open_with_flags(path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    )?);

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
    
    Ok(())
}
