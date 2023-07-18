use std::collections::HashSet;
use std::path::PathBuf;
use clap::{Arg, Command, value_parser};
use rusqlite::{Connection, Result, OpenFlags};
use yansi::{Condition, Paint};

mod table;
use table::{
    get_table_names,
    get_table_indexes,
    get_column_info,
    get_index_columns,
    count_rows,
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
    let conn = Connection::open_with_flags(path, 
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    )?;

    let filename = PathBuf::from(path.file_name().unwrap());
    let table_names = get_table_names(&conn)?;
    println!("{} — {} tables", filename.display().bold(), table_names.len());
    println!();

    for tbl in table_names {
        let mut cols_unique = HashSet::new();  // Columns to label UNIQUE
        let mut pk_cols = Vec::new();         // Columns in the primary key
        let mut other_indexes = Vec::new();  // Indexes we'll list
        for ix in get_table_indexes(&conn, &tbl)? {
            let cols = get_index_columns(&conn, &ix.name)?;
            if ix.origin == "pk" {
                pk_cols = cols
            } else if ix.unique && cols.len() == 1 {
                cols_unique.insert(cols.get(0).unwrap().to_string());
            } else {
                other_indexes.push((ix, cols))
            }
        }
        let nrows = count_rows(&conn, &tbl)?;

        println!("{} table ({} rows):", tbl.bright_green().bold(), nrows);

        // Columns info
        for col_info in get_column_info(&conn, &tbl)? {
            print!("  {}", col_info.name.cyan());
            if !col_info.dtype.is_empty() {
                print!(" {}", col_info.dtype);
            }
            if col_info.notnull {
                print!(" NOT NULL")
            }
            if col_info.pk > 0 && pk_cols.len() == 1 {
                print!(" PRIMARY KEY");
            }
            if cols_unique.contains(&col_info.name) {
                print!(" UNIQUE")
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
