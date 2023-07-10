use std::collections::HashSet;
use std::path::PathBuf;
use clap::{Arg, Command, value_parser};
use rusqlite::{Connection, Result, OpenFlags, Row};
use yansi::{Condition, Paint};

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    dtype: String,
    notnull: bool,
    pk: u8,
    hidden: u8,
}

impl ColumnInfo {
    fn from_row(row: &Row) -> Result<ColumnInfo> {
        Ok(ColumnInfo {
            name: row.get("name")?,
            dtype: row.get("type")?,
            notnull: row.get("notnull")?,
            pk: row.get("pk")?,
            hidden: row.get("hidden")?,
        })
    }
}

#[derive(Debug)]
struct IndexInfo {
    name: String,
    unique: bool,
    origin: String,
    partial: bool,
}

impl IndexInfo {
    fn from_row(row: &Row) -> Result<IndexInfo> {
        Ok(IndexInfo {
            name: row.get("name")?,
            unique: row.get("unique")?,
            origin: row.get("origin")?,
            partial: row.get("partial")?,
        })
    }
}

fn get_column_info(conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare("SELECT * from pragma_table_xinfo(?)")?;
    let rows = stmt.query_map([table_name], |row| ColumnInfo::from_row(row))?;
    let mut res = Vec::new();
    for info_result in rows {
        res.push(info_result?);
    }
    Ok(res)
}

fn count_rows(conn: &Connection, table_name: &str) -> Result<u64> {
    // Formatting SQL like this is bad in general, but we can't give the table
    // name as a parameter, and we get it from 
    conn.query_row(
        &format!("SELECT count(*) from {}", table_name), [], |r| r.get(0)
    )
}

fn get_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    let mut table_names = Vec::new();
    for name_result in rows {
        table_names.push(name_result?);
    }
    Ok(table_names)
}

fn get_table_indexes(conn: &Connection, table_name: &str) -> Result<Vec<IndexInfo>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM pragma_index_list(?)"
    )?;
    let rows = stmt.query_map([table_name], |row| IndexInfo::from_row(row))?;
    let mut res = Vec::new();
    for result in rows {
        res.push(result?);
    }
    Ok(res)
}

fn get_index_columns(conn: &Connection, ix_name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT cid, name from pragma_index_info(?) ORDER BY seqno ASC"
    )?;
    let mut rows = stmt.query([ix_name])?;
    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(match row.get("cid")? {
            -1 => "<rowid>".to_string(),
            -2 => "<expression>".to_string(),
            _ => row.get("name")?,
        })
    }
    Ok(res)
}

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
