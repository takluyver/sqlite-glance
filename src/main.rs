use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process;
use std::rc::Rc;

use anyhow;
use clap::{value_parser, Arg, Command};
use comfy_table;
use comfy_table::presets::UTF8_FULL;
use crossterm::tty::IsTty;
use rusqlite;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags};
use yansi::{Condition, Paint};

mod table;
use table::{get_table_names, get_view_names, Table};

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

    pager_proc
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())?;
    pager_proc.wait()?;
    Ok(())
}

/// Show sample rows from one SQLite table
/// Main implementation for `sqlite-glance file.db table`
fn inspect_table(
    db_table: Table,
    filename: &Path,
    where_clause: Option<&str>,
    limit: &u32,
) -> anyhow::Result<()> {
    let mut output = String::new();
    writeln!(
        output,
        "{}: {} {}",
        filename.display(),
        db_table.escaped_name().bright_green().bold(),
        db_table.obj_type()?
    )?;

    let mut stmt = db_table.conn.prepare(&format!(
        "SELECT * FROM {} {} LIMIT ?",
        db_table.escaped_name(),
        if let Some(w) = where_clause {
            format!("WHERE {}", w)
        } else {
            "".to_string()
        },
    ))?;
    let ncols = stmt.column_count();

    let mut table = comfy_table::Table::new();
    table.load_preset(UTF8_FULL).set_header(stmt.column_names());

    let mut rows = stmt.query([limit])?;
    let mut nrows: usize = 0;
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
        nrows += 1;
    }
    writeln!(output, "{}", table)?;
    if let Some(w) = where_clause {
        let nsel: u64 = db_table.conn.query_row(
            &format!(
                "SELECT count(*) from {} WHERE {}",
                db_table.escaped_name(),
                w
            ),
            [],
            |r| r.get(0),
        )?;
        writeln!(
            output,
            "{} of {} selected rows (of {} in table)",
            nrows,
            nsel,
            db_table.count_rows()?
        )?;
    } else {
        writeln!(output, "{} of {} rows", nrows, db_table.count_rows()?)?;
    }

    if std::io::stdout().is_tty() {
        // Crude way to figure out how much space the output takes
        let out_height = output.lines().count();
        // 2nd line (nth(1)) is top of table: ┌───┬─ ...
        let tbl_width = output.lines().nth(1).unwrap().chars().count();

        let (term_cols, term_rows) = crossterm::terminal::size()?;
        if (tbl_width > term_cols.into()) || (out_height > term_rows.into()) {
            show_in_pager(&output)?;
        } else {
            println!("{}", output);
        }
    } else {
        println!("{}", output);
    }

    Ok(())
}

fn inspect_schema(conn: Rc<Connection>, filename: &Path) -> anyhow::Result<()> {
    let table_names = get_table_names(&conn)?;
    println!(
        "{} — {} tables",
        filename.display().bold(),
        table_names.len()
    );
    println!();

    for tbl in table_names {
        let table = Table::new(&tbl, Rc::clone(&conn));

        let mut cols_unique = HashSet::new(); // Columns to label UNIQUE
        let mut cols_w_index = HashSet::new(); // 1-column indexes, not unique
        let mut pk_cols = Vec::new(); // Columns in the primary key
        let mut other_indexes = Vec::new(); // Indexes we'll list
        for ix in table.indexes_info()? {
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
        let foreign_keys = table.foreign_key_info()?;

        println!(
            "{} table ({} rows):",
            table.escaped_name().bright_green().bold(),
            nrows
        );

        // Columns info
        for col_info in table.columns_info()? {
            print!("  {}", col_info.name.cyan());
            if !col_info.dtype.is_empty() {
                print!(" {}", col_info.dtype);
            }
            if col_info.notnull {
                print!(" NOT NULL")
            }
            // Show primary key on column if it's a PK by itself.
            // pk_cols may be empty for integer PKs.
            if col_info.pk > 0 && pk_cols.len() < 1 {
                print!(" PRIMARY KEY");
            } else if cols_unique.contains(&col_info.name) {
                print!(" UNIQUE")
            } else if cols_w_index.contains(&col_info.name) {
                print!(" indexed")
            }
            // Show if column is a foreign key by itself
            if let Some(fk_info) = foreign_keys.for_name(&col_info.name) {
                print!(" REFERENCES {}", fk_info.to_table.bright_green());
                if &fk_info.to != &[""] {
                    print!(" ({})", fmt_col_names(&fk_info.to));
                }
            }
            println!();
        }
        if pk_cols.len() > 1 {
            println!("PRIMARY KEY ({})", fmt_col_names(&pk_cols));
        }

        for fk_info in foreign_keys.multicolumn() {
            println!(
                "FOREIGN KEY ({}) REFERENCES {} ({})",
                fmt_col_names(&fk_info.from),
                &fk_info.to_table.bright_green(),
                fmt_col_names(&fk_info.to)
            )
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

        println!(
            "{} view ({} rows):",
            view.escaped_name().bright_green().bold(),
            view.count_rows()?
        );

        for col_info in view.columns_info()? {
            println!("  {}", col_info.name.cyan());
        }
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("sqlite-glance")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::new("path")
                .required(true)
                .help("SQLite file to inspect")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("table")
                .required(false)
                .help("Table or view to inspect"),
        )
        .arg(
            Arg::new("where")
                .short('w')
                .long("where")
                .help("WHERE clause to select rows in table view"),
        )
        .arg(
            Arg::new("limit")
                .short('n')
                .long("limit")
                .default_value("12")
                .value_parser(value_parser!(u32))
                .help("Maximum number of rows to show in table view"),
        )
        .get_matches();

    yansi::whenever(Condition::TTY_AND_COLOR);

    let path = matches.get_one::<PathBuf>("path").unwrap();
    let filename = PathBuf::from(path.file_name().unwrap());
    let conn = Rc::new(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?);

    if let Some(table_name) = matches.get_one::<String>("table") {
        // Table/view name specified - show data
        let table = Table::new(table_name, Rc::clone(&conn));
        if !table.in_db()? {
            anyhow::bail!("No such table: {}", table_name);
        }
        let where_cl = matches.get_one::<String>("where").map(|x| x.as_str());
        let limit = matches.get_one::<u32>("limit").unwrap();
        inspect_table(table, &filename, where_cl, limit)
    } else {
        // No table specified - show DB schema
        inspect_schema(conn, &filename)
    }
}
