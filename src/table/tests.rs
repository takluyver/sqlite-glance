#![cfg(test)]
use std::rc::Rc;

use super::Table;
use anyhow;
use rusqlite::Connection;

const SCHEMA: &str = r#"
CREATE TABLE t1 (
    a INT
);
CREATE UNIQUE INDEX t1_a ON t1 (a);
CREATE TABLE multi_pk (a, b, c, PRIMARY KEY (b, a));
-- Check we can use keywords as identifiers with "double quotes"
CREATE TABLE "select" (
    "CREATE" INTEGER PRIMARY KEY,
    a, b,
FOREIGN KEY (a, b) REFERENCES multi_pk (a, b));
-- Identifiers in "double quotes" can use any characters except Null.
CREATE TABLE "foo 
""bar" (a);
CREATE VIEW v1 (recip_a) AS SELECT (1/a) FROM t1 WHERE a != 0;
"#;

#[test]
fn lookup() -> anyhow::Result<()> {
    let conn = Rc::new(Connection::open_in_memory()?);
    conn.execute_batch(SCHEMA)?;

    let t1 = Table::new("t1", Rc::clone(&conn));
    assert_eq!(t1.in_db()?, true);
    assert_eq!(t1.obj_type()?, "table");

    let v1 = Table::new("v1", Rc::clone(&conn));
    assert_eq!(v1.in_db()?, true);
    assert_eq!(v1.obj_type()?, "view");

    assert_eq!(Table::new("nonesuch", Rc::clone(&conn)).in_db()?, false);
    Ok(())
}

#[test]
fn escape_name() -> anyhow::Result<()> {
    let conn = Rc::new(Connection::open_in_memory()?);
    conn.execute_batch(SCHEMA)?;

    let t1 = Table::new("t1", Rc::clone(&conn));
    assert_eq!(t1.escaped_name(), "t1");

    let t = Table::new("select", Rc::clone(&conn));
    assert_eq!(t.escaped_name(), "\"select\"");
    assert_eq!(t.count_rows()?, 0);

    let t = Table::new("foo \n\"bar", Rc::clone(&conn));
    assert!(t.in_db()?);
    assert_eq!(t.escaped_name(), "\"foo \n\"\"bar\"");
    assert_eq!(t.count_rows()?, 0);

    Ok(())
}

#[test]
fn foreign_keys() -> anyhow::Result<()> {
    let conn = Rc::new(Connection::open_in_memory()?);
    conn.execute_batch(SCHEMA)?;

    let fk = Table::new("select", Rc::clone(&conn)).foreign_key_info()?;
    assert_eq!(fk.for_name("a"), None);
    if let Some(fki) = fk.multicolumn().first() {
        assert_eq!(fki.to_table, "multi_pk");
        assert_eq!(fki.from, ["a", "b"]);
        assert_eq!(fki.to, ["a", "b"]);
    } else {
        unreachable!();
    }
    Ok(())
}

#[test]
fn index_cols() -> anyhow::Result<()> {
    let conn = Rc::new(Connection::open_in_memory()?);
    conn.execute_batch(SCHEMA)?;

    let iis = Table::new("t1", Rc::clone(&conn)).indexes_info()?;
    let ii = iis.first().unwrap();
    assert_eq!(ii.name, "t1_a");
    assert_eq!(ii.unique, true);
    assert_eq!(ii.column_names(&conn)?, ["a"]);
    Ok(())
}
