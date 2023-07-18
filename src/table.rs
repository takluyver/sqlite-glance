use rusqlite::{Connection, Result, Row};

#[derive(Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: String,
    pub notnull: bool,
    pub pk: u8,
    pub hidden: u8,
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
pub struct IndexInfo {
    pub name: String,
    pub unique: bool,
    pub origin: String,
    pub partial: bool,
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

pub fn get_column_info(conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare("SELECT * from pragma_table_xinfo(?)")?;
    let rows = stmt.query_map([table_name], |row| ColumnInfo::from_row(row))?;
    let mut res = Vec::new();
    for info_result in rows {
        res.push(info_result?);
    }
    Ok(res)
}

pub fn count_rows(conn: &Connection, table_name: &str) -> Result<u64> {
    // Formatting SQL like this is bad in general, but we can't give the table
    // name as a parameter, and we get it from 
    conn.query_row(
        &format!("SELECT count(*) from {}", table_name), [], |r| r.get(0)
    )
}

pub fn get_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    let mut table_names = Vec::new();
    for name_result in rows {
        table_names.push(name_result?);
    }
    Ok(table_names)
}

pub fn get_table_indexes(conn: &Connection, table_name: &str) -> Result<Vec<IndexInfo>> {
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

pub fn get_index_columns(conn: &Connection, ix_name: &str) -> Result<Vec<String>> {
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
