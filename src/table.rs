use std::rc::Rc;

use rusqlite::{Connection, Result, Row, Statement};

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

    pub fn column_names(&self, conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT cid, name from pragma_index_info(?) ORDER BY seqno ASC"
        )?;
        let mut rows = stmt.query([&self.name])?;
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
}

pub struct Table {
    pub name: String,
    pub conn: Rc<Connection>,
}

impl Table {
    pub fn new(name: &str, conn: Rc<Connection>) -> Table {
        Table {
            name: name.to_string(),
            conn: conn,
        }
    }

    pub fn in_db(&self) -> Result<bool> {
        let count: usize = self.conn.query_row(
            "SELECT count(*) FROM pragma_table_list WHERE name=?",
            [&self.name], |r| r.get(0)
        )?;
        Ok(count > 0)
    }

    /// 'table' or 'view'
    pub fn obj_type(&self) -> Result<String> {
        Ok(self.conn.query_row(
            "SELECT type FROM pragma_table_list WHERE name=?",
            [&self.name], |r| r.get(0)
        )?)
    }

    pub fn columns_info(&self) -> Result<Vec<ColumnInfo>> {
        let mut stmt = self.conn.prepare("SELECT * from pragma_table_xinfo(?)")?;
        let rows = stmt.query_map([&self.name], |row| ColumnInfo::from_row(row))?;
        let mut res = Vec::new();
        for info_result in rows {
            res.push(info_result?);
        }
        Ok(res)
    }

    pub fn indices_info(&self) -> Result<Vec<IndexInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM pragma_index_list(?)"
        )?;
        let rows = stmt.query_map([&self.name], |row| IndexInfo::from_row(row))?;
        let mut res = Vec::new();
        for result in rows {
            res.push(result?);
        }
        Ok(res)
    }

    pub fn escaped_name(&self) -> String {
        // SQLite actually allows $ and any non-ascii character in identifiers
        // without quoting, but this more restrictive rule is OK for now.
        // https://www.sqlite.org/draft/tokenreq.html
        if self.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            self.name.clone()
        } else {
            format!("\"{}\"", &self.name.replace('"', "\"\""))
        }
    }

    // Building SQL queries with string formatting is not great, but we can't
    // give the table name as a parameter. Quoting the name *should* work.

    pub fn count_rows(&self) -> Result<u64> {
        self.conn.query_row(
            &format!("SELECT count(*) from {}", &self.escaped_name()), [], |r| r.get(0)
        )
    }

    pub fn sample_query(&self) -> Result<Statement> {
        Ok(self.conn.prepare(&format!("SELECT * FROM {} LIMIT ?",
                                      self.escaped_name()))?)
    }
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


pub fn get_view_names(conn: &Connection)-> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'view'")?;
    let mut rows = stmt.query([])?; //, |row| (row.get(0)?, row.get(1)?));
    let mut res = Vec::new();

    while let Some(row) = rows.next()? {
        res.push(row.get(0)?)
    }
    Ok(res)
}
