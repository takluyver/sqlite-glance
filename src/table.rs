use std::rc::Rc;

use rusqlite::{Connection, Result, Row, Rows};

mod keywords;
mod tests;

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
        let mut stmt =
            conn.prepare("SELECT cid, name from pragma_index_info(?) ORDER BY seqno ASC")?;
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

#[derive(Clone, Debug, PartialEq)]
pub struct ForeignKeyInfo {
    pub to_table: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub on_update: String,
    pub on_delete: String,
}

impl ForeignKeyInfo {
    fn new() -> ForeignKeyInfo {
        return ForeignKeyInfo {
            to_table: "".to_string(),
            from: Vec::new(),
            to: Vec::new(),
            on_update: "NO ACTION".to_string(),
            on_delete: "NO ACTION".to_string(),
        };
    }
}

pub struct ForeignKeys {
    pub list: Vec<ForeignKeyInfo>,
}

impl ForeignKeys {
    fn from_rows(mut rows: Rows) -> Result<ForeignKeys> {
        let mut l = Vec::new();
        let mut current_id = 0;
        let mut current = ForeignKeyInfo::new();
        while let Some(row) = rows.next()? {
            let id: i32 = row.get("id")?;
            if id != current_id {
                l.push(current);
                current = ForeignKeyInfo::new();
                current_id = id;
            }
            current.to_table = row.get("table")?;
            current.from.push(row.get("from")?);
            let to: Option<String> = row.get("to")?;
            current.to.push(to.unwrap_or("".to_string()));
            current.on_update = row.get("on_update")?;
            current.on_delete = row.get("on_delete")?;
        }
        if current.from.len() > 0 {
            l.push(current);
        }
        Ok(ForeignKeys { list: l })
    }

    pub fn for_name(&self, name: &str) -> Option<ForeignKeyInfo> {
        for fk in &self.list {
            if fk.from.len() == 1 && &fk.from[0] == name {
                return Some(fk.clone());
            }
        }
        None
    }

    pub fn multicolumn(&self) -> Vec<ForeignKeyInfo> {
        let mut res = Vec::new();
        for fk in &self.list {
            if fk.from.len() > 1 {
                res.push(fk.clone())
            }
        }
        res
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

    /// Does a table/view with this name exist in the database?
    pub fn in_db(&self) -> Result<bool> {
        let count: usize = self.conn.query_row(
            "SELECT count(*) FROM pragma_table_list WHERE name=?",
            [&self.name],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// 'table' or 'view'
    pub fn obj_type(&self) -> Result<String> {
        Ok(self.conn.query_row(
            "SELECT type FROM pragma_table_list WHERE name=?",
            [&self.name],
            |r| r.get(0),
        )?)
    }

    /// Get the CREATE TABLE / CREATE VIEW statement for this object
    pub fn create_sql(&self) -> Result<String> {
        Ok(self.conn.query_row(
            "SELECT sql from sqlite_schema WHERE name=?",
            [&self.name],
            |r| r.get(0),
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

    /// Get information about indexes on this table
    pub fn indexes_info(&self) -> Result<Vec<IndexInfo>> {
        let mut stmt = self.conn.prepare("SELECT * FROM pragma_index_list(?)")?;
        let rows = stmt.query_map([&self.name], |row| IndexInfo::from_row(row))?;
        let mut res = Vec::new();
        for result in rows {
            res.push(result?);
        }
        Ok(res)
    }

    pub fn foreign_key_info(&self) -> Result<ForeignKeys> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM pragma_foreign_key_list(?)")?;
        let rows = stmt.query([&self.name])?;
        ForeignKeys::from_rows(rows)
    }

    /// Quote the table name if needed to ensure it's a valid identifier
    pub fn escaped_name(&self) -> String {
        // SQLite actually allows $ and any non-ascii character in identifiers
        // without quoting, but this more restrictive rule is OK for now.
        // https://www.sqlite.org/draft/tokenreq.html
        if self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !keywords::is_keyword(&self.name)
        {
            self.name.clone()
        } else {
            format!("\"{}\"", &self.name.replace('"', "\"\""))
        }
    }

    // Building SQL queries with string formatting is not great, but we can't
    // give the table name as a parameter. Quoting the name *should* work.

    pub fn count_rows(&self) -> Result<u64> {
        self.conn.query_row(
            &format!("SELECT count(*) from {}", &self.escaped_name()),
            [],
            |r| r.get(0),
        )
    }
}

/// Get the names of all tables in the database
pub fn get_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    let mut table_names = Vec::new();
    for name_result in rows {
        table_names.push(name_result?);
    }
    Ok(table_names)
}

/// Get the names of all views in the database
pub fn get_view_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'view'")?;
    let mut rows = stmt.query([])?;
    let mut res = Vec::new();

    while let Some(row) = rows.next()? {
        res.push(row.get(0)?)
    }
    Ok(res)
}
