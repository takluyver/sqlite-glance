## 0.6

- Triggers are now shown in schema view, just below the table or view they are
  attached to.
- The schema view indicates where tables are defined as STRICT or WITHOUT ROWID.

## 0.5

- Virtual tables are now marked as such in the schema view, along with the
  module they use (e.g. `fts5` for full text search).
- A new `--hidden` flag to show in the schema view:
  - Hidden columns in virtual tables (previously always shown)
  - Shadow tables, used internally by virtual tables (previously always shown)
  - System tables like `sqlite_schema` (previously never shown)

## 0.4.1

- Fix showing the PRIMARY KEY marker for non-integer primary keys.

## 0.4

2024-02-11

- Blob values in a table are now displayed as the first few bytes plus the blob
  size.
- The expression used to create generated columns is now shown in schema view.
