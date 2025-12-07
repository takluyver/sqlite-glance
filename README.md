## sqlite-glance

A terminal interface to quickly see the contents of an SQLite database file.

Installation:

```
cargo install --locked sqlite-glance
```

Usage:

```
sqlite-glance my_db.sqlite

sqlite-glance my_db.sqlite table
```

With only one argument, it presents the database structure in a (hopefully)
readable format.
Given a table (or view) name, it will show the contents of the first few rows.
