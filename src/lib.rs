use std::{marker::PhantomData};

use rusqlite::{Connection, CachedStatement};

pub trait Row: Sized {
    /// Returns a slice of the column names for this row.
    /// This method is primary used for building queries.
    fn columns<'a>() -> &'a[&'a str];

    /// Parses an instance of Self from an rusqlite row.
    fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<Self>;

    fn select_from(table_name: &str) -> SelectQuery<Self> {
        SelectQuery::<Self> { table_name, _phantom: PhantomData }
    }
}

pub struct SelectQuery<'a, T: Row> {
    table_name: &'a str,
    _phantom: PhantomData<T>
}

impl <'a, T: Row> SelectQuery<'a, T> {
    pub fn prepare_sql(&self) -> String {
        let columns = T::columns().join(", ");
        format!("SELECT {} FROM {}", columns, self.table_name);
    }

    pub fn prepare_statement<'conn>(&self, connection: &'conn Connection) -> Result<CachedStatement<'conn>, rusqlite::Error> {
        connection.prepare_cached(&self.prepare_sql())
    }

    pub fn execute(&self, connection: &Connection) -> _ {
        self.prepare_statement(connection)?.query_map([], f)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Row};

    #[test]
    fn select_rows() -> Result<(), rusqlite::Error> {
        // Setting up the struct and trait implementation
        struct TestRow {
            id: u64,
            name: String,
            optional: Option<String>
        }

        impl Row for TestRow {
            fn columns<'a>() -> &'a[&'a str] {
                &["id", "name", "optional"]
            }

            fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
                Ok(Self { 
                    id: row.get(0)?,
                    name: row.get(1)?,
                    optional: row.get(2)?
                })
            }
        }

        // Open the database and insert test data
        let connection = rusqlite::Connection::open_in_memory()?;
        connection.execute(r#"
            CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, optional TEXT);
            INSERT INTO test_table (id, name, optional) VALUES
                (0, "Orange", "Strawberry"),
                (1, "Apple", NULL),
                (2, "Peach", "Raspberry")"#, [])?;

        TestRow::select_from("test_table").build();

        Ok(())
        
    }
}
