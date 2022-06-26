use log::warn;
use rusqlite::{Statement, Params, MappedRows, Connection, CachedStatement};

pub trait Row: Sized {
    /// Returns a slice of the column names for this row.
    /// This method is primary used for building queries.
    fn columns<'a>() -> &'a[&'a str];

    /// Parses an instance of `Self` from an rusqlite row.
    fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<Self>;

    /// Returns an iterator of `Self` from an rusqlite prepared statement.
    /// It is expected that the prepared statement is a select query of somekind.
    fn from_statement<'stmt, P: Params>(statement: &'stmt mut Statement, params: P) -> rusqlite::Result<MappedRows<'stmt, fn(&rusqlite::Row) -> rusqlite::Result<Self>>> {
        check_columns(&statement, Self::columns());
        statement.query_map(params, Self::parse_row)
    }
}

fn check_columns(statement: &Statement, columns: &[& str]) {
    if statement.column_count() != columns.len() {
        warn!(target: "sealion_parsing_events", 
            "Column count mismatch. Expected {} columns, statement only selects {}",
            columns.len(),
            statement.column_count())
    }

    let mismatched_columns: Vec<String> = statement
        .column_names()
        .iter()
        .zip(columns)
        .filter_map(|(&a, &b)| { if a.eq_ignore_ascii_case(b) {
            Some(format!("{} != {}", a, b))
        } else {
            None
        }})
        .collect();
    
    if mismatched_columns.len() > 0 {
        warn!(target: "sealion_parsing_events",
            "Column name mismatch: {}",
            mismatched_columns.join(", "))
    }
}

pub struct SelectQuery {
    table_name: String,
}

impl SelectQuery {
    pub fn build_sql_string(&self, columns: &[&str]) -> String {
        format!("SELECT {} FROM {}", columns.join(", "), self.table_name)
    }

    pub fn prepare_statement_columns<'conn>(&self, connection: &'conn Connection, columns: &[&str]) -> rusqlite::Result<CachedStatement<'conn>> {
        connection.prepare_cached(&self.build_sql_string(columns))
    }

    pub fn prepare_statement<'conn, R: Row>(&self, connection: &'conn Connection) -> rusqlite::Result<CachedStatement<'conn>> {
        self.prepare_statement_columns(connection, R::columns())
    }

    pub fn execute<R: Row>(&self, connection: &Connection) -> rusqlite::Result<Vec<R>> {
        let mut statement = self.prepare_statement::<R>(connection)?;
        let rows_iterator = R::from_statement(&mut statement, [])?;
        rows_iterator.collect()
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{Row};

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

    fn setup_test_db() -> Result<Connection, rusqlite::Error> {
        let connection = rusqlite::Connection::open_in_memory()?;
        connection.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, optional TEXT)", [])?;
        let rows_modified = connection.execute(r#" INSERT INTO test_table (id, name, optional) VALUES
            (0, "Orange", "Strawberry"),
            (1, "Apple", NULL),
            (2, "Peach", "Raspberry")"#, [])?;
        assert_eq!(rows_modified, 3, "Test data has not been created properly.");
        Ok(connection)
    }

    #[test]
    fn select_low_level() -> Result<(), rusqlite::Error> {
        let connection = setup_test_db()?;
        
        let rows: Vec<rusqlite::Result<TestRow>> = 
            Row::from_statement(&mut connection.prepare("SELECT id, name, optional FROM test_table")?, [])?.collect();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].as_ref().unwrap().name, "Apple");

        Ok(())
        
    }
}
