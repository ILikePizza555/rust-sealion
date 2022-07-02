use std::result;
use std::fmt::Write;
use thiserror::Error;
use log::warn;
use rusqlite::{Statement, Params, MappedRows, Connection, CachedStatement};

#[derive(Error, Debug)]
pub enum SealionError {
    #[error(transparent)]
    IoError(#[from] std::fmt::Error),
    #[error(transparent)]
    RusqliteError(#[from] rusqlite::Error)
}

type SealionResult<T> = result::Result<T, SealionError>;

pub trait Row: Sized {
    /// Returns a slice of the column names for this row.
    /// This method is primary used for building queries.
    fn columns<'a>() -> &'a[&'a str];

    /// Parses an instance of `Self` from an rusqlite row.
    fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<Self>;

    /// Returns an iterator of `Self` from an rusqlite prepared statement.
    /// It is expected that the prepared statement is a select query of somekind.
    fn from_statement<'stmt, P: Params>(statement: &'stmt mut Statement, params: P) -> SealionResult<MappedRows<'stmt, fn(&rusqlite::Row) -> rusqlite::Result<Self>>> {
        check_columns(&statement, Self::columns());
        statement.query_map(params, Self::parse_row as fn(&rusqlite::Row) -> rusqlite::Result<Self>)
            .map_err(|err| SealionError::RusqliteError(err))
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
    pub table_name: String,
    pub where_clause: Option<String>
}

impl SelectQuery {
    pub fn new<S: ToString>(table_name: S) -> Self {
        Self { 
            table_name: table_name.to_string(),
            where_clause: None
        }
    }

    pub fn r#where<S: ToString>(&mut self, where_clause: S) -> &mut Self {
        self.where_clause = Some(where_clause.to_string());
        self
    }

    pub fn build_sql_string(&self, columns: &[&str]) -> SealionResult<String> {
        let mut sql_string = format!("SELECT {} ", columns.join(", "));
        write!(sql_string, "FROM {} ", self.table_name)?;
        
        if let Some(where_string) = &self.where_clause {
            write!(sql_string, "WHERE {}", where_string)?;
        }

        Ok(sql_string)
    }

    pub fn prepare_statement_columns<'conn>(&self, connection: &'conn Connection, columns: &[&str]) -> SealionResult<CachedStatement<'conn>> {
        connection.prepare_cached(&self.build_sql_string(columns)?)
            .map_err(|err| SealionError::RusqliteError(err))
    }

    pub fn prepare_statement<'conn, R: Row>(&self, connection: &'conn Connection) -> SealionResult<CachedStatement<'conn>> {
        self.prepare_statement_columns(connection, R::columns())
    }

    pub fn execute<R: Row>(&self, connection: &Connection) -> SealionResult<Vec<R>> {
        let mut statement = self.prepare_statement::<R>(connection)?;
        let rows_iterator = R::from_statement(&mut statement, [])?;
        
        rows_iterator.collect::<rusqlite::Result<Vec<R>>>()
            .map_err(|err| SealionError::RusqliteError(err))
    }

    /// Similar to execute, but instead of failing-fast on collection, this method will instead iterate
    /// through all the rows, attempt to parse them, and return every error and result.
    pub fn execute_collect_errors<R: Row>(&self, connection: &Connection) -> SealionResult<(Vec<R>, Vec<SealionError>)> {
        let mut statement = self.prepare_statement::<R>(connection)?;
        
        let mut parsing_errors: Vec<SealionError> = Vec::new();
        let values: Vec<R> = R::from_statement(&mut statement, [])?
            .filter_map(|result| match result {
                Ok(row) => Some(row),
                Err(err) => {
                    parsing_errors.push(SealionError::RusqliteError(err));
                    None
                }
            })
            .collect();
        
        Ok((values, parsing_errors))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{Row, SelectQuery, SealionResult};

    #[derive(Debug, PartialEq, Eq)]
    struct TestRow {
        id: u64,
        name: String,
        optional: Option<String>
    }

    impl Row for TestRow {
        fn columns<'a>() -> &'a[ &'a str] {
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

    fn setup_test_db() -> SealionResult<Connection> {
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
    fn select_low_level() -> SealionResult<()> {
        let connection = setup_test_db()?;
        
        let rows: Vec<rusqlite::Result<TestRow>> = 
            Row::from_statement(&mut connection.prepare("SELECT id, name, optional FROM test_table")?, [])?.collect();
        assert_eq!(rows, vec![
            Ok(TestRow { id: 0, name: "Orange".to_string(), optional: Some("Strawberry".to_string()) }),
            Ok(TestRow { id: 1, name: "Apple".to_string(), optional: None }),
            Ok(TestRow { id: 2, name: "Peach".to_string(), optional: Some("Raspberry".to_string()) })
        ]);

        Ok(())
    }

    #[test]
    fn select_with_query() -> SealionResult<()> {
        let connection = setup_test_db()?;

        let rows: Vec<TestRow> = SelectQuery::new("test_table").execute(&connection)?;
        assert_eq!(rows, vec![
            TestRow { id: 0, name: "Orange".to_string(), optional: Some("Strawberry".to_string()) },
            TestRow { id: 1, name: "Apple".to_string(), optional: None },
            TestRow { id: 2, name: "Peach".to_string(), optional: Some("Raspberry".to_string()) }
        ]);

        Ok(())
    }
}
