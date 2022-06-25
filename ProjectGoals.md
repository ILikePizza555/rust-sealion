Eventually I want to do something like:

```rust
#[derive(Row)]
struct MyData {
  item_history_id: u64,
  last_modified: DateTime<Utc>,
  uuid: Uuid,
  name: String,
  quantity: u64,
  status: Option<String>
}
let mapped_rows = MyData::select_from("current_items").where("deleted = 0").execute(db_connection);
```

# API Designs

```rust
trait Row {
    //fn columns();
    //fn parse_row();
    fn select_from(table_name) -> Query;
}

trait Query<R: Row> {
    fn execute(connection) -> Result<Row>
}

Row::select_from("table").execute(conn);
//or
Query::<Row>::new("table").execute(conn);

```

Advantages:
- Easy invocation syntax: [Datatype]::[query].execute();
- Both `Row` and `Query` decoupled from `Connection`

Disadvantages
- `Query::execute` needs to call `Row::columns()` and `Row::parse_row()`, which means that Query has to have an associated type with Row.

---

```rust
trait Row {
    ...
    fn from(statement) -> Result<Iter<Self>>
}

trait Query {
    fn prepare_statement(connection) -> PreparedStatement;
}

Row::from(Query::select_from("table").prepare_statement(connection));
```

Advantages:
- `Query` and `Row` are completely decoupled.
- Use rusqlite types as the intermediary, therefore the user is not required to use `Query` if they don't want too.
- Runtime checks on the prepared statement can be done in `Row::from`

Disadvantages:
- This api style is slightly more obtuse than the previous.
    - Might be able to be fixed by adding generic helper functions on `Query` or `Row`