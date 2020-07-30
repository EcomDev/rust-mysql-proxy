use mysql_async::{prelude::*, BinaryProtocol, Conn, QueryResult, Row, Statement, TextProtocol};

use std::collections::HashMap;

use super::errors::map_error;
use super::value_mapper::map_columns;
use super::value_mapper::map_mysql_values_to_values;
use super::value_mapper::map_values_to_mysql_values;
use crate::lib::messages::{Column, Command, ErrorType, Event, TypeHint, Value};

use std::marker::PhantomData;

const ERROR_CONNECTION_ESTABLISHED: &str =
    "Connection is already established, close previously open connection to proceed.";

pub(in crate::lib) struct MySQLConnection {
    inner: Conn,
    statement_cache: HashMap<u32, Statement>,
    current_result: Option<MySQLResult>,
}

impl MySQLConnection {
    fn new(connection: Conn) -> Self {
        MySQLConnection {
            inner: connection,
            statement_cache: HashMap::new(),
            current_result: None,
        }
    }

    fn has_result(&self) -> bool {
        self.current_result.is_some()
    }

    async fn execute_query<T: AsRef<str>>(mut self, query: T) -> (Event, Self) {
        let mut result = self.inner.query_iter(query.as_ref()).await.unwrap();

        if result.is_empty() {
            return (
                Event::command(result.last_insert_id(), result.affected_rows()),
                self,
            );
        }

        let row = result.next().await.unwrap().unwrap();

        let columns = row.columns();

        let values = row.unwrap();

        self.current_result = Some(MySQLResult::TextResult(MySQLQueryResult::new(
            Event::result_row(map_mysql_values_to_values(values)),
        )));

        return (Event::result_set(map_columns(columns)), self);
    }

    async fn execute_statement(mut self, statement_id: u32, params: Vec<Value>) -> (Event, Self) {
        let statement = self.statement_cache.get(&statement_id).unwrap();

        let mut result = self
            .inner
            .exec_iter(statement, map_values_to_mysql_values(params))
            .await
            .unwrap();

        if result.is_empty() {
            return (
                Event::command(result.last_insert_id(), result.affected_rows()),
                self,
            );
        }

        let row = result.next().await.unwrap().unwrap();

        let columns = row.columns();

        let values = row.unwrap();

        self.current_result = Some(MySQLResult::BinaryResult(MySQLQueryResult::new(
            Event::result_row(map_mysql_values_to_values(values)),
        )));

        return (Event::result_set(map_columns(columns)), self);
    }

    async fn fetch_result(mut self) -> (Event, Self) {
        let mut result = self.current_result.take().unwrap();

        if let Some(event) = result.take_next_event() {
            self.current_result = Some(result);

            return (event, self);
        }

        let row = result.fetch_row(&mut self.inner).await;

        match row {
            Some(row) => {
                self.current_result = Some(result);
                let row = row.unwrap();
                (Event::result_row(map_mysql_values_to_values(row)), self)
            }
            None => {
                if !result.is_fetched(&mut self.inner) {
                    let row = result.fetch_next_result_set(&mut self.inner).await;

                    let event = match row {
                        Some(row) => {
                            let columns = row.columns();

                            result.put_next_event(Event::result_row(map_mysql_values_to_values(
                                row.unwrap(),
                            )));

                            self.current_result = Some(result);

                            Event::result_set(map_columns(columns))
                        }
                        None => Event::result_end(),
                    };

                    return (event, self);
                }
                (Event::result_end(), self)
            }
        }
    }
}

pub(in crate::lib) enum MySQLResult {
    BinaryResult(MySQLQueryResult<BinaryProtocol>),
    TextResult(MySQLQueryResult<TextProtocol>),
}

impl MySQLResult {
    async fn fetch_row(&mut self, connection: &mut Conn) -> Option<Row> {
        match self {
            MySQLResult::BinaryResult(result) => result.fetch_row(connection).await,
            MySQLResult::TextResult(result) => result.fetch_row(connection).await,
        }
    }

    async fn fetch_next_result_set(&mut self, connection: &mut Conn) -> Option<Row> {
        match self {
            MySQLResult::BinaryResult(result) => result.fetch_next_result_set(connection).await,
            MySQLResult::TextResult(result) => result.fetch_next_result_set(connection).await,
        }
    }

    fn take_next_event(&mut self) -> Option<Event> {
        match self {
            MySQLResult::BinaryResult(result) => result.next_event.take(),
            MySQLResult::TextResult(result) => result.next_event.take(),
        }
    }

    fn put_next_event(&mut self, event: Event) {
        match self {
            MySQLResult::BinaryResult(result) => result.next_event.replace(event),
            MySQLResult::TextResult(result) => result.next_event.replace(event),
        };
    }

    fn is_fetched(&self, connection: &mut Conn) -> bool {
        match self {
            MySQLResult::BinaryResult(result) => result.is_fetched(connection),
            MySQLResult::TextResult(result) => result.is_fetched(connection),
        }
    }
}

pub(in crate::lib) struct MySQLQueryResult<Proto>
where
    Proto: Protocol,
{
    next_event: Option<Event>,
    proto: PhantomData<Proto>,
}

impl<'a, Proto> MySQLQueryResult<Proto>
where
    Proto: Protocol,
{
    pub(crate) fn new(event: Event) -> Self {
        Self {
            next_event: Some(event),
            proto: PhantomData,
        }
    }

    async fn fetch_row(&self, connection: &mut Conn) -> Option<Row> {
        let mut query_result: QueryResult<'_, '_, Proto> = QueryResult::new(connection);

        query_result.next().await.unwrap()
    }

    async fn fetch_next_result_set(&self, connection: &mut Conn) -> Option<Row> {
        let mut query_result: QueryResult<'_, '_, Proto> = QueryResult::new(connection);

        query_result.next().await.unwrap()
    }

    fn is_fetched(&self, connection: &mut Conn) -> bool {
        let query_result: QueryResult<'_, '_, Proto> = QueryResult::new(connection);

        query_result.is_empty()
    }
}

pub(in crate::lib) enum ConnectionState {
    None,
    Connected(MySQLConnection),
    Closed,
}

impl ConnectionState {
    fn connected(connection: MySQLConnection) -> Self {
        Self::Connected(connection)
    }

    fn has_result(&self) -> bool {
        match self {
            Self::Connected(connection) => connection.has_result(),
            _ => false,
        }
    }
}

impl Default for ConnectionState {
    fn default() -> Self {
        ConnectionState::None
    }
}

pub(in crate::lib) async fn process_command(
    command: Command,
    state: ConnectionState,
) -> (Event, ConnectionState) {
    match (command, state) {
        (Command::Connect(url), ConnectionState::None) => connect_to_mysql_server(url).await,
        (Command::Connect(url), ConnectionState::Closed) => connect_to_mysql_server(url).await,
        (Command::Connect(_), state) => (Event::other_error(ERROR_CONNECTION_ESTABLISHED), state),
        (Command::Prepare(query), ConnectionState::Connected(connection)) => {
            prepare_statement(query, connection).await
        }
        (Command::Execute(statement_id, params), ConnectionState::Connected(connection)) => {
            execute_statement(statement_id, params, connection).await
        }
        (Command::Fetch, ConnectionState::Connected(connection)) => fetch_result(connection).await,
        (Command::Query(query), ConnectionState::Connected(connection)) => {
            execute_query(query, connection).await
        }
        _ => unimplemented!(),
    }
}

async fn prepare_statement(
    query: String,
    mut connection: MySQLConnection,
) -> (Event, ConnectionState) {
    let statement_result = connection.inner.prep(query).await;
    match statement_result {
        Ok(statement) => {
            let event = Event::prepared_statement(statement.id(), statement.num_params().into());

            connection.statement_cache.insert(statement.id(), statement);
            (event, ConnectionState::Connected(connection))
        }
        Err(error) => (map_error(error), ConnectionState::Connected(connection)),
    }
}

async fn execute_statement(
    statement_id: u32,
    params: Vec<Value>,
    connection: MySQLConnection,
) -> (Event, ConnectionState) {
    let (event, connection) = connection.execute_statement(statement_id, params).await;
    return (event, ConnectionState::Connected(connection));
}

async fn execute_query<T: AsRef<str>>(
    query: T,
    connection: MySQLConnection,
) -> (Event, ConnectionState) {
    let (event, connection) = connection.execute_query(query).await;
    return (event, ConnectionState::Connected(connection));
}

async fn fetch_result(connection: MySQLConnection) -> (Event, ConnectionState) {
    let (event, connection) = connection.fetch_result().await;
    return (event, ConnectionState::Connected(connection));
}

async fn connect_to_mysql_server(url: String) -> (Event, ConnectionState) {
    match Conn::from_url(url).await {
        Ok(connection) => {
            let version = connection.server_version();
            (
                Event::connected(
                    format!("{}.{}.{}", version.0, version.1, version.2),
                    connection.id(),
                ),
                ConnectionState::connected(MySQLConnection::new(connection)),
            )
        }
        Err(error) => (map_error(error), ConnectionState::default()),
    }
}

#[cfg(test)]
mod process_command_tests {
    use super::*;

    const SERVER_URL: &str = "mysql://root:tests@localhost/catalog";

    #[tokio::test]
    async fn it_reports_version_of_mysql_server() {
        let (event, _) = connect_to_database().await;

        assert!(matches!(
            event,
            Event::Connected{ version, process_id }
                if version == "5.7.30" && process_id > 0
        ));
    }

    #[tokio::test]
    async fn it_returns_back_connected_state() {
        let (_, state) = connect_to_database().await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_errors_out_if_connect_requested_for_already_connected_item() {
        let (event, _) = process_command(
            Command::connect(SERVER_URL),
            connect_to_database_and_get_state().await,
        )
        .await;

        assert_eq!(event, Event::other_error(ERROR_CONNECTION_ESTABLISHED))
    }

    #[tokio::test]
    async fn it_errors_out_if_connect_url_is_malformed() {
        let (event, _) =
            process_command(Command::connect("mysql//"), ConnectionState::default()).await;

        assert_eq!(
            event,
            Event::error(
                "URL parse error: relative URL without a base",
                ErrorType::Url
            )
        )
    }

    #[tokio::test]
    async fn it_errors_out_if_connect_is_not_refused() {
        let (event, _) = process_command(
            Command::connect("mysql://localhost:12345/"),
            ConnectionState::default(),
        )
        .await;

        assert_eq!(
            event,
            Event::error("Connection refused (os error 111)", ErrorType::Io)
        )
    }

    #[tokio::test]
    async fn it_connects_previously_closed_connection() {
        let (event, _) =
            process_command(Command::connect(SERVER_URL), ConnectionState::Closed).await;

        assert!(matches!(
            event,
            Event::Connected{ version, process_id }
                if version == "5.7.30" && process_id > 0
        ));
    }

    #[tokio::test]
    async fn it_prepares_statement_on_mysql_connection() {
        let (event, _) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await,
        )
        .await;

        assert!(matches!(
            event,
            Event::PreparedStatement { parameter_count, .. } if parameter_count == 3
        ));
    }

    #[tokio::test]
    async fn it_keeps_connected_after_statement_is_prepared() {
        let (_, state) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await,
        )
        .await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_adds_statement_to_cache() {
        let (_, state) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await,
        )
        .await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_errors_out_if_statement_has_syntax_error() {
        let (event, _) = process_command(
            Command::prepare("SELECT none"),
            connect_to_database_and_get_state().await,
        )
        .await;

        assert_eq!(
            event,
            Event::error(
                "ERROR 42S22 (1054): Unknown column 'none' in 'field list'",
                ErrorType::Server
            )
        )
    }

    #[tokio::test]
    async fn it_stores_prepared_statement_in_cache() {
        let state = process_command(
            Command::prepare("SELECT ?, ?, ?"),
            connect_to_database_and_get_state().await,
        )
        .await
        .1;

        let state = process_command(Command::prepare("SELECT ? + ?"), state)
            .await
            .1;

        let state = process_command(Command::prepare("SELECT ? * ? * ?"), state)
            .await
            .1;

        let connection = match state {
            ConnectionState::Connected(connection) => connection,
            _ => unreachable!(),
        };

        let mut statement_ids = connection.statement_cache.keys().collect::<Vec<&u32>>();

        statement_ids.sort();

        let statement_params: Vec<u16> = statement_ids
            .iter()
            .map(|key| connection.statement_cache.get(key).unwrap().num_params())
            .collect();

        assert_eq!(statement_params, vec![3, 2, 3])
    }

    #[tokio::test]
    async fn it_executes_prepared_command_statement_for_appending_new_record() {
        let (statement_id, state) =
            prepare_statement("INSERT INTO some_sequence (sequence_id) VALUES (?)", None).await;

        let (last_insert_id, affected_rows, _) =
            execute_statement(statement_id, vec![Value::Null], state).await;

        assert!(last_insert_id.is_some());
        assert_eq!(affected_rows, 1);
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_where_there_is_no_last_insert_id() {
        let (statement_id, state) = prepare_statement("SET @custom_variable=?", None).await;

        let (last_insert_id, affected_rows, _) =
            execute_statement(statement_id, vec![Value::bytes(b"value!".to_vec())], state).await;

        assert_eq!(last_insert_id, None);
        assert_eq!(affected_rows, 0);
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_with_result() {
        let (statement_id, state) = prepare_statement("SET @another=?", None).await;

        let (_, _, state) =
            execute_statement(statement_id, vec![Value::bytes(b"value!".to_vec())], state).await;

        let (statement_id, state) = prepare_statement(
            "SELECT CAST(@another as CHAR) as variable, ? + ? as sum, ? * ? as multiply",
            Some(state),
        )
        .await;

        let events = fetch_statement(
            statement_id,
            vec![
                Value::uint(2),
                Value::int(2),
                Value::uint(12),
                Value::double(0.5),
            ],
            state,
        )
        .await;

        assert_eq!(
            events,
            vec![
                Event::result_set(vec![
                    Column::new("variable", TypeHint::Bytes),
                    Column::new("sum", TypeHint::Int),
                    Column::new("multiply", TypeHint::Double),
                ]),
                Event::result_row(vec![
                    Value::bytes(b"value!".to_vec()),
                    Value::uint(4),
                    Value::double(6.0)
                ]),
                Event::result_end()
            ]
        )
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_with_multiple_rows() {
        let (statement_id, state) =
            prepare_statement("SELECT sku, type, created_at FROM product", None).await;

        let events = fetch_statement(statement_id, vec![], state).await;

        assert_eq!(
            events,
            vec![
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("type", TypeHint::Bytes),
                    Column::new("created_at", TypeHint::DateTime),
                ]),
                Event::result_row(vec![
                    Value::from("SKU1"),
                    Value::from("simple"),
                    Value::datetime(2010, 01, 1, 0, 0, 0, 0),
                ]),
                Event::result_row(vec![
                    Value::from("SKU2"),
                    Value::from("simple"),
                    Value::datetime(2011, 01, 1, 10, 10, 10, 0),
                ]),
                Event::result_row(vec![
                    Value::from("SKU3"),
                    Value::from("simple"),
                    Value::datetime(2012, 01, 1, 20, 20, 20, 0),
                ]),
                Event::result_row(vec![
                    Value::from("SKU4"),
                    Value::from("configurable"),
                    Value::datetime(2013, 01, 1, 1, 10, 10, 0),
                ]),
                Event::result_end()
            ]
        )
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_with_multiple_result_sets() {
        let (statement_id, state) = prepare_statement("CALL all_product_data(?)", None).await;

        let events = fetch_statement(statement_id, vec![Value::from("SKU1")], state).await;

        assert_eq!(
            events,
            vec![
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("type", TypeHint::Bytes)
                ]),
                Event::result_row(vec![Value::from("SKU1"), Value::from("simple")]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Int),
                ]),
                Event::result_row(vec![Value::from("SKU1"), Value::int(1), Value::int(1)]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU1"),
                    Value::int(2),
                    Value::from("10.0000")
                ]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU1"),
                    Value::int(3),
                    Value::from("Name 1")
                ]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU1"),
                    Value::int(4),
                    Value::from("Description 1")
                ]),
                Event::result_end()
            ]
        )
    }

    #[tokio::test]
    async fn it_executes_query_with_multiple_result_sets() {
        let (event, state) = process_command(
            Command::query("CALL all_product_data('SKU2')"),
            connect_to_database_and_get_state().await,
        )
        .await;

        let events = fetch_all_results(state, vec![event]).await;

        assert_eq!(
            events,
            vec![
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("type", TypeHint::Bytes)
                ]),
                Event::result_row(vec![Value::from("SKU2"), Value::from("simple")]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Int),
                ]),
                Event::result_row(vec![
                    Value::from("SKU2"),
                    Value::from("1"),
                    Value::from("1"),
                ]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU2"),
                    Value::from("2"),
                    Value::from("5.0000")
                ]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU2"),
                    Value::from("3"),
                    Value::from("Name 2")
                ]),
                Event::result_set(vec![
                    Column::new("sku", TypeHint::Bytes),
                    Column::new("attribute_id", TypeHint::Int),
                    Column::new("value", TypeHint::Bytes),
                ]),
                Event::result_row(vec![
                    Value::from("SKU2"),
                    Value::from("4"),
                    Value::from("Description 2")
                ]),
                Event::result_end()
            ]
        )
    }

    #[tokio::test]
    async fn it_executes_query_with_auto_increment_value() {
        let event = process_command(
            Command::query("INSERT INTO some_sequence (sequence_id) VALUES (NULL)"),
            connect_to_database_and_get_state().await,
        )
        .await
        .0;

        assert!(matches!(event, Event::Command { affected_rows, ..} if affected_rows == 1))
    }

    async fn fetch_statement(
        statement_id: u32,
        params: Vec<Value>,
        state: ConnectionState,
    ) -> Vec<Event> {
        let mut events = vec![];

        let (event, state) = process_command(Command::execute(statement_id, params), state).await;

        events.push(event);

        fetch_all_results(state, events).await
    }

    async fn fetch_all_results(state: ConnectionState, mut events: Vec<Event>) -> Vec<Event> {
        let mut prev_state = state;

        loop {
            let state = std::mem::take(&mut prev_state);

            let pair = process_command(Command::fetch(), state).await;

            events.push(pair.0);

            if !pair.1.has_result() {
                break;
            }

            std::mem::replace(&mut prev_state, pair.1);
        }

        events
    }

    async fn execute_statement(
        statement_id: u32,
        params: Vec<Value>,
        state: ConnectionState,
    ) -> (Option<u64>, u64, ConnectionState) {
        let (event, state) = process_command(Command::execute(statement_id, params), state).await;

        let (last_insert_id, affected_rows) = match event {
            Event::Command {
                last_insert_id,
                affected_rows,
            } => (last_insert_id, affected_rows),
            _ => unreachable!(),
        };

        (last_insert_id, affected_rows, state)
    }

    async fn prepare_statement<T: AsRef<str>>(
        query: T,
        state: Option<ConnectionState>,
    ) -> (u32, ConnectionState) {
        let state = match state {
            Some(state) => state,
            None => connect_to_database_and_get_state().await,
        };

        let (event, state) = process_command(Command::prepare(query), state).await;

        let statement_id = match event {
            Event::PreparedStatement { statement_id, .. } => statement_id,
            _ => unreachable!("It should succeed with statement"),
        };

        (statement_id, state)
    }

    async fn connect_to_database() -> (Event, ConnectionState) {
        process_command(Command::connect(SERVER_URL), ConnectionState::default()).await
    }

    async fn connect_to_database_and_get_state() -> ConnectionState {
        connect_to_database().await.1
    }
}
