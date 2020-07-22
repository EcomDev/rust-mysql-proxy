use mysql_async::{
    Conn, QueryResult, BinaryProtocol,
    TextProtocol, Statement, Error as MySQLError,
    IoError as MySQLIoError,
    Value as MySQLValue,
    prelude::*
};

use std::collections::HashMap;

use crate::lib::messages::{Event, Command, ErrorType, Value};
use std::fmt::Display;

const ERROR_CONNECTION_ESTABLISHED: &str = "Connection is already established, close previously open connection to processed.";

fn error_to_string(error: impl Display) -> String
{
    format!("{}", error)
}

fn map_error(error: MySQLError) -> Event
{
    match error {
        MySQLError::Server(server_error) => Event::error(
            error_to_string(server_error),
            ErrorType::Server
        ),
        MySQLError::Io(MySQLIoError::Io(io_error)) => Event::error(
            error_to_string(io_error),
            ErrorType::Io
        ),
        MySQLError::Other(value) => Event::other_error(error_to_string(value)),
        MySQLError::Driver(driver_error) => Event::error(
            error_to_string(driver_error),
            ErrorType::Driver
        ),
        MySQLError::Url(url_error) => Event::error(
            error_to_string(url_error),
            ErrorType::Url
        ),
        MySQLError::Io(MySQLIoError::Tls(tls_error)) => Event::error(
            error_to_string(tls_error),
            ErrorType::Tls
        )
    }
}

pub (crate) struct MySQLConnection {
    inner: Conn,
    statement_cache: HashMap<u32, Statement>
}

impl MySQLConnection {
    fn new(connection: Conn) -> Self {
        MySQLConnection {
            inner: connection,
            statement_cache: HashMap::new()
        }
    }
}

pub (crate) enum ConnectionState <'a>
{
    None,
    Connected(MySQLConnection),
    BinaryResult(MySQLConnection, QueryResult<'a, 'a, BinaryProtocol>),
    TextResult(MySQLConnection, QueryResult<'a, 'a, TextProtocol>),
    Closed
}

impl ConnectionState <'_> {
    fn connected(connection: MySQLConnection) -> Self {
        Self::Connected(connection)
    }
}

impl <'a> Default for ConnectionState<'a>
{
    fn default() -> Self {
        ConnectionState::None
    }
}

pub (crate) async fn process_command(command: Command, state: ConnectionState<'_>) -> (Event, ConnectionState<'_>) {
    match (command, state) {
        (Command::Connect(url), ConnectionState::None) => {
            connect_to_mysql_server(url).await
        },
        (Command::Connect(url), ConnectionState::Closed) => {
            connect_to_mysql_server(url).await
        },
        (Command::Connect(_), state) => {
            (Event::other_error(ERROR_CONNECTION_ESTABLISHED), state)
        },
        (Command::Prepare(query), ConnectionState::Connected(connection)) => {
            prepare_statement(query, connection).await
        },
        (Command::Execute(statement_id, params), ConnectionState::Connected(connection)) => {
            execute_statement(statement_id, params, connection).await
        }
        _ => unimplemented!()
    }
}

async fn prepare_statement<'a>(query: String, mut connection: MySQLConnection) -> (Event, ConnectionState<'a>) {
    let statement_result = connection.inner.prep(query).await;
    match statement_result {
        Ok(statement) => {
            let event = Event::prepared_statement(
                statement.id(),
                statement.num_params().into()
            );

            connection.statement_cache.insert(statement.id(), statement);
            (event, ConnectionState::Connected(connection))
        },
        Err(error) => (
            map_error(error),
            ConnectionState::Connected(connection)
        )
    }
}

async fn execute_statement<'a>(
    statement_id: u32,
    params: Vec<Value>,
    mut connection: MySQLConnection
) -> (Event, ConnectionState<'a>) {
    let statement = connection.statement_cache.get(&statement_id).unwrap();

    let result = connection.inner.exec_iter(
        statement,
        vec![MySQLValue::NULL]
    ).await.unwrap();

    (
        Event::command(result.last_insert_id(), result.affected_rows()),
        ConnectionState::Connected(connection)
    )
}

async fn connect_to_mysql_server<'a>(url: String) -> (Event, ConnectionState<'a>) {
    match Conn::from_url(url).await {
        Ok(connection) => {
            let version = connection.server_version();
            (
                Event::connected(format!("{}.{}.{}", version.0, version.1, version.2), connection.id()),
                ConnectionState::connected(MySQLConnection::new(connection))
            )
        },
        Err(error) => (map_error(error), ConnectionState::default())
    }
}

#[cfg(test)]
mod error_mapper_tests {
    use super::*;
    use mysql_async::{ServerError, IoError, DriverError, UrlError};
    use crate::lib::messages::ErrorType;
    use std::io;

    #[test]
    fn it_does_export_server_error() {
        assert_eq!(
            map_error(MySQLError::Server(ServerError {
                code: 99,
                message: "Some error".into(),
                state: "some state data".into()
            })),
            Event::error("ERROR some state data (99): Some error", ErrorType::Server)
        );
    }

    #[test]
    fn it_does_export_io_simple_error() {
        assert_eq!(
            map_error(
                MySQLError::Io(
                    IoError::Io(io::Error::new(io::ErrorKind::Other, "Some error"))
                )
            ),
            Event::error("Some error", ErrorType::Io)
        )
    }

    #[test]
    fn it_does_export_other_error_type() {
        assert_eq!(
            map_error(
                MySQLError::Other(
                    "Some value".into()
                )
            ),
            Event::error("Some value", ErrorType::Other)
        )
    }

    #[test]
    fn it_does_export_driver_error()
    {
        assert_eq!(
            map_error(
                MySQLError::Driver(
                    DriverError::ConnectionClosed
                )
            ),
            Event::error(
                "Connection to the server is closed.",
                ErrorType::Driver
            )
        )
    }

    #[test]
    fn it_does_export_url_parsing_error()
    {
        assert_eq!(
            map_error(
                MySQLError::Url(
                    UrlError::Invalid
                )
            ),
            Event::error(
                "Invalid or incomplete connection URL",
                ErrorType::Url
            )
        )
    }
}

#[cfg(test)]
mod process_command_tests {
    use super::*;
    use futures_core::Future;
    use crate::lib::messages::Value;

    const SERVER_URL: &str = "mysql://root:tests@localhost/catalog";

    #[tokio::test]
    async fn it_reports_version_of_mysql_server() {
        let (event, _) = connect_to_database().await;

        assert!(
            matches!(
                event,
                Event::Connected{ version, process_id }
                    if version == "5.7.30" && process_id > 0
            )
        );
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
            connect_to_database_and_get_state().await
        ).await;

        assert_eq!(event, Event::other_error(ERROR_CONNECTION_ESTABLISHED))
    }

    #[tokio::test]
    async fn it_errors_out_if_connect_url_is_malformed() {
        let (event, _) = process_command(Command::connect("mysql//"), ConnectionState::default()).await;

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
        let (event, _) = process_command(Command::connect("mysql://localhost:12345/"), ConnectionState::default()).await;

        assert_eq!(
            event,
            Event::error(
                "Connection refused (os error 111)",
                ErrorType::Io
            )
        )
    }

    #[tokio::test]
    async fn it_connects_previously_closed_connection() {
        let (event, _) = process_command(
            Command::connect(SERVER_URL),
            ConnectionState::Closed
        ).await;

        assert!(
            matches!(
                event,
                Event::Connected{ version, process_id }
                    if version == "5.7.30" && process_id > 0
            )
        );
    }

    #[tokio::test]
    async fn it_prepares_statement_on_mysql_connection() {
        let (event, _) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await
        ).await;

        assert!(
            matches!(
                event,
                Event::PreparedStatement {
                    parameter_count,
                    statement_id
                }
                if parameter_count == 3
            )
        );
    }

    #[tokio::test]
    async fn it_keeps_connected_after_statement_is_prepared() {
        let (_, state) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await
        ).await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_adds_statement_to_cache() {
        let (_, state) = process_command(
            Command::prepare("SELECT ?,?,?"),
            connect_to_database_and_get_state().await
        ).await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_errors_out_if_statement_has_syntax_error() {
        let (event, _) = process_command(
            Command::prepare("SELECT none"),
            connect_to_database_and_get_state().await
        ).await;

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
            connect_to_database_and_get_state().await
        ).await.1;

        let state = process_command(
            Command::prepare("SELECT ? + ?"),
            state
        ).await.1;

        let state = process_command(
            Command::prepare("SELECT ? * ? * ?"),
            state
        ).await.1;

        let connection = match state {
            ConnectionState::Connected(connection) => connection,
            _ => unreachable!()
        };

        let mut statement_ids = connection.statement_cache
            .keys()
            .collect::<Vec<&u32>>();

        statement_ids.sort();

        let statement_params: Vec<u16> = statement_ids
                .iter()
                .map(|key| connection.statement_cache.get(key).unwrap().num_params())
                .collect();

        assert_eq!(statement_params, vec![3, 2, 3])
    }

    #[tokio::test]
    async fn it_executes_prepared_command_statement_for_appending_new_record() {
        let (statement_id, state) = prepare_statement(
            "INSERT INTO some_sequence (sequence_id) VALUES (?)"
        ).await;

        let (last_insert_id, affected_rows, _) = execute_statement(
            statement_id,
            vec![Value::Null],
            state
        ).await;

        assert!(last_insert_id.is_some());
        assert_eq!(affected_rows, 1);
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_where_there_is_no_last_insert_id() {
        let (statement_id, state) = prepare_statement(
            "SET @custom_variable=?"
        ).await;

        let (last_insert_id, affected_rows, _) = execute_statement(
            statement_id,
            vec![Value::Bytes(b"value!".to_vec())],
            state
        ).await;

        assert_eq!(last_insert_id, None);
        assert_eq!(affected_rows, 0);
    }

    #[tokio::test]
    async fn it_executes_prepared_statement_with_result() {
        let (statement_id, state) = prepare_statement(
            "SET @another_variable=?"
        ).await;

        let (_, _, state) = execute_statement(
            statement_id, vec![Value::Bytes(b"value!".to_vec())], state
        ).await;

        let (statement_id, state) = prepare_statement(
            "SELECT @another_variable as variable, ? + ? as sum, ? * ? as multiply"
        ).await;

        let (header_event, state) = process_command(
            Command::execute(
                statement_id,
                vec![
                    Value::UnsignedInt(2),
                    Value::Int(2),
                    Value::UnsignedInt(12),
                    Value::Float(0.5),
                ]
            ),
            state
        ).await;
    }

    async fn execute_statement(
        statement_id: u32,
        params: Vec<Value>,
        state: ConnectionState<'_>
    ) -> (Option<u64>, u64, ConnectionState<'_>)
    {
        let (event, state) = process_command(
            Command::execute(statement_id, params),
            state
        ).await;

        let (last_insert_id, affected_rows) = match event {
            Event::Command { last_insert_id, affected_rows } => (
                last_insert_id,
                affected_rows
            ),
            _ => unreachable!()
        };

        (last_insert_id, affected_rows, state)
    }

    async fn prepare_statement<T: AsRef<str>>(query: T) -> (u32, ConnectionState<'static>) {
        let (event, state) = process_command(
            Command::prepare(query),
            connect_to_database_and_get_state().await
        ).await;

        let statement_id = match event {
            Event::PreparedStatement { statement_id, parameter_count } => statement_id,
            _ => unreachable!("It should succeed with statement")
        };

        (statement_id, state)
    }

    fn connect_to_database() -> impl Future<Output=(Event, ConnectionState<'static>)> {
        process_command(Command::connect(SERVER_URL), ConnectionState::default())
    }

    async fn connect_to_database_and_get_state() -> ConnectionState<'static> {
        connect_to_database().await.1
    }
}