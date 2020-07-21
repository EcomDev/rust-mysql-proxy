use mysql_async::{Conn, QueryResult, BinaryProtocol, TextProtocol, Statement};
use std::collections::HashMap;
use crate::lib::messages::{Event, Command};

const ERROR_CONNECTION_ESTABLISHED: &str = "Connection is already established, close previously open connection to processed.";

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
        (Command::Connect(_), state) => {
            (Event::error(ERROR_CONNECTION_ESTABLISHED), state)
        },
        _ => unimplemented!()
    }
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
        _ => unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_core::Future;


    #[tokio::test]
    async fn it_reports_version_of_mysql_server() {
        let (event, _) = connect_to_database().await;

        assert!(matches!(event, Event::Connected{ version, process_id } if version == "5.7.30" && process_id > 0));
    }

    #[tokio::test]
    async fn it_returns_back_connected_state() {
        let (_, state) = connect_to_database().await;

        assert!(matches!(state, ConnectionState::Connected(_)));
    }

    #[tokio::test]
    async fn it_errors_out_if_connect_requested_for_already_connected_item() {
        let (_, state) = connect_to_database().await;
        let (event, _) = process_command(
            Command::connect("mysql://root:tests@localhost/"),
            state
        ).await;

        assert_eq!(
            event,
            Event::error(
                ERROR_CONNECTION_ESTABLISHED
            )
        )
    }

    fn connect_to_database() -> impl Future<Output=(Event, ConnectionState<'static>)> {
        process_command(
            Command::connect("mysql://root:tests@localhost/"),
            ConnectionState::None
        )
    }
}