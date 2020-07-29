use crate::lib::messages::{Command, Event};
use crate::lib::mysql::ConnectionState;

use core::pin::Pin;
use core::task::Poll;


use futures_core::{Future, Stream, ready};

use futures_core::task::Context;
use pin_project_lite::*;

pin_project!{
    struct Connection <St, Pr, Fut>
    {
        #[pin]
        commands: St,
        state: ConnectionState,
        command_processor: Pr,
        current_future: Option<Pin<Box<Fut>>>
    }
}

impl < St, Pr, Fut> Connection< St, Pr, Fut>
    where St: Stream<Item=Command>,
          Pr: Fn(Command, ConnectionState) -> Fut,
          Fut: Future<Output=(Event, ConnectionState)>
{
    pub fn new(source: St, processor: Pr) -> Self {
        Connection {
            commands: source,
            state: ConnectionState::default(),
            command_processor: processor,
            current_future: None
        }
    }
}

impl <St, Pr, Fut> Stream for Connection<St, Pr, Fut>
    where St: Stream<Item=Command>,
          Pr: Fn(Command, ConnectionState) -> Fut,
          Fut: Future<Output=(Event, ConnectionState)>
{
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match &mut this.current_future {
                Some(future) => {
                    match future.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready((event, state)) => {
                            let mut retry_fetch = |state: ConnectionState| {
                                let processor = (this.command_processor)(
                                    Command::fetch(),
                                    state
                                );

                                *this.current_future = Some(Box::pin(processor));
                            };

                            match &event {
                                Event::ResultSet(_) => retry_fetch(state),
                                Event::ResultRow(_) => retry_fetch(state),
                                _ => {
                                    *this.state = state;
                                    *this.current_future = None;
                                }
                            };

                            return Poll::Ready(Some(event))
                        },
                    }
                },
                None => {
                    let command = match ready!(this.commands.as_mut().poll_next(cx)) {
                        None => return Poll::Ready(None),
                        Some(command) => command
                    };

                    let processor = (this.command_processor)(
                        command,
                        std::mem::take(this.state)
                    );

                    *this.current_future = Some(Box::pin(processor));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests
{
    use super::*;
    use crate::lib::messages::{Column, TypeHint, Value};

    async fn dummy_processor(command: Command, state: ConnectionState) -> (Event, ConnectionState) {
        match command {
            _ => (Event::other_error(format!("{:?}", command)), state)
        }
    }

    #[tokio::test]
    async fn it_does_not_execute_any_commands_on_empty_stream()
    {
        let connection = Connection::new(stream::empty(), dummy_processor);
        assert_eq!(connection.collect::<Vec<_>>().await, vec![]);
    }

    #[tokio::test]
    async fn it_passes_state_and_commands_into_dummy_processor() {
        let connection = Connection::new(
            stream::iter(vec![
                Command::connect("tcp://something"),
                Command::prepare("SELECT ?"),
                Command::close()
            ]),
            dummy_processor
        );

        assert_eq!(
            connection.collect::<Vec<_>>().await, vec![
                Event::other_error(r#"Connect("tcp://something")"#),
                Event::other_error(r#"Prepare("SELECT ?")"#),
                Event::other_error(r#"Close"#)
            ]
        );
    }

    async fn modify_state(command: Command, state: ConnectionState)
        -> (Event, ConnectionState) {
        (Event::other_error(format!("{:?}", command)), match state {
            ConnectionState::None => ConnectionState::Closed,
            ConnectionState::Closed => ConnectionState::None,
            _ => unimplemented!()
        })
    }

    #[tokio::test]
    async fn it_modifies_state_with_updated_one() {

        let mut connection = Connection::new(
            stream::iter(vec![
                Command::connect("tcp://something"),
                Command::close(),
                Command::connect("tcp://something"),
            ]),
            modify_state
        );

        connection.next().await;
        connection.next().await;
        connection.next().await;

        assert!(matches!(
            connection.state, ConnectionState::Closed
        ));
    }

    async fn fake_result_set_processor(command: Command, state: ConnectionState)
        -> (Event, ConnectionState) {
        match (command, state) {
            (Command::Query(_), ConnectionState::None) => (
                Event::ResultSet(vec![Column::new("one", TypeHint::Int)]),
                ConnectionState::Closed
            ),
            (Command::Fetch, ConnectionState::Closed) => (
                Event::ResultRow(vec![Value::Int(1)]),
                ConnectionState::None
            ),
            (Command::Fetch, ConnectionState::None) => (
                Event::ResultEnd, ConnectionState::None
            ),
            _ => (Event::closed(), ConnectionState::Closed)
        }
    }

    #[tokio::test]
    async fn it_automatically_fetches_result_set() {
        let connection = Connection::new(
            stream::iter(vec![
                Command::query("SELECT 1 as one"),
                Command::close(),
            ]),
            fake_result_set_processor
        );

        assert_eq!(
            connection.collect::<Vec<_>>().await,
            vec![
                Event::result_set(
                    vec![Column::new("one", TypeHint::Int)]
                ),
                Event::result_row(
                    vec![Value::Int(1)]
                ),
                Event::result_end(),
                Event::closed()
            ]
        );
    }
}