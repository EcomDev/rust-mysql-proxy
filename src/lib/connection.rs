use crate::lib::messages::{Command, Event};
use crate::lib::mysql::ConnectionState;

use core::pin::Pin;
use core::task::Poll;


use futures_core::{Future, Stream, ready};
use tokio::stream::{self, StreamExt};
use futures_core::task::Context;
use pin_project_lite::*;

pin_project!{
    struct Connection <'a, St, Pr, Fut>
    {
        #[pin]
        commands: St,
        state: ConnectionState<'a>,
        command_processor: Pr,
        current_future: Option<Pin<Box<Fut>>>
    }
}

impl <'a, St, Pr, Fut> Connection<'a, St, Pr, Fut>
    where St: Stream<Item=Command>,
          Pr: Fn(Command, ConnectionState<'a>) -> Fut,
          Fut: Future<Output=(Event, ConnectionState<'a>)>
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

impl <'a, St, Pr, Fut> Stream for Connection<'a, St, Pr, Fut>
    where St: Stream<Item=Command>,
          Pr: Fn(Command, ConnectionState<'a>) -> Fut,
          Fut: Future<Output=(Event, ConnectionState<'a>)>
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
                            *this.state = state;
                            *this.current_future = None;

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

    async fn dummy_processor(command: Command, state: ConnectionState<'_>) -> (Event, ConnectionState<'_>) {
        match command {
            _ => (Event::Error(format!("{:?}", command)), state)
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
                Command::Close()
            ]),
            dummy_processor
        );

        assert_eq!(
            connection.collect::<Vec<_>>().await, vec![
                Event::error(r#"Connect("tcp://something")"#),
                Event::error(r#"Prepare("SELECT ?")"#),
                Event::error(r#"Close"#)
            ]
        );
    }

    async fn modify_state(command: Command, state: ConnectionState<'_>)
        -> (Event, ConnectionState<'_>) {
        (Event::error(format!("{:?}", command)), match state {
            ConnectionState::None => ConnectionState::Closed,
            ConnectionState::Closed => ConnectionState::None,
            _ => unreachable!()
        })
    }

    #[tokio::test]
    async fn it_modifies_state_with_updated_one() {

        let mut connection = Connection::new(
            stream::iter(vec![
                Command::Connect("tcp://something".into()),
                Command::Close(),
                Command::Connect("tcp://something".into()),
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
}