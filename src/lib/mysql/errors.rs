use crate::lib::messages::{ErrorType, Event};

use core::fmt::Display;
use mysql_async::{Error, IoError};

pub(super) fn map_error(error: Error) -> Event {
    match error {
        Error::Server(server_error) => {
            Event::error(error_to_string(server_error), ErrorType::Server)
        }
        Error::Io(IoError::Io(io_error)) => Event::error(error_to_string(io_error), ErrorType::Io),
        Error::Other(value) => Event::other_error(error_to_string(value)),
        Error::Driver(driver_error) => {
            Event::error(error_to_string(driver_error), ErrorType::Driver)
        }
        Error::Url(url_error) => Event::error(error_to_string(url_error), ErrorType::Url),
        Error::Io(IoError::Tls(tls_error)) => {
            Event::error(error_to_string(tls_error), ErrorType::Tls)
        }
    }
}

fn error_to_string(error: impl Display) -> String {
    format!("{}", error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::messages::ErrorType;
    use mysql_async::{DriverError, IoError, ServerError, UrlError};
    use std::io;

    #[test]
    fn it_does_export_server_error() {
        assert_eq!(
            map_error(Error::Server(ServerError {
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
            map_error(Error::Io(IoError::Io(io::Error::new(
                io::ErrorKind::Other,
                "Some error"
            )))),
            Event::error("Some error", ErrorType::Io)
        )
    }

    #[test]
    fn it_does_export_other_error_type() {
        assert_eq!(
            map_error(Error::Other("Some value".into())),
            Event::error("Some value", ErrorType::Other)
        )
    }

    #[test]
    fn it_does_export_driver_error() {
        assert_eq!(
            map_error(Error::Driver(DriverError::ConnectionClosed)),
            Event::error("Connection to the server is closed.", ErrorType::Driver)
        )
    }

    #[test]
    fn it_does_export_url_parsing_error() {
        assert_eq!(
            map_error(Error::Url(UrlError::Invalid)),
            Event::error("Invalid or incomplete connection URL", ErrorType::Url)
        )
    }
}
