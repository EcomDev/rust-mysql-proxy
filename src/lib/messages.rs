#[derive(Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Bytes(Vec<u8>),
    UnsignedInt(u64),
    Int(i64),
    Float(f32),
    Double(f64),
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u32,
    },
    DateInterval {
        negative: bool,
        day: u32,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u32,
    },
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Bytes(value.as_bytes().to_vec())
    }
}

impl Value {
    pub(crate) fn null() -> Self {
        Value::Null
    }

    pub(crate) fn uint(value: u64) -> Self {
        Value::UnsignedInt(value)
    }

    pub(crate) fn int(value: i64) -> Self {
        Value::Int(value)
    }

    pub(crate) fn float(value: f32) -> Self {
        Value::Float(value)
    }

    pub(crate) fn double(value: f64) -> Self {
        Value::Double(value)
    }

    pub(crate) fn datetime(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u32,
    ) -> Self {
        Self::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
        }
    }

    pub(crate) fn date_interval(
        negative: bool,
        day: u32,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u32,
    ) -> Self {
        Self::DateInterval {
            negative,
            day,
            hour,
            minute,
            second,
            millisecond,
        }
    }

    pub(crate) fn bytes(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum TypeHint {
    Null,
    Bytes,
    Int,
    Float,
    Double,
    DateTime,
    DateInterval,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Column {
    type_hint: TypeHint,
    name: String,
}

impl Column {
    pub(crate) fn new<T: AsRef<str>>(name: T, type_hint: TypeHint) -> Self {
        Self {
            type_hint,
            name: String::from(name.as_ref()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Fetch,
    Connect(String),
    Query(String),
    Prepare(String),
    Execute(u32, Vec<Value>),
    Close,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ErrorType {
    Driver,
    Server,
    Io,
    Tls,
    Url,
    Other,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Event {
    Connected {
        version: String,
        process_id: u32,
    },
    PreparedStatement {
        statement_id: u32,
        parameter_count: u32,
    },
    Command {
        affected_rows: u64,
        last_insert_id: Option<u64>,
    },
    ResultSet(Vec<Column>),
    ResultRow(Vec<Value>),
    ResultEnd,
    Error(String, ErrorType),
    Closed,
}

impl Event {
    pub(super) fn connected<T: AsRef<str>>(version: T, process_id: u32) -> Self {
        Self::Connected {
            version: String::from(version.as_ref()),
            process_id,
        }
    }

    pub(super) fn other_error<T: AsRef<str>>(error_message: T) -> Self {
        Self::Error(String::from(error_message.as_ref()), ErrorType::Other)
    }

    pub(super) fn error<T: AsRef<str>>(error_message: T, error_type: ErrorType) -> Self {
        Self::Error(String::from(error_message.as_ref()), error_type)
    }

    pub(super) fn prepared_statement(statement_id: u32, parameter_count: u32) -> Self {
        Self::PreparedStatement {
            statement_id,
            parameter_count,
        }
    }

    pub(super) fn command(last_insert_id: Option<u64>, affected_rows: u64) -> Self {
        Self::Command {
            last_insert_id,
            affected_rows,
        }
    }

    pub(super) fn result_set(columns: Vec<Column>) -> Self {
        Self::ResultSet(columns)
    }

    pub(super) fn result_row(row: Vec<Value>) -> Self {
        Self::ResultRow(row)
    }

    pub(super) fn result_end() -> Self {
        Self::ResultEnd
    }

    pub(super) fn closed() -> Self {
        Self::Closed
    }
}

impl Command {
    pub(super) fn connect<T: AsRef<str>>(connection_url: T) -> Self {
        Self::Connect(String::from(connection_url.as_ref()))
    }

    pub(super) fn query<T: AsRef<str>>(query: T) -> Self {
        Self::Query(String::from(query.as_ref()))
    }

    pub(super) fn prepare<T: AsRef<str>>(query: T) -> Self {
        Self::Prepare(String::from(query.as_ref()))
    }

    pub(super) fn execute(statement_id: u32, parameters: Vec<Value>) -> Self {
        Self::Execute(statement_id, parameters)
    }

    pub(super) fn fetch() -> Self {
        Self::Fetch
    }

    pub(super) fn close() -> Self {
        Self::Close
    }
}
