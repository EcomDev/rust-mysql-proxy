
#[derive(Debug,PartialEq)]
pub (crate) enum Value {
    Null,
    Bytes(Vec<u32>),
    UnsignedInt(u64),
    Int(i64),
    Float(f32),
    Double(f64),
    DateTime(u16, u8, u8, u8, u8, u8, u32),
    DateInterval(bool, u32, u8, u8, u8, u32)
}

#[derive(Debug,PartialEq)]
pub (crate) enum TypeHint {
    UnsignedInt,
    Bytes,
    Int,
    Float,
    Double,
    DateTime,
    DateInterval
}

#[derive(Debug,PartialEq)]
pub (crate) struct Column {
    type_hint: TypeHint,
    name: String
}

#[derive(Debug,PartialEq)]
pub (crate) enum Command {
    Connect(String),
    Query(String),
    Prepare(String),
    Execute(u32, Vec<Value>),
    Close()
}

#[derive(Debug,PartialEq)]
pub (crate) enum Event {
    Connected {
        version: String,
        process_id: u32
    },
    PreparedStatement {
        statement_id: u32,
        parameter_count: u32
    },
    Command {
        affected_rows: u64,
        last_insert_id: u64
    },
    ResultSet(Vec<Column>),
    ResultRow(Vec<Value>),
    ResultEnd(),
    Error(String),
    Closed()
}

impl Event {
    pub (super) fn connected <T: AsRef<str>> (version: T, process_id: u32) -> Self {
        Self::Connected {
            version: String::from(version.as_ref()),
            process_id
        }
    }

    pub (super) fn error <T: AsRef<str>> (error_message: T) -> Self {
        Self::Error(String::from(error_message.as_ref()))
    }

    pub (super) fn prepared_statement <T: AsRef<str>> (statement_id: u32, parameter_count: u32) -> Self {
        Self::PreparedStatement {
            statement_id,
            parameter_count
        }
    }
}

impl Command
{
    pub (super) fn connect<T: AsRef<str>> (connection_url: T) -> Self {
        Self::Connect(String::from(connection_url.as_ref()))
    }

    pub (super) fn query<T: AsRef<str>> (query: T) -> Self {
        Self::Query(String::from(query.as_ref()))
    }

    pub (super) fn prepare<T: AsRef<str>> (query: T) -> Self {
        Self::Prepare(String::from(query.as_ref()))
    }

    pub (super) fn execute<T: AsRef<str>> (query: T) -> Self {
        Self::Prepare(String::from(query.as_ref()))
    }
}