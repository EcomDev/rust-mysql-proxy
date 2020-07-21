
#[derive(Debug,PartialEq)]
pub enum Value {
    Empty,
    Bytes(Vec<u32>),
    UnsignedInt(u64),
    Int(i64),
    Float(f32),
    Double(f64),
    DateTime(u16, u8, u8, u8, u8, u8, u32),
    DateInterval(bool, u32, u8, u8, u8, u32)
}

#[derive(Debug,PartialEq)]
pub enum TypeHint {
    UnsignedInt,
    Bytes,
    Int,
    Float,
    Double,
    DateTime,
    DateInterval
}

#[derive(Debug,PartialEq)]
pub struct Column {
    type_hint: TypeHint,
    name: String
}

#[derive(Debug,PartialEq)]
pub enum Command {
    Connect(String),
    Query(String),
    Prepare(String),
    Execute(u32, Vec<Value>),
    Close()
}

#[derive(Debug,PartialEq)]
pub enum Event {
    Connected(String, u32),
    AffectedRows(u32),
    ResultSet(Vec<Column>),
    ResultRow(Vec<Value>),
    ResultEnd(),
    Error(String),
    Closed()
}