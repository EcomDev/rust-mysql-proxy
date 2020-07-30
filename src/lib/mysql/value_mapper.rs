use crate::lib::messages::{Column, TypeHint, Value};
use mysql_async::{consts::ColumnType, Column as MySQLColumn, Value as MySQLValue};
use std::sync::Arc;

pub(super) fn map_values_to_mysql_values(values: Vec<Value>) -> Vec<MySQLValue> {
    values.into_iter().map(|value| value.into()).collect()
}

pub(super) fn map_mysql_values_to_values(values: Vec<MySQLValue>) -> Vec<Value> {
    values.into_iter().map(|value| value.into()).collect()
}

pub(super) fn map_columns(columns: Arc<[MySQLColumn]>) -> Vec<Column> {
    columns
        .as_ref()
        .iter()
        .map(|column| Column::new(column.name_str(), map_column_type(column.column_type())))
        .collect()
}

fn map_column_type(column_type: ColumnType) -> TypeHint {
    match column_type {
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_NEWDECIMAL
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_JSON => TypeHint::Bytes,
        ColumnType::MYSQL_TYPE_TINY => TypeHint::Int,
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => TypeHint::Int,
        ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => TypeHint::Int,
        ColumnType::MYSQL_TYPE_LONGLONG => TypeHint::Int,
        ColumnType::MYSQL_TYPE_FLOAT => TypeHint::Float,
        ColumnType::MYSQL_TYPE_DOUBLE => TypeHint::Double,
        ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_DATETIME => TypeHint::DateTime,
        ColumnType::MYSQL_TYPE_TIME => TypeHint::DateInterval,
        ColumnType::MYSQL_TYPE_NULL => TypeHint::Null,
        _ => TypeHint::Bytes,
    }
}

impl Into<MySQLValue> for Value {
    fn into(self) -> MySQLValue {
        match self {
            Value::Int(value) => MySQLValue::from(value),
            Value::UnsignedInt(value) => MySQLValue::from(value),
            Value::Float(value) => MySQLValue::from(value),
            Value::Double(value) => MySQLValue::from(value),
            Value::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
            } => MySQLValue::Date(year, month, day, hour, minute, second, millisecond),
            Value::DateInterval {
                negative,
                day,
                hour,
                minute,
                second,
                millisecond,
            } => MySQLValue::Time(negative, day, hour, minute, second, millisecond),
            Value::Bytes(value) => MySQLValue::from(value),
            Value::Null => MySQLValue::NULL,
        }
    }
}

impl Into<Value> for MySQLValue {
    fn into(self) -> Value {
        match self {
            MySQLValue::Int(value) => Value::int(value),
            MySQLValue::UInt(value) => Value::uint(value),
            MySQLValue::Bytes(value) => Value::bytes(value),
            MySQLValue::Double(value) => Value::double(value),
            MySQLValue::Float(value) => Value::float(value),
            MySQLValue::Date(year, month, day, hour, minute, second, millisecond) => {
                Value::datetime(year, month, day, hour, minute, second, millisecond)
            }
            MySQLValue::Time(negative, day, hour, minute, second, millisecond) => {
                Value::date_interval(negative, day, hour, minute, second, millisecond)
            }
            MySQLValue::NULL => Value::null(),
        }
    }
}

#[cfg(test)]
mod value_to_mysql_tests {
    use super::*;

    #[test]
    fn it_maps_null_value_to_null_value() {
        assert_eq!(
            map_values_to_mysql_values(vec![Value::Null]),
            vec![MySQLValue::NULL]
        )
    }

    #[test]
    fn it_maps_integer_values_to_mysql_values() {
        assert_eq!(
            map_values_to_mysql_values(vec![Value::Int(123), Value::UnsignedInt(200)]),
            vec![MySQLValue::Int(123), MySQLValue::UInt(200)]
        )
    }

    #[test]
    fn it_maps_float_values_to_mysql_values() {
        assert_eq!(
            map_values_to_mysql_values(vec![Value::Float(1234.0), Value::Double(1212.0)]),
            vec![MySQLValue::Float(1234.0), MySQLValue::Double(1212.0)]
        )
    }

    #[test]
    fn it_maps_time_values_to_mysql_values() {
        assert_eq!(
            map_values_to_mysql_values(vec![
                Value::date_interval(true, 1, 2, 12, 123, 1450),
                Value::datetime(2010, 10, 1, 12, 23, 05, 1)
            ]),
            vec![
                MySQLValue::Time(true, 1, 2, 12, 123, 1450),
                MySQLValue::Date(2010, 10, 1, 12, 23, 05, 1)
            ]
        )
    }

    #[test]
    fn it_maps_bytes_values_to_mysql_bytes() {
        assert_eq!(
            map_values_to_mysql_values(vec![
                Value::bytes(b"value1".to_vec()),
                Value::bytes(b"value2".to_vec()),
                Value::bytes(b"value3".to_vec()),
            ]),
            vec![
                MySQLValue::Bytes(b"value1".to_vec()),
                MySQLValue::Bytes(b"value2".to_vec()),
                MySQLValue::Bytes(b"value3".to_vec())
            ]
        )
    }
}

#[cfg(test)]
mod mysql_to_value_tests {
    use super::*;

    #[test]
    fn it_maps_null_value_to_null_value() {
        assert_eq!(
            map_mysql_values_to_values(vec![MySQLValue::NULL]),
            vec![Value::Null]
        )
    }

    #[test]
    fn it_maps_integer_values_to_mysql_values() {
        assert_eq!(
            map_mysql_values_to_values(vec![MySQLValue::Int(123), MySQLValue::UInt(200)]),
            vec![Value::Int(123), Value::UnsignedInt(200)]
        )
    }

    #[test]
    fn it_maps_float_values_to_mysql_values() {
        assert_eq!(
            map_mysql_values_to_values(vec![MySQLValue::Float(1234.0), MySQLValue::Double(1212.0)]),
            vec![Value::Float(1234.0), Value::Double(1212.0)]
        )
    }

    #[test]
    fn it_maps_time_values_to_mysql_values() {
        assert_eq!(
            map_mysql_values_to_values(vec![
                MySQLValue::Time(true, 1, 2, 12, 123, 1450),
                MySQLValue::Date(2010, 10, 1, 12, 23, 05, 1)
            ]),
            vec![
                Value::date_interval(true, 1, 2, 12, 123, 1450),
                Value::datetime(2010, 10, 1, 12, 23, 05, 1)
            ]
        )
    }

    #[test]
    fn it_maps_bytes_values_to_mysql_bytes() {
        assert_eq!(
            map_mysql_values_to_values(vec![
                MySQLValue::Bytes(b"value1".to_vec()),
                MySQLValue::Bytes(b"value2".to_vec()),
                MySQLValue::Bytes(b"value3".to_vec())
            ]),
            vec![
                Value::bytes(b"value1".to_vec()),
                Value::bytes(b"value2".to_vec()),
                Value::bytes(b"value3".to_vec()),
            ]
        )
    }
}
