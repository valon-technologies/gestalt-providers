use std::collections::BTreeMap;

use prost_types::value::Kind;
use prost_types::{Struct, Value};
use sqlx::any::AnyRow;
use sqlx::Row;

use gestalt::proto::v1::ColumnDef;

const TYPE_INT: i32 = 1;
const TYPE_FLOAT: i32 = 2;
const TYPE_BOOL: i32 = 3;

/// A value that can be bound to a sqlx::Any query parameter.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Float(f64),
    SmallInt(i16),
    Null,
}

/// Extract a protobuf Value into a SqlValue according to the column type.
pub fn prost_value_to_sql(val: Option<&Value>, col_type: i32) -> SqlValue {
    let kind = match val.and_then(|v| v.kind.as_ref()) {
        Some(k) => k,
        None => return SqlValue::Null,
    };

    match (kind, col_type) {
        (Kind::NullValue(_), _) => SqlValue::Null,
        (Kind::NumberValue(n), TYPE_INT) => SqlValue::Int(*n as i64),
        (Kind::NumberValue(n), TYPE_FLOAT) => SqlValue::Float(*n),
        (Kind::NumberValue(n), TYPE_BOOL) => SqlValue::SmallInt(if *n != 0.0 { 1 } else { 0 }),
        (Kind::BoolValue(b), TYPE_BOOL) => SqlValue::SmallInt(if *b { 1 } else { 0 }),
        (Kind::BoolValue(b), _) => SqlValue::Text(b.to_string()),
        (Kind::StringValue(s), TYPE_INT) => s
            .parse::<i64>()
            .map(SqlValue::Int)
            .unwrap_or(SqlValue::Text(s.clone())),
        (Kind::StringValue(s), TYPE_FLOAT) => s
            .parse::<f64>()
            .map(SqlValue::Float)
            .unwrap_or(SqlValue::Text(s.clone())),
        (Kind::StringValue(s), _) => SqlValue::Text(s.clone()),
        (Kind::NumberValue(n), _) => SqlValue::Float(*n),
        (Kind::StructValue(s), _) => {
            SqlValue::Text(prost_struct_to_json_string(s))
        }
        (Kind::ListValue(l), _) => {
            SqlValue::Text(prost_list_to_json_string(l))
        }
    }
}

/// Convert a prost_types::Value to a SqlValue, inferring type from the value itself.
pub fn prost_value_to_sql_inferred(val: &Value) -> SqlValue {
    match val.kind.as_ref() {
        None | Some(Kind::NullValue(_)) => SqlValue::Null,
        Some(Kind::StringValue(s)) => SqlValue::Text(s.clone()),
        Some(Kind::NumberValue(n)) => SqlValue::Text(n.to_string()),
        Some(Kind::BoolValue(b)) => SqlValue::Text(b.to_string()),
        Some(Kind::StructValue(s)) => {
            SqlValue::Text(prost_struct_to_json_string(s))
        }
        Some(Kind::ListValue(l)) => {
            SqlValue::Text(prost_list_to_json_string(l))
        }
    }
}

/// Extract column values from a protobuf Struct for the given column definitions.
pub fn struct_to_sql_values(record: &Struct, columns: &[ColumnDef]) -> Vec<SqlValue> {
    columns
        .iter()
        .map(|col| {
            let val = record.fields.get(&col.name);
            prost_value_to_sql(val, col.r#type)
        })
        .collect()
}

/// Convert a sqlx AnyRow back into a protobuf Struct using column definitions.
pub fn row_to_struct(row: &AnyRow, columns: &[ColumnDef]) -> Struct {
    let mut fields = BTreeMap::new();

    for col in columns {
        let kind = match col.r#type {
            TYPE_INT => {
                let v: Option<i64> = row.try_get(col.name.as_str()).ok();
                match v {
                    Some(n) => Kind::NumberValue(n as f64),
                    None => Kind::NullValue(0),
                }
            }
            TYPE_FLOAT => {
                let v: Option<f64> = row.try_get(col.name.as_str()).ok();
                match v {
                    Some(n) => Kind::NumberValue(n),
                    None => Kind::NullValue(0),
                }
            }
            TYPE_BOOL => {
                let v: Option<i16> = row.try_get(col.name.as_str()).ok();
                Kind::BoolValue(v.unwrap_or(0) != 0)
            }
            // TypeString, TypeTime, TypeBytes, TypeJSON — all stored as TEXT
            _ => {
                let v: Option<String> = row.try_get(col.name.as_str()).ok();
                match v {
                    Some(s) => Kind::StringValue(s),
                    None => Kind::NullValue(0),
                }
            }
        };
        fields.insert(
            col.name.clone(),
            Value {
                kind: Some(kind),
            },
        );
    }

    Struct { fields }
}

fn prost_value_to_json(val: &Value) -> serde_json::Value {
    match val.kind.as_ref() {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::NumberValue(n)) => serde_json::json!(*n),
        Some(Kind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Kind::StructValue(s)) => {
            let map: serde_json::Map<String, serde_json::Value> = s
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Some(Kind::ListValue(l)) => {
            let arr: Vec<serde_json::Value> = l.values.iter().map(prost_value_to_json).collect();
            serde_json::Value::Array(arr)
        }
    }
}

fn prost_struct_to_json_string(s: &Struct) -> String {
    let map: serde_json::Map<String, serde_json::Value> = s
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
        .collect();
    serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default()
}

fn prost_list_to_json_string(l: &prost_types::ListValue) -> String {
    let arr: Vec<serde_json::Value> = l.values.iter().map(prost_value_to_json).collect();
    serde_json::to_string(&serde_json::Value::Array(arr)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prost_value_to_sql_string() {
        let val = Value {
            kind: Some(Kind::StringValue("hello".into())),
        };
        match prost_value_to_sql(Some(&val), 0) {
            SqlValue::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn test_prost_value_to_sql_int() {
        let val = Value {
            kind: Some(Kind::NumberValue(42.0)),
        };
        match prost_value_to_sql(Some(&val), TYPE_INT) {
            SqlValue::Int(n) => assert_eq!(n, 42),
            _ => panic!("expected Int"),
        }
    }

    #[test]
    fn test_prost_value_to_sql_bool() {
        let val = Value {
            kind: Some(Kind::BoolValue(true)),
        };
        match prost_value_to_sql(Some(&val), TYPE_BOOL) {
            SqlValue::SmallInt(n) => assert_eq!(n, 1),
            _ => panic!("expected SmallInt"),
        }
    }

    #[test]
    fn test_prost_value_to_sql_null() {
        match prost_value_to_sql(None, 0) {
            SqlValue::Null => {}
            _ => panic!("expected Null"),
        }
    }

    #[test]
    fn test_struct_to_sql_values() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "id".to_string(),
            Value {
                kind: Some(Kind::StringValue("u1".into())),
            },
        );
        fields.insert(
            "count".to_string(),
            Value {
                kind: Some(Kind::NumberValue(5.0)),
            },
        );
        let record = Struct { fields };
        let columns = vec![
            ColumnDef {
                name: "id".into(),
                r#type: 0,
                primary_key: true,
                not_null: true,
                unique: false,
            },
            ColumnDef {
                name: "count".into(),
                r#type: TYPE_INT,
                primary_key: false,
                not_null: false,
                unique: false,
            },
        ];
        let values = struct_to_sql_values(&record, &columns);
        assert_eq!(values.len(), 2);
        match &values[0] {
            SqlValue::Text(s) => assert_eq!(s, "u1"),
            _ => panic!("expected Text"),
        }
        match &values[1] {
            SqlValue::Int(n) => assert_eq!(*n, 5),
            _ => panic!("expected Int"),
        }
    }
}
