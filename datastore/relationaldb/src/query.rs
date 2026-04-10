use gestalt::proto::v1::{ColumnDef, KeyRange};
use prost_types::value::Kind;

use crate::convert::{SqlValue, prost_value_to_sql_inferred};
use crate::store::StoreMeta;

/// Build a SELECT for all columns by primary key.
pub fn select_by_pk(meta: &StoreMeta) -> String {
    let cols = col_list(&meta.columns);
    format!(
        "SELECT {} FROM {} WHERE {} = $1",
        cols,
        quote_ident(&meta.table_name),
        quote_ident(&meta.pk_column)
    )
}

/// Build a SELECT returning only the primary key column, filtered by primary key.
pub fn select_key_by_pk(meta: &StoreMeta) -> String {
    format!(
        "SELECT {} FROM {} WHERE {} = $1",
        quote_ident(&meta.pk_column),
        quote_ident(&meta.table_name),
        quote_ident(&meta.pk_column)
    )
}

/// Build an INSERT statement for all columns.
pub fn insert(meta: &StoreMeta) -> String {
    let cols = col_list(&meta.columns);
    let placeholders = (1..=meta.columns.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(&meta.table_name),
        cols,
        placeholders
    )
}

/// Build a DELETE by primary key.
pub fn delete_by_pk(meta: &StoreMeta) -> String {
    format!(
        "DELETE FROM {} WHERE {} = $1",
        quote_ident(&meta.table_name),
        quote_ident(&meta.pk_column)
    )
}

/// Build a DELETE all rows (CLEAR).
pub fn delete_all(meta: &StoreMeta) -> String {
    format!("DELETE FROM {}", quote_ident(&meta.table_name))
}

/// Build a SELECT all columns with optional KeyRange filter on primary key.
/// Returns (sql, bind_values).
pub fn select_all_with_range(
    meta: &StoreMeta,
    range: Option<&KeyRange>,
) -> (String, Vec<SqlValue>) {
    let cols = col_list(&meta.columns);
    let table = quote_ident(&meta.table_name);
    let (where_clause, params) = key_range_where(&meta.pk_column, range, 0);
    let sql = if where_clause.is_empty() {
        format!("SELECT {} FROM {}", cols, table)
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            cols, table, where_clause
        )
    };
    (sql, params)
}

/// Build a SELECT returning only primary keys with optional KeyRange.
pub fn select_keys_with_range(
    meta: &StoreMeta,
    range: Option<&KeyRange>,
) -> (String, Vec<SqlValue>) {
    let pk = quote_ident(&meta.pk_column);
    let table = quote_ident(&meta.table_name);
    let (where_clause, params) = key_range_where(&meta.pk_column, range, 0);
    let sql = if where_clause.is_empty() {
        format!("SELECT {} FROM {}", pk, table)
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            pk, table, where_clause
        )
    };
    (sql, params)
}

/// Build a COUNT(*) with optional KeyRange.
pub fn count_with_range(
    meta: &StoreMeta,
    range: Option<&KeyRange>,
) -> (String, Vec<SqlValue>) {
    let table = quote_ident(&meta.table_name);
    let (where_clause, params) = key_range_where(&meta.pk_column, range, 0);
    let sql = if where_clause.is_empty() {
        format!("SELECT COUNT(*) FROM {}", table)
    } else {
        format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            table, where_clause
        )
    };
    (sql, params)
}

/// Build a DELETE with KeyRange.
pub fn delete_with_range(
    meta: &StoreMeta,
    range: Option<&KeyRange>,
) -> (String, Vec<SqlValue>) {
    let table = quote_ident(&meta.table_name);
    let (where_clause, params) = key_range_where(&meta.pk_column, range, 0);
    let sql = if where_clause.is_empty() {
        format!("DELETE FROM {}", table)
    } else {
        format!(
            "DELETE FROM {} WHERE {}",
            table, where_clause
        )
    };
    (sql, params)
}

/// Build an index query: SELECT all columns WHERE index columns match values,
/// with optional KeyRange and optional LIMIT 1.
pub fn index_select(
    meta: &StoreMeta,
    index_name: &str,
    values: &[prost_types::Value],
    range: Option<&KeyRange>,
    limit_one: bool,
    select_expr: &str,
) -> Option<(String, Vec<SqlValue>)> {
    let index = meta.indexes.iter().find(|i| i.name == index_name)?;

    let mut params = Vec::new();
    let mut clauses = Vec::new();
    let mut param_idx = 0;

    for (i, col_name) in index.key_path.iter().enumerate() {
        if i >= values.len() {
            break;
        }
        param_idx += 1;
        clauses.push(format!("{} = ${}", quote_ident(col_name), param_idx));
        params.push(prost_value_to_sql_inferred(&values[i]));
    }

    let (range_clause, range_params) = key_range_where(&meta.pk_column, range, param_idx);
    if !range_clause.is_empty() {
        clauses.push(range_clause);
    }
    params.extend(range_params);

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    let limit = if limit_one { " LIMIT 1" } else { "" };

    let sql = format!(
        "SELECT {} FROM {}{}{}",
        select_expr, quote_ident(&meta.table_name), where_clause, limit
    );

    Some((sql, params))
}

/// Build a DELETE WHERE index columns match values.
pub fn index_delete(
    meta: &StoreMeta,
    index_name: &str,
    values: &[prost_types::Value],
) -> Option<(String, Vec<SqlValue>)> {
    let index = meta.indexes.iter().find(|i| i.name == index_name)?;

    let mut params = Vec::new();
    let mut clauses = Vec::new();

    for (i, col_name) in index.key_path.iter().enumerate() {
        if i >= values.len() {
            break;
        }
        clauses.push(format!("{} = ${}", quote_ident(col_name), i + 1));
        params.push(prost_value_to_sql_inferred(&values[i]));
    }

    if clauses.is_empty() {
        return None;
    }

    let sql = format!(
        "DELETE FROM {} WHERE {}",
        quote_ident(&meta.table_name),
        clauses.join(" AND ")
    );
    Some((sql, params))
}

/// Build a COUNT WHERE index columns match values, with optional KeyRange.
pub fn index_count(
    meta: &StoreMeta,
    index_name: &str,
    values: &[prost_types::Value],
    range: Option<&KeyRange>,
) -> Option<(String, Vec<SqlValue>)> {
    let index = meta.indexes.iter().find(|i| i.name == index_name)?;

    let mut params = Vec::new();
    let mut clauses = Vec::new();
    let mut param_idx = 0;

    for (i, col_name) in index.key_path.iter().enumerate() {
        if i >= values.len() {
            break;
        }
        param_idx += 1;
        clauses.push(format!("{} = ${}", quote_ident(col_name), param_idx));
        params.push(prost_value_to_sql_inferred(&values[i]));
    }

    let (range_clause, range_params) = key_range_where(&meta.pk_column, range, param_idx);
    if !range_clause.is_empty() {
        clauses.push(range_clause);
    }
    params.extend(range_params);

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    let sql = format!(
        "SELECT COUNT(*) FROM {}{}",
        quote_ident(&meta.table_name), where_clause
    );
    Some((sql, params))
}

// ---- helpers ----

/// Escape a SQL identifier by doubling any embedded double quotes.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn col_list(columns: &[ColumnDef]) -> String {
    if columns.is_empty() {
        quote_ident("id")
    } else {
        columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn key_range_where(
    pk_col: &str,
    range: Option<&KeyRange>,
    param_offset: usize,
) -> (String, Vec<SqlValue>) {
    let range = match range {
        Some(r) => r,
        None => return (String::new(), Vec::new()),
    };

    let mut clauses = Vec::new();
    let mut params = Vec::new();
    let mut idx = param_offset;

    let pk = quote_ident(pk_col);

    if let Some(lower) = &range.lower {
        if !matches!(lower.kind, Some(Kind::NullValue(_)) | None) {
            idx += 1;
            let op = if range.lower_open { ">" } else { ">=" };
            clauses.push(format!("{} {} ${}", pk, op, idx));
            params.push(prost_value_to_sql_inferred(lower));
        }
    }

    if let Some(upper) = &range.upper {
        if !matches!(upper.kind, Some(Kind::NullValue(_)) | None) {
            idx += 1;
            let op = if range.upper_open { "<" } else { "<=" };
            clauses.push(format!("{} {} ${}", pk, op, idx));
            params.push(prost_value_to_sql_inferred(upper));
        }
    }

    (clauses.join(" AND "), params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gestalt::proto::v1::{ColumnDef, IndexSchema};

    fn test_meta() -> StoreMeta {
        StoreMeta {
            table_name: "users".into(),
            pk_column: "id".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    r#type: 0,
                    primary_key: true,
                    not_null: true,
                    unique: false,
                },
                ColumnDef {
                    name: "email".into(),
                    r#type: 0,
                    primary_key: false,
                    not_null: true,
                    unique: true,
                },
            ],
            indexes: vec![IndexSchema {
                name: "by_email".into(),
                key_path: vec!["email".into()],
                unique: true,
            }],
        }
    }

    #[test]
    fn test_select_by_pk() {
        let sql = select_by_pk(&test_meta());
        assert_eq!(
            sql,
            "SELECT \"id\", \"email\" FROM \"users\" WHERE \"id\" = $1"
        );
    }

    #[test]
    fn test_insert() {
        let sql = insert(&test_meta());
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"email\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_delete_by_pk() {
        let sql = delete_by_pk(&test_meta());
        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = $1");
    }

    #[test]
    fn test_select_all_no_range() {
        let (sql, params) = select_all_with_range(&test_meta(), None);
        assert_eq!(sql, "SELECT \"id\", \"email\" FROM \"users\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_select_all_with_range() {
        let range = KeyRange {
            lower: Some(prost_types::Value {
                kind: Some(Kind::StringValue("a".into())),
            }),
            upper: Some(prost_types::Value {
                kind: Some(Kind::StringValue("z".into())),
            }),
            lower_open: false,
            upper_open: true,
        };
        let (sql, params) = select_all_with_range(&test_meta(), Some(&range));
        assert_eq!(
            sql,
            "SELECT \"id\", \"email\" FROM \"users\" WHERE \"id\" >= $1 AND \"id\" < $2"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_index_select() {
        let meta = test_meta();
        let values = vec![prost_types::Value {
            kind: Some(Kind::StringValue("test@example.com".into())),
        }];
        let (sql, params) = index_select(&meta, "by_email", &values, None, true, "\"id\", \"email\"").unwrap();
        assert!(sql.contains("WHERE \"email\" = $1"));
        assert!(sql.contains("LIMIT 1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_index_delete() {
        let meta = test_meta();
        let values = vec![prost_types::Value {
            kind: Some(Kind::StringValue("test@example.com".into())),
        }];
        let (sql, params) = index_delete(&meta, "by_email", &values).unwrap();
        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"email\" = $1");
        assert_eq!(params.len(), 1);
    }
}
