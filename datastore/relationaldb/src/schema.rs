use gestalt::proto::v1::{IndexSchema, ObjectStoreSchema};

use crate::query::quote_ident;

const TYPE_STRING: i32 = 0;
const TYPE_INT: i32 = 1;
const TYPE_FLOAT: i32 = 2;
const TYPE_BOOL: i32 = 3;
const TYPE_TIME: i32 = 4;
const TYPE_BYTES: i32 = 5;
const TYPE_JSON: i32 = 6;

pub fn sql_type(col_type: i32) -> &'static str {
    match col_type {
        TYPE_INT => "BIGINT",
        TYPE_FLOAT => "DOUBLE PRECISION",
        TYPE_BOOL => "SMALLINT",
        TYPE_STRING | TYPE_TIME | TYPE_BYTES | TYPE_JSON => "TEXT",
        _ => "TEXT",
    }
}

pub fn create_table_sql(table: &str, schema: &ObjectStoreSchema) -> String {
    let columns = &schema.columns;
    if columns.is_empty() {
        return format!(
            "CREATE TABLE IF NOT EXISTS {} ({} TEXT NOT NULL PRIMARY KEY)",
            quote_ident(table),
            quote_ident("id")
        );
    }

    let col_defs: Vec<String> = columns
        .iter()
        .map(|col| {
            let mut def = format!("{} {}", quote_ident(&col.name), sql_type(col.r#type));
            if col.not_null || col.primary_key {
                def.push_str(" NOT NULL");
            }
            if col.primary_key {
                def.push_str(" PRIMARY KEY");
            }
            if col.unique && !col.primary_key {
                def.push_str(" UNIQUE");
            }
            def
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        col_defs.join(", ")
    )
}

pub fn create_index_sql(table: &str, index: &IndexSchema) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let cols: Vec<String> = index.key_path.iter().map(|c| quote_ident(c)).collect();
    let index_name = format!("idx_{}_{}", table, index.name);
    format!(
        "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
        unique,
        quote_ident(&index_name),
        quote_ident(table),
        cols.join(", ")
    )
}

pub fn drop_table_sql(table: &str) -> String {
    format!("DROP TABLE IF EXISTS {}", quote_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gestalt::proto::v1::ColumnDef;

    fn users_schema() -> ObjectStoreSchema {
        ObjectStoreSchema {
            indexes: vec![IndexSchema {
                name: "by_email".into(),
                key_path: vec!["email".into()],
                unique: true,
            }],
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    r#type: TYPE_STRING,
                    primary_key: true,
                    not_null: false,
                    unique: false,
                },
                ColumnDef {
                    name: "email".into(),
                    r#type: TYPE_STRING,
                    primary_key: false,
                    not_null: true,
                    unique: true,
                },
                ColumnDef {
                    name: "display_name".into(),
                    r#type: TYPE_STRING,
                    primary_key: false,
                    not_null: false,
                    unique: false,
                },
                ColumnDef {
                    name: "created_at".into(),
                    r#type: TYPE_TIME,
                    primary_key: false,
                    not_null: false,
                    unique: false,
                },
            ],
        }
    }

    #[test]
    fn test_create_table() {
        let ddl = create_table_sql("users", &users_schema());
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"users\""));
        assert!(ddl.contains("\"id\" TEXT NOT NULL PRIMARY KEY"));
        assert!(ddl.contains("\"email\" TEXT NOT NULL UNIQUE"));
        assert!(ddl.contains("\"display_name\" TEXT"));
        assert!(ddl.contains("\"created_at\" TEXT"));
    }

    #[test]
    fn test_create_index() {
        let idx = &users_schema().indexes[0];
        let sql = create_index_sql("users", idx);
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_users_by_email\" ON \"users\" (\"email\")"
        );
    }

    #[test]
    fn test_create_table_no_columns() {
        let schema = ObjectStoreSchema {
            indexes: vec![],
            columns: vec![],
        };
        let ddl = create_table_sql("empty", &schema);
        assert_eq!(
            ddl,
            "CREATE TABLE IF NOT EXISTS \"empty\" (\"id\" TEXT NOT NULL PRIMARY KEY)"
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(drop_table_sql("users"), "DROP TABLE IF EXISTS \"users\"");
    }

    #[test]
    fn test_type_mapping() {
        assert_eq!(sql_type(TYPE_STRING), "TEXT");
        assert_eq!(sql_type(TYPE_INT), "BIGINT");
        assert_eq!(sql_type(TYPE_FLOAT), "DOUBLE PRECISION");
        assert_eq!(sql_type(TYPE_BOOL), "SMALLINT");
        assert_eq!(sql_type(TYPE_TIME), "TEXT");
        assert_eq!(sql_type(TYPE_BYTES), "TEXT");
        assert_eq!(sql_type(TYPE_JSON), "TEXT");
    }
}
