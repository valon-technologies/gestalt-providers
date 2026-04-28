package relationaldb

import (
	"fmt"
	"strings"
)

// bindStyle controls how query placeholders are rendered.
type bindStyle int

const (
	bindQuestion bindStyle = iota // ? (MySQL, SQLite)
	bindDollar                    // $1, $2 (PostgreSQL)
	bindAtP                       // @p1, @p2 (SQL Server)
)

type dialect int

const (
	dialectSQLite dialect = iota
	dialectPostgres
	dialectMySQL
	dialectSQLServer
)

// rebind replaces ? placeholders with the style expected by the driver.
func rebind(style bindStyle, query string) string {
	if style == bindQuestion {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 16)
	n := 0
	for _, ch := range query {
		if ch == '?' {
			n++
			switch style {
			case bindDollar:
				fmt.Fprintf(&b, "$%d", n)
			case bindAtP:
				fmt.Fprintf(&b, "@p%d", n)
			}
		} else {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

// quoteIdent escapes a SQL identifier using the quoting style expected by the
// target database.
func quoteIdent(d dialect, name string) string {
	if d == dialectMySQL {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteTableName(d dialect, name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteIdent(d, part))
	}
	return strings.Join(quoted, ".")
}

func qualifyTableName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func baseTableName(table string) string {
	if idx := strings.LastIndex(table, "."); idx >= 0 {
		return table[idx+1:]
	}
	return table
}

// sqlType maps a proto ColumnDef type to a portable SQL type name.
func sqlType(d dialect, colType int32, indexed bool) string {
	switch colType {
	case 1: // TypeInt
		return "BIGINT"
	case 2: // TypeFloat
		if d == dialectMySQL {
			return "DOUBLE"
		}
		return "DOUBLE PRECISION"
	case 3: // TypeBool
		return "SMALLINT"
	case 4: // TypeTime
		switch d {
		case dialectPostgres:
			return "TIMESTAMPTZ"
		case dialectMySQL:
			return "DATETIME(6)"
		case dialectSQLServer:
			return "DATETIME2"
		default:
			return "TEXT"
		}
	case 5: // TypeBytes
		switch d {
		case dialectPostgres:
			return "BYTEA"
		case dialectMySQL:
			if indexed {
				return "VARBINARY(255)"
			}
			return "LONGBLOB"
		case dialectSQLServer:
			if indexed {
				return "VARBINARY(255)"
			}
			return "VARBINARY(MAX)"
		default:
			return "BLOB"
		}
	default: // TypeString, TypeTime, TypeBytes, TypeJSON
		if d == dialectMySQL {
			if indexed {
				return "VARCHAR(255)"
			}
			return "LONGTEXT"
		}
		if d == dialectSQLServer {
			if indexed {
				return "NVARCHAR(255)"
			}
			return "NVARCHAR(MAX)"
		}
		return "TEXT"
	}
}

func metadataTableSQL(d dialect, table string) string {
	if d == dialectSQLServer {
		return fmt.Sprintf(
			"IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s VARCHAR(255) NOT NULL PRIMARY KEY, %s %s NOT NULL)",
			table,
			quoteTableName(d, table),
			quoteIdent(d, "name"),
			quoteIdent(d, "schema_json"),
			sqlType(d, 0, false),
		)
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(255) NOT NULL PRIMARY KEY, %s %s NOT NULL)",
		quoteTableName(d, table),
		quoteIdent(d, "name"),
		quoteIdent(d, "schema_json"),
		sqlType(d, 0, false),
	)
}

func createGenericRecordsTableSQL(d dialect, table string) string {
	defs := []string{
		quoteIdent(d, "store_name") + " " + sqlType(d, 0, true) + " NOT NULL",
		quoteIdent(d, "pk_hash") + " " + sqlType(d, 5, true) + " NOT NULL",
		quoteIdent(d, "pk_bytes") + " " + sqlType(d, 5, false) + " NOT NULL",
		quoteIdent(d, "record_blob") + " " + sqlType(d, 5, false) + " NOT NULL",
	}
	if d == dialectSQLServer {
		return fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s)",
			table, quoteTableName(d, table), strings.Join(defs, ", "))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteTableName(d, table), strings.Join(defs, ", "))
}

func createGenericIndexEntriesTableSQL(d dialect, table string) string {
	defs := []string{
		quoteIdent(d, "store_name") + " " + sqlType(d, 0, true) + " NOT NULL",
		quoteIdent(d, "index_name") + " " + sqlType(d, 0, true) + " NOT NULL",
		quoteIdent(d, "index_key_hash") + " " + sqlType(d, 5, true) + " NOT NULL",
		quoteIdent(d, "index_key_bytes") + " " + sqlType(d, 5, false) + " NOT NULL",
		quoteIdent(d, "pk_hash") + " " + sqlType(d, 5, true) + " NOT NULL",
		quoteIdent(d, "pk_bytes") + " " + sqlType(d, 5, false) + " NOT NULL",
	}
	if d == dialectSQLServer {
		return fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s)",
			table, quoteTableName(d, table), strings.Join(defs, ", "))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteTableName(d, table), strings.Join(defs, ", "))
}

func createGenericRecordsLookupIndexSQL(d dialect, table string) string {
	indexName := fmt.Sprintf("idx_%s_record_lookup", baseTableName(table))
	return createColumnsIndexSQL(d, table, indexName, []string{"store_name", "pk_hash"}, true)
}

func createGenericRecordsStoreIndexSQL(d dialect, table string) string {
	indexName := fmt.Sprintf("idx_%s_store", baseTableName(table))
	return createColumnsIndexSQL(d, table, indexName, []string{"store_name"}, false)
}

func createGenericIndexLookupIndexSQL(d dialect, table string, unique bool) string {
	indexName := fmt.Sprintf("idx_%s_lookup", baseTableName(table))
	return createColumnsIndexSQL(d, table, indexName, []string{"store_name", "index_name", "index_key_hash"}, unique)
}

func createGenericIndexRecordIndexSQL(d dialect, table string) string {
	indexName := fmt.Sprintf("idx_%s_record", baseTableName(table))
	return createColumnsIndexSQL(d, table, indexName, []string{"store_name", "pk_hash"}, false)
}

func createGenericIndexScanIndexSQL(d dialect, table string) string {
	indexName := fmt.Sprintf("idx_%s_scan", baseTableName(table))
	return createColumnsIndexSQL(d, table, indexName, []string{"store_name", "index_name"}, false)
}

func createColumnsIndexSQL(d dialect, table, indexName string, columns []string, unique bool) string {
	uniquePrefix := ""
	if unique {
		uniquePrefix = "UNIQUE "
	}
	cols := make([]string, len(columns))
	for i, col := range columns {
		cols[i] = quoteIdent(d, col)
	}
	createStmt := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniquePrefix, quoteIdent(d, indexName), quoteTableName(d, table), strings.Join(cols, ", "))
	if d == dialectMySQL {
		return createStmt
	}
	if d == dialectSQLServer {
		return sqlServerCreateIndexIfMissing(table, indexName, createStmt)
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		uniquePrefix, quoteIdent(d, indexName), quoteTableName(d, table), strings.Join(cols, ", "))
}

func sqlServerCreateIndexIfMissing(table, indexName, createStmt string) string {
	return fmt.Sprintf(
		"DECLARE @gestalt_object_id int = OBJECT_ID(%s); "+
			"IF @gestalt_object_id IS NULL THROW 51000, 'failed to resolve index target object', 1; "+
			"DECLARE @gestalt_lock_resource nvarchar(255) = CONCAT(N'gestalt:index:', CONVERT(nvarchar(32), @gestalt_object_id)); "+
			"DECLARE @gestalt_lock_result int; "+
			"EXEC @gestalt_lock_result = sp_getapplock @Resource = @gestalt_lock_resource, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = -1; "+
			"IF @gestalt_lock_result < 0 THROW 51000, 'failed to acquire index creation lock', 1; "+
			"BEGIN TRY "+
			"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = %s AND object_id = @gestalt_object_id) %s; "+
			"END TRY "+
			"BEGIN CATCH "+
			"EXEC sp_releaseapplock @Resource = @gestalt_lock_resource, @LockOwner = 'Session'; "+
			"THROW; "+
			"END CATCH; "+
			"EXEC sp_releaseapplock @Resource = @gestalt_lock_resource, @LockOwner = 'Session';",
		sqlServerStringLiteral(sqlServerObjectName(table)),
		sqlServerStringLiteral(indexName),
		createStmt,
	)
}

func sqlServerStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlServerObjectName(table string) string {
	parts := strings.Split(table, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, "["+strings.ReplaceAll(part, "]", "]]")+"]")
	}
	return strings.Join(quoted, ".")
}
