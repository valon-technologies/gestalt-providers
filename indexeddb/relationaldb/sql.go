package relationaldb

import (
	"fmt"
	"strings"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
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
			return "BLOB"
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

func indexedColumns(schema *proto.ObjectStoreSchema) map[string]struct{} {
	cols := make(map[string]struct{}, len(schema.GetIndexes()))
	for _, idx := range schema.GetIndexes() {
		for _, key := range idx.GetKeyPath() {
			cols[key] = struct{}{}
		}
	}
	return cols
}

func createTableSQL(d dialect, table string, schema *proto.ObjectStoreSchema) string {
	cols := schema.GetColumns()
	if len(cols) == 0 {
		if d == dialectSQLServer {
			return fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s %s NOT NULL PRIMARY KEY)",
				table, quoteTableName(d, table), quoteIdent(d, "id"), sqlType(d, 0, true))
		}
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s NOT NULL PRIMARY KEY)",
			quoteTableName(d, table), quoteIdent(d, "id"), sqlType(d, 0, true))
	}

	indexed := indexedColumns(schema)
	defs := make([]string, len(cols))
	for i, col := range cols {
		_, participatesInIndex := indexed[col.Name]
		def := quoteIdent(d, col.Name) + " " + sqlType(d, col.Type, col.PrimaryKey || col.Unique || participatesInIndex)
		if col.NotNull || col.PrimaryKey {
			def += " NOT NULL"
		}
		if col.PrimaryKey {
			def += " PRIMARY KEY"
		}
		if col.Unique && !col.PrimaryKey {
			def += " UNIQUE"
		}
		defs[i] = def
	}
	if d == dialectSQLServer {
		return fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL CREATE TABLE %s (%s)",
			table, quoteTableName(d, table), strings.Join(defs, ", "))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteTableName(d, table), strings.Join(defs, ", "))
}

func createIndexSQL(d dialect, table string, idx *proto.IndexSchema, schema *proto.ObjectStoreSchema) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	columnTypes := make(map[string]int32, len(schema.GetColumns()))
	for _, col := range schema.GetColumns() {
		columnTypes[col.Name] = col.Type
	}
	cols := make([]string, len(idx.KeyPath))
	for i, c := range idx.KeyPath {
		colSQL := quoteIdent(d, c)
		if d == dialectMySQL && !idx.Unique && len(idx.KeyPath) > 1 && columnTypes[c] == 0 {
			colSQL += "(128)"
		}
		cols[i] = colSQL
	}
	indexName := fmt.Sprintf("idx_%s_%s", baseTableName(table), idx.Name)
	createStmt := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, quoteIdent(d, indexName), quoteTableName(d, table), strings.Join(cols, ", "))
	if d == dialectMySQL {
		return createStmt
	}
	if d == dialectSQLServer {
		return sqlServerCreateIndexIfMissing(table, indexName, createStmt)
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, quoteIdent(d, indexName), quoteTableName(d, table), strings.Join(cols, ", "))
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

func dropTableSQL(d dialect, table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteTableName(d, table))
}

func colList(d dialect, cols []*proto.ColumnDef) string {
	if len(cols) == 0 {
		return quoteIdent(d, "id")
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(d, c.Name)
	}
	return strings.Join(parts, ", ")
}

func selectByPK(d dialect, table, pkCol string, cols []*proto.ColumnDef) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		colList(d, cols), quoteTableName(d, table), quoteIdent(d, pkCol))
}

func selectKeyByPK(d dialect, table, pkCol string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		quoteIdent(d, pkCol), quoteTableName(d, table), quoteIdent(d, pkCol))
}

func insertSQL(d dialect, table string, cols []*proto.ColumnDef) string {
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteTableName(d, table), colList(d, cols), strings.Join(placeholders, ", "))
}

func deleteByPK(d dialect, table, pkCol string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteTableName(d, table), quoteIdent(d, pkCol))
}

func deleteAll(d dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s", quoteTableName(d, table))
}

func selectDocumentPayloadByPK(d dialect, table, pkCol string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		quoteIdent(d, documentPayloadColumn),
		quoteTableName(d, table),
		quoteIdent(d, pkCol),
	)
}

func selectDocumentPayloadsWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	selectExpr := quoteIdent(d, documentPayloadColumn)
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", selectExpr, quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectExpr, quoteTableName(d, m.table), where), args, nil
}
