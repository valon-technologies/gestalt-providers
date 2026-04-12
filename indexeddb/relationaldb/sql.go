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
	default: // TypeString, TypeTime, TypeBytes, TypeJSON
		if d == dialectMySQL {
			if indexed {
				return "VARCHAR(255)"
			}
			return "LONGTEXT"
		}
		return "TEXT"
	}
}

func metadataTableSQL(d dialect) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(255) NOT NULL PRIMARY KEY, %s %s NOT NULL)",
		quoteIdent(d, "_gestalt_stores"),
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
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s NOT NULL PRIMARY KEY)",
			quoteIdent(d, table), quoteIdent(d, "id"), sqlType(d, 0, true))
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
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdent(d, table), strings.Join(defs, ", "))
}

func createIndexSQL(d dialect, table string, idx *proto.IndexSchema) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	cols := make([]string, len(idx.KeyPath))
	for i, c := range idx.KeyPath {
		cols[i] = quoteIdent(d, c)
	}
	indexName := fmt.Sprintf("idx_%s_%s", table, idx.Name)
	if d == dialectMySQL || d == dialectSQLServer {
		return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
			unique, quoteIdent(d, indexName), quoteIdent(d, table), strings.Join(cols, ", "))
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, quoteIdent(d, indexName), quoteIdent(d, table), strings.Join(cols, ", "))
}

func dropTableSQL(d dialect, table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(d, table))
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
		colList(d, cols), quoteIdent(d, table), quoteIdent(d, pkCol))
}

func selectKeyByPK(d dialect, table, pkCol string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		quoteIdent(d, pkCol), quoteIdent(d, table), quoteIdent(d, pkCol))
}

func insertSQL(d dialect, table string, cols []*proto.ColumnDef) string {
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(d, table), colList(d, cols), strings.Join(placeholders, ", "))
}

func deleteByPK(d dialect, table, pkCol string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(d, table), quoteIdent(d, pkCol))
}

func deleteAll(d dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s", quoteIdent(d, table))
}
