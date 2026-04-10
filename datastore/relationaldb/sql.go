package relationaldb

import (
	"fmt"
	"strings"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

// bindStyle controls how query placeholders are rendered.
type bindStyle int

const (
	bindQuestion   bindStyle = iota // ? (MySQL, SQLite)
	bindDollar                      // $1, $2 (PostgreSQL)
	bindAtP                         // @p1, @p2 (SQL Server)
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

// quoteIdent escapes a SQL identifier by doubling any embedded double quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sqlType maps a proto ColumnDef type to a portable SQL type name.
func sqlType(colType int32) string {
	switch colType {
	case 1: // TypeInt
		return "BIGINT"
	case 2: // TypeFloat
		return "DOUBLE PRECISION"
	case 3: // TypeBool
		return "SMALLINT"
	default: // TypeString, TypeTime, TypeBytes, TypeJSON
		return "TEXT"
	}
}

func createTableSQL(table string, schema *proto.ObjectStoreSchema) string {
	cols := schema.GetColumns()
	if len(cols) == 0 {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s TEXT NOT NULL PRIMARY KEY)",
			quoteIdent(table), quoteIdent("id"))
	}

	defs := make([]string, len(cols))
	for i, col := range cols {
		def := quoteIdent(col.Name) + " " + sqlType(col.Type)
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
		quoteIdent(table), strings.Join(defs, ", "))
}

func createIndexSQL(table string, idx *proto.IndexSchema) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	cols := make([]string, len(idx.KeyPath))
	for i, c := range idx.KeyPath {
		cols[i] = quoteIdent(c)
	}
	indexName := fmt.Sprintf("idx_%s_%s", table, idx.Name)
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, quoteIdent(indexName), quoteIdent(table), strings.Join(cols, ", "))
}

func dropTableSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(table))
}

func colList(cols []*proto.ColumnDef) string {
	if len(cols) == 0 {
		return quoteIdent("id")
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c.Name)
	}
	return strings.Join(parts, ", ")
}

func selectByPK(table, pkCol string, cols []*proto.ColumnDef) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		colList(cols), quoteIdent(table), quoteIdent(pkCol))
}

func selectKeyByPK(table, pkCol string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		quoteIdent(pkCol), quoteIdent(table), quoteIdent(pkCol))
}

func insertSQL(table string, cols []*proto.ColumnDef) string {
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table), colList(cols), strings.Join(placeholders, ", "))
}

func deleteByPK(table, pkCol string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(table), quoteIdent(pkCol))
}

func deleteAll(table string) string {
	return fmt.Sprintf("DELETE FROM %s", quoteIdent(table))
}
