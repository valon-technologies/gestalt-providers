package relationaldb

import (
	"context"
	"fmt"
	"strings"
)

type tableRequirement struct {
	name                 string
	columns              []string
	mysqlLongBlobColumns []string
}

func (s *Store) validateGenericTables(ctx context.Context) error {
	for _, table := range s.genericTableRequirements() {
		columns := make([]string, len(table.columns))
		for i, column := range table.columns {
			columns[i] = quoteIdent(s.dialect, column)
		}
		rows, err := s.query(ctx,
			"SELECT "+strings.Join(columns, ", ")+" FROM "+quoteTableName(s.dialect, table.name)+" WHERE 1 = 0",
		)
		if err != nil {
			return fmt.Errorf("validate table %q: %w", table.name, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("validate table %q: %w", table.name, err)
		}
	}
	return s.validateGenericMySQLLongBlobColumns(ctx)
}

func (s *Store) genericTableRequirements() []tableRequirement {
	indexColumns := []string{
		"store_name", "index_name", "index_key_hash", "index_key_bytes",
		"index_key_ord", "pk_hash", "pk_bytes",
	}
	return []tableRequirement{
		{
			name:    s.metadataTable(),
			columns: []string{"name", "schema_json"},
		},
		{
			name:                 s.genericRecordsTable(),
			columns:              []string{"store_name", "pk_hash", "pk_bytes", "record_blob"},
			mysqlLongBlobColumns: []string{"pk_bytes", "record_blob"},
		},
		{
			name:                 s.genericIndexTable(),
			columns:              indexColumns,
			mysqlLongBlobColumns: []string{"index_key_bytes", "pk_bytes"},
		},
		{
			name:                 s.genericUniqueIndexTable(),
			columns:              indexColumns,
			mysqlLongBlobColumns: []string{"index_key_bytes", "pk_bytes"},
		},
	}
}
