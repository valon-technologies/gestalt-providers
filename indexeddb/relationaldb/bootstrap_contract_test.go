package relationaldb_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

// Existing databases must remain usable by a runtime account without schema
// privileges. Schema creation is exercised by the administrative first open.
func TestMySQLExistingSchemaWithoutDDL(t *testing.T) {
	dsn := os.Getenv("GESTALT_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("GESTALT_TEST_MYSQL_DSN is not set")
	}
	ctx := context.Background()
	admin, err := relationaldb.NewStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin.Close()
	cfg, err := mysql.ParseDSN(strings.TrimPrefix(dsn, "mysql://"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	user := fmt.Sprintf("contract_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE USER '" + user + "'@'%' IDENTIFIED BY 'contract-test'"); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP USER '" + user + "'@'%'")
	if _, err := db.Exec("GRANT SELECT, INSERT, UPDATE, DELETE ON `" + strings.ReplaceAll(cfg.DBName, "`", "``") + "`.* TO '" + user + "'@'%'"); err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd = user, "contract-test"
	runtime, err := relationaldb.NewStore("mysql://" + cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open provisioned database with DML-only account: %v", err)
	}
	defer runtime.Close()
	if err := runtime.CreateObjectStore(ctx, user, gestalt.ObjectStoreOptions{Indexes: []gestalt.IndexSchema{{Name: "by_group", KeyPath: []string{"group"}}}}); err != nil {
		t.Fatal(err)
	}
	want := gestalt.Record{"id": "record", "group": "ready"}
	if err := runtime.Put(ctx, gestalt.IndexedDBRecordRequest{Store: user, Record: want}); err != nil {
		t.Fatal(err)
	}
	got, err := runtime.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: user, Index: "by_group"})
	if err != nil || !reflect.DeepEqual(got, []gestalt.Record{want}) {
		t.Fatalf("read after write without DDL: %v, %v", got, err)
	}
}
