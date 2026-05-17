package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	relationshipsStore = "relationships"
	relationshipsIndex = "by_resource"

	agentSessionResourceType = "agent_session"
	agentSessionRouteStore   = "agent_session_routes"
	agentTurnRouteStore      = "agent_turn_routes"

	defaultExpectedProject  = "gitlab-peach-street"
	defaultExpectedInstance = "terra-east4"
	defaultExpectedSecret   = "gestalt-mysql-dsn-east4"
	defaultExpectedSchema   = "gestaltd"
)

type options struct {
	DSN    string
	DSNEnv string

	Project     string
	Instance    string
	Secret      string
	Schema      string
	TablePrefix string

	ExpectedProject  string
	ExpectedInstance string
	ExpectedSecret   string
	ExpectedSchema   string

	Execute   bool
	MaxDelete int64

	BackupConfirmation            string
	ProviderVisibilityDeployedRef string
	OldGestaltdDrained            bool

	Timeout time.Duration
}

type storeCount struct {
	Exists  bool
	Records int64
	Keys    []string
}

type summary struct {
	RelationshipKeys       []string
	RelationshipRecords    int64
	RelationshipSampleKeys []string
	SessionRoutes          storeCount
	TurnRoutes             storeCount
	RawRouteRows           map[string]rawStoreRows
	DeletedRelationships   int64
	DeletedRouteStores     []string
}

type rawStoreRows struct {
	MetadataRows    int64
	RecordRows      int64
	IndexRows       int64
	UniqueIndexRows int64
}

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "cleanup-agent-session-state: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	var opts options
	flags := flag.NewFlagSet("cleanup-agent-session-state", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.DSN, "dsn", "", "relationaldb DSN; omitted from logs")
	flags.StringVar(&opts.DSNEnv, "dsn-env", "GESTALT_INDEXEDDB_DSN", "environment variable containing the relationaldb DSN when --dsn is empty")
	flags.StringVar(&opts.Project, "project", "", "operator-confirmed GCP project")
	flags.StringVar(&opts.Instance, "instance", "", "operator-confirmed Cloud SQL instance")
	flags.StringVar(&opts.Secret, "secret", "", "operator-confirmed secret containing the DSN")
	flags.StringVar(&opts.Schema, "schema", defaultExpectedSchema, "relationaldb schema/database name")
	flags.StringVar(&opts.TablePrefix, "table-prefix", "", "relationaldb table prefix")
	flags.StringVar(&opts.ExpectedProject, "expected-project", defaultExpectedProject, "required project guard")
	flags.StringVar(&opts.ExpectedInstance, "expected-instance", defaultExpectedInstance, "required instance guard")
	flags.StringVar(&opts.ExpectedSecret, "expected-secret", defaultExpectedSecret, "required secret guard")
	flags.StringVar(&opts.ExpectedSchema, "expected-schema", defaultExpectedSchema, "required schema guard")
	flags.BoolVar(&opts.Execute, "execute", false, "delete records after all execute-only confirmations pass")
	flags.Int64Var(&opts.MaxDelete, "max-delete", 0, "maximum relationship rows plus route rows allowed to be deleted when --execute is set")
	flags.StringVar(&opts.BackupConfirmation, "backup-confirmation", "", "backup/export operation id or ticket proving rollback data exists; required for --execute")
	flags.StringVar(&opts.ProviderVisibilityDeployedRef, "provider-visibility-deployed-ref", "", "deployed ref containing provider-owned Slack visibility; required for --execute")
	flags.BoolVar(&opts.OldGestaltdDrained, "old-gestaltd-drained", false, "confirm old gestaltd revisions that write legacy state are drained; required for --execute")
	flags.DurationVar(&opts.Timeout, "timeout", 30*time.Second, "operation timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	return run(ctx, stdout, opts)
}

func run(ctx context.Context, stdout io.Writer, opts options) error {
	dsn, err := resolveDSN(opts)
	if err != nil {
		return err
	}
	if err := validateGuards(opts); err != nil {
		return err
	}
	if err := validateMetadataPreflight(ctx, dsn, opts); err != nil {
		return err
	}

	provider := relationaldb.New()
	defer provider.Close()
	config := map[string]any{"dsn": dsn}
	if strings.TrimSpace(opts.Schema) != "" {
		config["schema"] = strings.TrimSpace(opts.Schema)
	}
	if strings.TrimSpace(opts.TablePrefix) != "" {
		config["table_prefix"] = strings.TrimSpace(opts.TablePrefix)
	}
	if err := provider.Configure(ctx, "cleanup-agent-session-state", config); err != nil {
		return fmt.Errorf("configure relationaldb provider: %w", err)
	}

	before, err := inspect(ctx, provider, dsn, opts)
	if err != nil {
		return err
	}
	writeSummary(stdout, "before", opts, before)

	if !opts.Execute {
		fmt.Fprintln(stdout, "mode: dry-run")
		fmt.Fprintln(stdout, "result: no records deleted")
		return nil
	}
	if err := validateExecuteGuards(opts, before); err != nil {
		return err
	}

	after, err := executeCleanup(ctx, provider, dsn, opts, before)
	if err != nil {
		return err
	}
	writeSummary(stdout, "after", opts, after)
	fmt.Fprintln(stdout, "mode: execute")
	fmt.Fprintf(stdout, "deleted_agent_session_relationships: %d\n", after.DeletedRelationships)
	fmt.Fprintf(stdout, "deleted_route_stores: %s\n", strings.Join(after.DeletedRouteStores, ","))
	return nil
}

func resolveDSN(opts options) (string, error) {
	if strings.TrimSpace(opts.DSN) != "" {
		return opts.DSN, nil
	}
	if strings.TrimSpace(opts.DSNEnv) == "" {
		return "", fmt.Errorf("dsn is required")
	}
	if value := os.Getenv(opts.DSNEnv); strings.TrimSpace(value) != "" {
		return value, nil
	}
	return "", fmt.Errorf("dsn is required; set --dsn or %s", opts.DSNEnv)
}

func validateGuards(opts options) error {
	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "project", got: opts.Project, want: opts.ExpectedProject},
		{name: "instance", got: opts.Instance, want: opts.ExpectedInstance},
		{name: "secret", got: opts.Secret, want: opts.ExpectedSecret},
		{name: "schema", got: opts.Schema, want: opts.ExpectedSchema},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.want) != "" && strings.TrimSpace(check.got) == "" {
			return fmt.Errorf("%s guard is required", check.name)
		}
		if strings.TrimSpace(check.got) != strings.TrimSpace(check.want) {
			return fmt.Errorf("%s guard mismatch: got %q, want %q", check.name, check.got, check.want)
		}
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	return nil
}

func validateExecuteGuards(opts options, before summary) error {
	if !opts.Execute {
		return nil
	}
	if opts.MaxDelete < 0 {
		return fmt.Errorf("--max-delete must be non-negative when --execute is set")
	}
	total := before.RelationshipRecords + before.SessionRoutes.Records + before.TurnRoutes.Records
	if total != opts.MaxDelete {
		return fmt.Errorf("refusing to delete %d records because --max-delete is %d; rerun dry-run and pass the exact count", total, opts.MaxDelete)
	}
	if strings.TrimSpace(opts.BackupConfirmation) == "" {
		return fmt.Errorf("--backup-confirmation is required when --execute is set")
	}
	if strings.TrimSpace(opts.ProviderVisibilityDeployedRef) == "" {
		return fmt.Errorf("--provider-visibility-deployed-ref is required when --execute is set")
	}
	if !opts.OldGestaltdDrained {
		return fmt.Errorf("--old-gestaltd-drained is required when --execute is set")
	}
	return nil
}

func inspect(ctx context.Context, provider *relationaldb.Provider, dsn string, opts options) (summary, error) {
	relationshipKeys, err := provider.IndexGetAllKeys(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  relationshipsStore,
		Index:  relationshipsIndex,
		Values: []any{agentSessionResourceType},
	})
	if err != nil {
		return summary{}, fmt.Errorf("count legacy agent_session relationships: %w", err)
	}
	sort.Strings(relationshipKeys)

	sessionRoutes, err := countStore(ctx, provider, agentSessionRouteStore)
	if err != nil {
		return summary{}, err
	}
	turnRoutes, err := countStore(ctx, provider, agentTurnRouteStore)
	if err != nil {
		return summary{}, err
	}
	raw, err := rawRouteCounts(ctx, dsn, opts, []string{agentSessionRouteStore, agentTurnRouteStore})
	if err != nil {
		return summary{}, err
	}

	return summary{
		RelationshipKeys:       relationshipKeys,
		RelationshipRecords:    int64(len(relationshipKeys)),
		RelationshipSampleKeys: redactKeys(relationshipKeys, 5),
		SessionRoutes:          sessionRoutes,
		TurnRoutes:             turnRoutes,
		RawRouteRows:           raw,
	}, nil
}

func countStore(ctx context.Context, provider *relationaldb.Provider, store string) (storeCount, error) {
	keys, err := provider.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store})
	if err == nil {
		sort.Strings(keys)
		return storeCount{Exists: true, Records: int64(len(keys)), Keys: keys}, nil
	}
	if isNotFound(err) {
		return storeCount{}, nil
	}
	return storeCount{}, fmt.Errorf("count %s: %w", store, err)
}

func executeCleanup(ctx context.Context, provider *relationaldb.Provider, dsn string, opts options, before summary) (summary, error) {
	deletedRelationships, err := deleteKeys(ctx, provider, relationshipsStore, before.RelationshipKeys)
	if err != nil {
		return summary{}, fmt.Errorf("delete legacy agent_session relationships: %w", err)
	}
	if deletedRelationships != before.RelationshipRecords {
		return summary{}, fmt.Errorf("deleted %d agent_session relationships, expected %d", deletedRelationships, before.RelationshipRecords)
	}

	var deletedRouteStores []string
	if before.SessionRoutes.Exists {
		if _, err := deleteKeys(ctx, provider, agentSessionRouteStore, before.SessionRoutes.Keys); err != nil {
			return summary{}, fmt.Errorf("delete %s records: %w", agentSessionRouteStore, err)
		}
		if err := verifyStoreEmpty(ctx, provider, agentSessionRouteStore); err != nil {
			return summary{}, err
		}
		if err := deleteEmptyObjectStoreMetadata(ctx, dsn, opts, agentSessionRouteStore); err != nil {
			return summary{}, err
		}
		deletedRouteStores = append(deletedRouteStores, agentSessionRouteStore)
	}
	if before.TurnRoutes.Exists {
		if _, err := deleteKeys(ctx, provider, agentTurnRouteStore, before.TurnRoutes.Keys); err != nil {
			return summary{}, fmt.Errorf("delete %s records: %w", agentTurnRouteStore, err)
		}
		if err := verifyStoreEmpty(ctx, provider, agentTurnRouteStore); err != nil {
			return summary{}, err
		}
		if err := deleteEmptyObjectStoreMetadata(ctx, dsn, opts, agentTurnRouteStore); err != nil {
			return summary{}, err
		}
		deletedRouteStores = append(deletedRouteStores, agentTurnRouteStore)
	}

	after, err := inspect(ctx, provider, dsn, opts)
	if err != nil {
		return summary{}, err
	}
	after.DeletedRelationships = deletedRelationships
	after.DeletedRouteStores = deletedRouteStores
	if after.RelationshipRecords != 0 {
		return summary{}, fmt.Errorf("post-delete verification found %d remaining agent_session relationships", after.RelationshipRecords)
	}
	if after.SessionRoutes.Exists || after.TurnRoutes.Exists {
		return summary{}, fmt.Errorf("post-delete verification found remaining route object stores")
	}
	for store, rows := range after.RawRouteRows {
		if rows.MetadataRows != 0 || rows.RecordRows != 0 || rows.IndexRows != 0 || rows.UniqueIndexRows != 0 {
			return summary{}, fmt.Errorf("post-delete physical verification found remaining rows for %s: %+v", store, rows)
		}
	}
	return after, nil
}

func deleteKeys(ctx context.Context, provider *relationaldb.Provider, store string, keys []string) (int64, error) {
	var deleted int64
	for _, key := range keys {
		if err := provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: store, ID: key}); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func verifyStoreEmpty(ctx context.Context, provider *relationaldb.Provider, store string) error {
	count, err := countStore(ctx, provider, store)
	if err != nil {
		return err
	}
	if count.Records != 0 {
		return fmt.Errorf("refusing to delete %s object-store metadata because %d unconfirmed records remain", store, count.Records)
	}
	return nil
}

func deleteEmptyObjectStoreMetadata(ctx context.Context, dsn string, opts options, store string) error {
	db, cfg, err := openRawDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := rawStoreCount(ctx, db, cfg, opts, store)
	if err != nil {
		return err
	}
	if rows.RecordRows != 0 || rows.IndexRows != 0 || rows.UniqueIndexRows != 0 {
		return fmt.Errorf("refusing to delete %s object-store metadata because raw rows remain: %+v", store, rows)
	}

	schema := strings.TrimSpace(opts.Schema)
	metadataTable := qualifyTable(cfg.Dialect, schema, "_gestalt_stores")
	recordsTable := qualifyTable(cfg.Dialect, schema, tableName(opts.TablePrefix, "_gestalt_records"))
	indexTable := qualifyTable(cfg.Dialect, schema, tableName(opts.TablePrefix, "_gestalt_index_entries"))
	uniqueIndexTable := qualifyTable(cfg.Dialect, schema, tableName(opts.TablePrefix, "_gestalt_unique_index_entries"))
	storeNameColumn := quoteIdent(cfg.Dialect, "store_name")
	query := "DELETE FROM " + metadataTable +
		" WHERE " + quoteIdent(cfg.Dialect, "name") + " = ?" +
		" AND NOT EXISTS (SELECT 1 FROM " + recordsTable + " WHERE " + storeNameColumn + " = ?)" +
		" AND NOT EXISTS (SELECT 1 FROM " + indexTable + " WHERE " + storeNameColumn + " = ?)" +
		" AND NOT EXISTS (SELECT 1 FROM " + uniqueIndexTable + " WHERE " + storeNameColumn + " = ?)"
	result, err := db.ExecContext(ctx, rebind(cfg.Bind, query), metadataStoreKey(cfg.Dialect, opts, store), store, store, store)
	if err != nil {
		return fmt.Errorf("delete %s object-store metadata: %w", store, err)
	}
	deleted, _ := result.RowsAffected()
	if deleted != 1 {
		current, countErr := rawStoreCount(ctx, db, cfg, opts, store)
		if countErr != nil {
			return fmt.Errorf("delete %s object-store metadata affected %d rows and recount failed: %w", store, deleted, countErr)
		}
		return fmt.Errorf("delete %s object-store metadata affected %d rows; current raw rows: %+v", store, deleted, current)
	}
	return nil
}

func validateMetadataPreflight(ctx context.Context, dsn string, opts options) error {
	db, cfg, err := openRawDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	schemaJSON, err := readMetadataSchema(ctx, db, cfg, opts, relationshipsStore)
	if err != nil {
		return err
	}
	var schema struct {
		Indexes []struct {
			Name    string   `json:"name"`
			KeyPath []string `json:"key_path"`
		} `json:"indexes"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return fmt.Errorf("decode %s object-store metadata: %w", relationshipsStore, err)
	}
	for _, idx := range schema.Indexes {
		if idx.Name == relationshipsIndex && sameStrings(idx.KeyPath, []string{"resource_type", "resource_id"}) {
			return nil
		}
	}
	return fmt.Errorf("%s object store is missing %s index on [resource_type resource_id]", relationshipsStore, relationshipsIndex)
}

func readMetadataSchema(ctx context.Context, db *sql.DB, cfg relationaldb.DriverConfig, opts options, store string) (string, error) {
	table := qualifyTable(cfg.Dialect, strings.TrimSpace(opts.Schema), "_gestalt_stores")
	query := rebind(cfg.Bind, "SELECT "+quoteIdent(cfg.Dialect, "schema_json")+" FROM "+table+" WHERE "+quoteIdent(cfg.Dialect, "name")+" = ?")
	var schemaJSON string
	if err := db.QueryRowContext(ctx, query, metadataStoreKey(cfg.Dialect, opts, store)).Scan(&schemaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%s object-store metadata not found; refusing to touch this database", store)
		}
		return "", fmt.Errorf("read %s object-store metadata: %w", store, err)
	}
	return schemaJSON, nil
}

func rawRouteCounts(ctx context.Context, dsn string, opts options, stores []string) (map[string]rawStoreRows, error) {
	db, cfg, err := openRawDB(dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	out := make(map[string]rawStoreRows, len(stores))
	for _, store := range stores {
		rows, err := rawStoreCount(ctx, db, cfg, opts, store)
		if err != nil {
			return nil, err
		}
		out[store] = rows
	}
	return out, nil
}

func rawStoreCount(ctx context.Context, db *sql.DB, cfg relationaldb.DriverConfig, opts options, store string) (rawStoreRows, error) {
	storeKey := metadataStoreKey(cfg.Dialect, opts, store)
	count := func(table, where string, args ...any) (int64, error) {
		query := rebind(cfg.Bind, "SELECT COUNT(*) FROM "+qualifyTable(cfg.Dialect, strings.TrimSpace(opts.Schema), table)+" WHERE "+where)
		var value int64
		if err := db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
			return 0, err
		}
		return value, nil
	}
	metadataRows, err := count("_gestalt_stores", quoteIdent(cfg.Dialect, "name")+" = ?", storeKey)
	if err != nil {
		return rawStoreRows{}, fmt.Errorf("raw count %s metadata rows: %w", store, err)
	}
	recordRows, err := count(tableName(opts.TablePrefix, "_gestalt_records"), quoteIdent(cfg.Dialect, "store_name")+" = ?", store)
	if err != nil {
		return rawStoreRows{}, fmt.Errorf("raw count %s record rows: %w", store, err)
	}
	indexRows, err := count(tableName(opts.TablePrefix, "_gestalt_index_entries"), quoteIdent(cfg.Dialect, "store_name")+" = ?", store)
	if err != nil {
		return rawStoreRows{}, fmt.Errorf("raw count %s index rows: %w", store, err)
	}
	uniqueIndexRows, err := count(tableName(opts.TablePrefix, "_gestalt_unique_index_entries"), quoteIdent(cfg.Dialect, "store_name")+" = ?", store)
	if err != nil {
		return rawStoreRows{}, fmt.Errorf("raw count %s unique index rows: %w", store, err)
	}
	return rawStoreRows{
		MetadataRows:    metadataRows,
		RecordRows:      recordRows,
		IndexRows:       indexRows,
		UniqueIndexRows: uniqueIndexRows,
	}, nil
}

func openRawDB(dsn string) (*sql.DB, relationaldb.DriverConfig, error) {
	cfg := relationaldb.ParseDSN(dsn)
	db, err := sql.Open(cfg.Driver, cfg.ConnString)
	if err != nil {
		return nil, cfg, fmt.Errorf("open raw relationaldb connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, cfg, fmt.Errorf("ping raw relationaldb connection: %w", err)
	}
	return db, cfg, nil
}

func metadataStoreKey(dialect relationaldb.Dialect, opts options, store string) string {
	if dialect == relationaldb.DialectSQLite && strings.TrimSpace(opts.Schema) == "" && strings.TrimSpace(opts.TablePrefix) != "" {
		return strings.TrimSpace(opts.TablePrefix) + store
	}
	return store
}

func tableName(prefix, name string) string {
	return strings.TrimSpace(prefix) + name
}

func qualifyTable(dialect relationaldb.Dialect, schema, table string) string {
	quotedTable := quoteIdent(dialect, table)
	if strings.TrimSpace(schema) == "" {
		return quotedTable
	}
	return quoteIdent(dialect, strings.TrimSpace(schema)) + "." + quotedTable
}

func quoteIdent(dialect relationaldb.Dialect, name string) string {
	if dialect == relationaldb.DialectMySQL {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func rebind(style relationaldb.BindStyle, query string) string {
	if style == relationaldb.BindQuestion {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 16)
	n := 0
	for _, ch := range query {
		if ch != '?' {
			b.WriteRune(ch)
			continue
		}
		n++
		switch style {
		case relationaldb.BindDollar:
			fmt.Fprintf(&b, "$%d", n)
		case relationaldb.BindAtP:
			fmt.Fprintf(&b, "@p%d", n)
		default:
			b.WriteRune('?')
		}
	}
	return b.String()
}

func writeSummary(stdout io.Writer, label string, opts options, s summary) {
	fmt.Fprintf(stdout, "summary: %s\n", label)
	fmt.Fprintf(stdout, "target_project: %s\n", opts.Project)
	fmt.Fprintf(stdout, "target_instance: %s\n", opts.Instance)
	fmt.Fprintf(stdout, "target_secret: %s\n", opts.Secret)
	fmt.Fprintf(stdout, "target_schema: %s\n", opts.Schema)
	fmt.Fprintf(stdout, "agent_session_relationships: %d\n", s.RelationshipRecords)
	if len(s.RelationshipSampleKeys) > 0 {
		fmt.Fprintf(stdout, "agent_session_relationship_sample_keys: %s\n", strings.Join(s.RelationshipSampleKeys, ","))
	}
	fmt.Fprintf(stdout, "agent_session_route_store_exists: %t\n", s.SessionRoutes.Exists)
	fmt.Fprintf(stdout, "agent_session_route_records: %d\n", s.SessionRoutes.Records)
	fmt.Fprintf(stdout, "agent_turn_route_store_exists: %t\n", s.TurnRoutes.Exists)
	fmt.Fprintf(stdout, "agent_turn_route_records: %d\n", s.TurnRoutes.Records)
	for _, store := range []string{agentSessionRouteStore, agentTurnRouteStore} {
		rows := s.RawRouteRows[store]
		fmt.Fprintf(stdout, "%s_raw_metadata_rows: %d\n", store, rows.MetadataRows)
		fmt.Fprintf(stdout, "%s_raw_record_rows: %d\n", store, rows.RecordRows)
		fmt.Fprintf(stdout, "%s_raw_index_rows: %d\n", store, rows.IndexRows)
		fmt.Fprintf(stdout, "%s_raw_unique_index_rows: %d\n", store, rows.UniqueIndexRows)
	}
}

func redactKeys(keys []string, limit int) []string {
	if limit <= 0 || len(keys) == 0 {
		return nil
	}
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, redactKey(key))
	}
	return out
}

func redactKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gestalt.ErrNotFound) {
		return true
	}
	return status.Code(err) == codes.NotFound
}
