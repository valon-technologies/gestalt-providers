package relationaldb

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const databaseTableName = "_gestalt_databases"
const databaseConnectionsTableName = "_gestalt_database_connections"

const connectionLeaseDuration = 30 * time.Second
const connectionHeartbeatInterval = 10 * time.Second

type dbPhase int

const (
	dbPhaseIdle dbPhase = iota
	dbPhaseBlockingUpgrade
	dbPhaseUpgrading
	dbPhaseDeleting
)

type relationalFactory struct {
	mu            sync.Mutex
	db            *sql.DB
	ownsDB        bool
	id            string
	registryKey   string
	bind          bindStyle
	dialect       dialect
	schemaName    string
	tablePrefix   string
	conn          connectionOptions
	states        map[string]*databaseState
	nextHandleID  uint64
	waitCh        chan struct{}
	stopHeartbeat chan struct{}
	closed        bool
	finalized     bool
}

type databaseState struct {
	name      string
	exists    bool
	version   uint64
	namespace string
	handles   map[uint64]*databaseHandle
	activeTx  int
	phase     dbPhase
}

type databaseHandle struct {
	id              uint64
	closed          bool
	activeTx        int
	onVersionChange func(context.Context, VersionChangeInfo) error
}

type databaseMetadata struct {
	name      string
	version   uint64
	namespace string
}

type versionChangeCallback func(context.Context, VersionChangeInfo) error

var factoryRegistry = struct {
	sync.Mutex
	factories map[string]map[*relationalFactory]struct{}
}{factories: make(map[string]map[*relationalFactory]struct{})}

func OpenFactoryDSN(dsn string, opts Options) (Factory, error) {
	driver, connStr, style, d := parseDSN(dsn)
	storeOpts := opts.storeOptions()
	if d == dialectSQLite && storeOpts.Schema != "" {
		return nil, fmt.Errorf("relationaldb: schema is not supported for sqlite")
	}
	if err := ensureRelationalTargetExists(dsn, storeOpts); err != nil {
		return nil, err
	}
	db, err := openConfiguredDB(driver, connStr, storeOpts.Connection)
	if err != nil {
		return nil, fmt.Errorf("relationaldb: open: %w", err)
	}
	if err := pingDatabase(context.Background(), db, storeOpts.Connection); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: ping: %w", err)
	}
	f, err := newRelationalFactory(db, style, d, storeOpts, true, factoryRegistryKey("dsn", driver, connStr, fmt.Sprint(d)))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return f, nil
}

func OpenFactoryDB(db *sql.DB, cfg DriverConfig, opts Options) (Factory, error) {
	if db == nil {
		return nil, status.Error(codes.InvalidArgument, "relationaldb: db is required")
	}
	_, _, style, d := cfg.internal()
	storeOpts := opts.storeOptions()
	if d == dialectSQLite && storeOpts.Schema != "" {
		return nil, fmt.Errorf("relationaldb: schema is not supported for sqlite")
	}
	storeOpts.Connection.apply(db)
	if err := pingDatabase(context.Background(), db, storeOpts.Connection); err != nil {
		return nil, fmt.Errorf("relationaldb: ping: %w", err)
	}
	if err := ensureRelationalNamespace(context.Background(), db, d, storeOpts.Schema, storeOpts.Connection); err != nil {
		return nil, err
	}
	return newRelationalFactory(db, style, d, storeOpts, false, factoryRegistryKey("db", fmt.Sprintf("%p", db), fmt.Sprint(style), fmt.Sprint(d)))
}

func newRelationalFactory(db *sql.DB, style bindStyle, d dialect, opts storeOptions, ownsDB bool, registryKey string) (*relationalFactory, error) {
	f := &relationalFactory{
		db:            db,
		ownsDB:        ownsDB,
		id:            newFactoryID(),
		registryKey:   factoryRegistryKey(registryKey, opts.Schema, opts.TablePrefix),
		bind:          style,
		dialect:       d,
		schemaName:    opts.Schema,
		tablePrefix:   opts.TablePrefix,
		conn:          opts.Connection,
		states:        make(map[string]*databaseState),
		waitCh:        make(chan struct{}),
		stopHeartbeat: make(chan struct{}),
	}
	if err := f.ensureFactoryStorage(context.Background()); err != nil {
		return nil, err
	}
	registerFactory(f)
	go f.heartbeatConnections()
	return f, nil
}

func newFactoryID() string {
	var raw [16]byte
	if _, err := crand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%p", &raw)))
	return hex.EncodeToString(sum[:16])
}

func factoryRegistryKey(parts ...string) string {
	return strings.Join(parts, "\x00")
}

func registerFactory(f *relationalFactory) {
	factoryRegistry.Lock()
	defer factoryRegistry.Unlock()
	group := factoryRegistry.factories[f.registryKey]
	if group == nil {
		group = make(map[*relationalFactory]struct{})
		factoryRegistry.factories[f.registryKey] = group
	}
	group[f] = struct{}{}
}

func unregisterFactory(f *relationalFactory) {
	factoryRegistry.Lock()
	defer factoryRegistry.Unlock()
	group := factoryRegistry.factories[f.registryKey]
	if group == nil {
		return
	}
	delete(group, f)
	if len(group) == 0 {
		delete(factoryRegistry.factories, f.registryKey)
	}
}

func (f *relationalFactory) Open(ctx context.Context, name string, opts OpenOptions) (Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Version != nil && *opts.Version == 0 {
		return nil, status.Error(codes.InvalidArgument, "explicit version 0 is invalid")
	}

	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "factory is closed")
		}
		state, err := f.loadStateLocked(ctx, name)
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		if state.phase != dbPhaseIdle {
			waitCh := f.waitCh
			f.mu.Unlock()
			if err := waitForFactoryChange(ctx, waitCh); err != nil {
				return nil, err
			}
			continue
		}

		requested := state.version
		if requested == 0 {
			requested = 1
		}
		if opts.Version != nil {
			requested = *opts.Version
		}
		if requested > math.MaxInt64 {
			f.mu.Unlock()
			return nil, status.Error(codes.InvalidArgument, "requested version exceeds portable SQL integer range")
		}
		if state.version > 0 && requested < state.version {
			f.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "requested version is lower than current version")
		}
		if state.version == 0 && state.exists {
			f.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "database open is in progress")
		}
		if state.version == requested && state.version > 0 {
			namespace := state.namespace
			f.mu.Unlock()
			store, err := f.storeForNamespace(namespace)
			if err != nil {
				return nil, err
			}
			db, ok, err := f.openDatabaseIfCurrent(ctx, name, namespace, requested, store, opts.OnVersionChange)
			if err != nil {
				return nil, err
			}
			if ok {
				return db, nil
			}
			continue
		}

		oldVersion := state.version
		namespace := state.namespace
		initialHandleID := uint64(0)
		initialConnectionRegistered := false
		cleanupInitialConnection := func() {
			if initialConnectionRegistered {
				_ = f.unregisterConnection(context.Background(), initialHandleID)
				initialConnectionRegistered = false
			}
		}
		if oldVersion == 0 {
			f.nextHandleID++
			initialHandleID = f.nextHandleID
			f.mu.Unlock()
			if err := f.insertConnectionAutoCommit(ctx, name, initialHandleID); err != nil {
				return nil, err
			}
			initialConnectionRegistered = true
			f.mu.Lock()
			if f.closed {
				f.mu.Unlock()
				cleanupInitialConnection()
				return nil, status.Error(codes.FailedPrecondition, "factory is closed")
			}
			state = f.states[name]
			if state == nil || state.phase != dbPhaseIdle || state.version != oldVersion || state.namespace != namespace || state.exists {
				waitCh := f.waitCh
				f.mu.Unlock()
				cleanupInitialConnection()
				if err := waitForFactoryChangeOrPoll(ctx, waitCh); err != nil {
					return nil, err
				}
				continue
			}
		}
		state.phase = dbPhaseBlockingUpgrade
		f.mu.Unlock()

		callbacks := f.handleSnapshotForDatabase(name)
		runVersionChangeCallbacks(ctx, callbacks, VersionChangeInfo{
			Name:       name,
			OldVersion: oldVersion,
			NewVersion: &requested,
			Reason:     VersionChangeUpgrade,
		})

		waitBlocked := false
		for {
			f.mu.Lock()
			state = f.states[name]
			if f.closed {
				if state != nil && state.phase == dbPhaseBlockingUpgrade {
					state.phase = dbPhaseIdle
					f.notifyLocked()
				}
				f.mu.Unlock()
				cleanupInitialConnection()
				return nil, status.Error(codes.FailedPrecondition, "factory is closed")
			}
			if state == nil {
				f.mu.Unlock()
				cleanupInitialConnection()
				break
			}
			if state.phase == dbPhaseIdle && state.version == requested && state.version > 0 {
				namespace := state.namespace
				f.mu.Unlock()
				cleanupInitialConnection()
				store, err := f.storeForNamespace(namespace)
				if err != nil {
					return nil, err
				}
				db, ok, err := f.openDatabaseIfCurrent(ctx, name, namespace, requested, store, opts.OnVersionChange)
				if err != nil {
					return nil, err
				}
				if ok {
					return db, nil
				}
				continue
			}
			if state.phase == dbPhaseIdle && state.version != oldVersion {
				f.mu.Unlock()
				cleanupInitialConnection()
				continue
			}
			if state.phase != dbPhaseBlockingUpgrade {
				waitCh := f.waitCh
				f.mu.Unlock()
				cleanupInitialConnection()
				if err := waitForFactoryChange(ctx, waitCh); err != nil {
					return nil, err
				}
				continue
			}
			waitCh := f.waitCh
			localBlockers := state.blockerCount()
			f.mu.Unlock()
			info := BlockedInfo{Name: name, OldVersion: oldVersion, NewVersion: &requested, Reason: VersionChangeUpgrade}
			blocked := false
			if oldVersion > 0 {
				var err error
				info, blocked, err = f.sqlBlockedInfo(ctx, name, oldVersion, &requested, VersionChangeUpgrade, initialHandleID)
				if err != nil {
					cleanupInitialConnection()
					return nil, err
				}
			}
			if !blocked && localBlockers == 0 {
				f.mu.Lock()
				state = f.states[name]
				if state == nil || state.phase != dbPhaseBlockingUpgrade {
					f.mu.Unlock()
					cleanupInitialConnection()
					continue
				}
				if state.blockerCount() != 0 {
					f.mu.Unlock()
					cleanupInitialConnection()
					continue
				}
				handleID := initialHandleID
				connectionRegistered := initialConnectionRegistered
				if handleID == 0 {
					f.nextHandleID++
					handleID = f.nextHandleID
				}
				state.phase = dbPhaseUpgrading
				f.mu.Unlock()
				db, err := f.runUpgrade(ctx, name, namespace, oldVersion, requested, handleID, connectionRegistered, opts.Upgrade, opts.OnVersionChange)
				initialConnectionRegistered = false
				if err != nil {
					f.resetPhase(name, dbPhaseUpgrading, oldVersion == 0)
					if isDatabaseStateChanged(err) {
						continue
					}
					return nil, err
				}
				return db, nil
			}
			if !waitBlocked {
				if err := handleBlocked(ctx, opts.OnBlocked, info); err != nil {
					f.resetPhase(name, dbPhaseBlockingUpgrade, oldVersion == 0)
					cleanupInitialConnection()
					return nil, err
				}
				waitBlocked = true
			}
			if err := waitForFactoryChangeOrPoll(ctx, waitCh); err != nil {
				f.resetPhase(name, dbPhaseBlockingUpgrade, oldVersion == 0)
				cleanupInitialConnection()
				return nil, err
			}
		}
	}
}

func (f *relationalFactory) OpenCurrent(ctx context.Context, name string) (Database, error) {
	return f.Open(ctx, name, OpenOptions{})
}

func (f *relationalFactory) DeleteDatabase(ctx context.Context, name string, opts ...DeleteOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	onBlocked := deleteOnBlocked(opts)
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return status.Error(codes.FailedPrecondition, "factory is closed")
		}
		state, err := f.loadStateLocked(ctx, name)
		if err != nil {
			f.mu.Unlock()
			return err
		}
		if state.phase != dbPhaseIdle {
			info := state.blockedInfo(nil, VersionChangeDelete)
			waitCh := f.waitCh
			f.mu.Unlock()
			if err := handleBlocked(ctx, onBlocked, info); err != nil {
				return err
			}
			if err := waitForFactoryChange(ctx, waitCh); err != nil {
				return err
			}
			continue
		}
		if state.version == 0 {
			info, blocked, err := f.sqlBlockedInfo(ctx, name, 0, nil, VersionChangeDelete, 0)
			if err != nil {
				f.mu.Unlock()
				return err
			}
			if blocked {
				waitCh := f.waitCh
				f.mu.Unlock()
				if err := handleBlocked(ctx, onBlocked, info); err != nil {
					return err
				}
				if err := waitForFactoryChangeOrPoll(ctx, waitCh); err != nil {
					return err
				}
				continue
			}
			if state.exists {
				f.mu.Unlock()
				return status.Error(codes.FailedPrecondition, "database open is in progress")
			}
			delete(f.states, name)
			f.mu.Unlock()
			return nil
		}
		state.phase = dbPhaseDeleting
		info := state.blockedInfo(nil, VersionChangeDelete)
		namespace := state.namespace
		f.mu.Unlock()

		callbacks := f.handleSnapshotForDatabase(name)
		runVersionChangeCallbacks(ctx, callbacks, VersionChangeInfo{
			Name:       name,
			OldVersion: info.OldVersion,
			NewVersion: nil,
			Reason:     VersionChangeDelete,
		})

		waitBlocked := false
		for {
			f.mu.Lock()
			state = f.states[name]
			if f.closed {
				if state != nil && state.phase == dbPhaseDeleting {
					state.phase = dbPhaseIdle
					f.notifyLocked()
				}
				f.mu.Unlock()
				return status.Error(codes.FailedPrecondition, "factory is closed")
			}
			if state == nil {
				f.mu.Unlock()
				return nil
			}
			if state.phase != dbPhaseDeleting {
				waitCh := f.waitCh
				f.mu.Unlock()
				if err := waitForFactoryChange(ctx, waitCh); err != nil {
					return err
				}
				continue
			}
			currentVersion := state.version
			waitCh := f.waitCh
			localBlockers := state.blockerCount()
			f.mu.Unlock()
			info, blocked, err := f.sqlBlockedInfo(ctx, name, currentVersion, nil, VersionChangeDelete, 0)
			if err != nil {
				return err
			}
			if !blocked && localBlockers == 0 {
				f.mu.Lock()
				state = f.states[name]
				if state == nil || state.phase != dbPhaseDeleting {
					f.mu.Unlock()
					continue
				}
				if state.blockerCount() != 0 {
					f.mu.Unlock()
					continue
				}
				currentVersion = state.version
				f.mu.Unlock()
				if err := f.deleteSQLDatabase(ctx, name, namespace, currentVersion); err != nil {
					f.resetPhase(name, dbPhaseDeleting, false)
					if isDatabaseStateChanged(err) {
						continue
					}
					return err
				}
				f.mu.Lock()
				delete(f.states, name)
				f.notifyLocked()
				f.mu.Unlock()
				return nil
			}
			if !waitBlocked {
				if err := handleBlocked(ctx, onBlocked, info); err != nil {
					f.resetPhase(name, dbPhaseDeleting, false)
					return err
				}
				waitBlocked = true
			}
			if err := waitForFactoryChangeOrPoll(ctx, waitCh); err != nil {
				f.resetPhase(name, dbPhaseDeleting, false)
				return err
			}
		}
	}
}

func (f *relationalFactory) Databases(ctx context.Context) ([]DatabaseInfo, error) {
	rows, err := queryWithRetry(ctx, f.db, f.conn,
		f.q("SELECT "+quoteIdent(f.dialect, "name")+", "+quoteIdent(f.dialect, "version")+
			" FROM "+quoteTableName(f.dialect, f.databaseTable())+
			" WHERE "+quoteIdent(f.dialect, "version")+" > 0"),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list databases: %v", err)
	}
	defer rows.Close()
	var out []DatabaseInfo
	for rows.Next() {
		var info DatabaseInfo
		if err := rows.Scan(&info.Name, &info.Version); err != nil {
			return nil, status.Errorf(codes.Internal, "scan database metadata: %v", err)
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate database metadata: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *relationalFactory) Cmp(a, b any) (int, error) {
	left, err := parseIndexedDBKey(a)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid left key: %v", err)
	}
	right, err := parseIndexedDBKey(b)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid right key: %v", err)
	}
	return compareIndexedDBKeys(left, right), nil
}

func (f *relationalFactory) Close() error {
	f.mu.Lock()
	alreadyClosed := f.closed
	if alreadyClosed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	for _, state := range f.states {
		for id, handle := range state.handles {
			handle.closed = true
			if handle.activeTx == 0 {
				delete(state.handles, id)
			}
		}
	}
	finalize := f.activeTransactionsLocked() == 0
	if finalize {
		f.finalized = true
		f.states = make(map[string]*databaseState)
		close(f.stopHeartbeat)
	}
	f.notifyLocked()
	f.mu.Unlock()

	unregisterFactory(f)
	_ = f.closeConnections(context.Background())
	if finalize {
		_ = f.cleanupInactiveConnections(context.Background())
		if f.ownsDB {
			return f.db.Close()
		}
	}
	return nil
}

func (f *relationalFactory) openDatabaseIfCurrent(ctx context.Context, name, namespace string, version uint64, store *Store, onVersionChange func(context.Context, VersionChangeInfo) error) (Database, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, false, status.Error(codes.FailedPrecondition, "factory is closed")
	}
	state := f.states[name]
	if state == nil || state.phase != dbPhaseIdle || state.version != version || state.namespace != namespace {
		return nil, false, nil
	}
	db, err := f.openDatabaseLocked(ctx, state, store, onVersionChange)
	if status.Code(err) == codes.Aborted {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return db, true, nil
}

func (f *relationalFactory) openDatabaseLocked(ctx context.Context, state *databaseState, store *Store, onVersionChange func(context.Context, VersionChangeInfo) error) (Database, error) {
	f.nextHandleID++
	handle := &databaseHandle{id: f.nextHandleID, onVersionChange: onVersionChange}
	if err := f.registerConnection(ctx, state.name, handle.id, state.version); err != nil {
		return nil, err
	}
	if state.handles == nil {
		state.handles = make(map[uint64]*databaseHandle)
	}
	state.handles[handle.id] = handle
	store.lifecycle = &storeLifecycle{check: f.storeLifecycleCheck(state.name, handle.id)}
	return &relationalDatabase{factory: f, store: store, name: state.name, version: state.version, handleID: handle.id}, nil
}

func (f *relationalFactory) openRegisteredDatabaseLocked(state *databaseState, store *Store, handleID uint64, onVersionChange func(context.Context, VersionChangeInfo) error) Database {
	handle := &databaseHandle{id: handleID, onVersionChange: onVersionChange}
	if state.handles == nil {
		state.handles = make(map[uint64]*databaseHandle)
	}
	state.handles[handle.id] = handle
	store.lifecycle = &storeLifecycle{check: f.storeLifecycleCheck(state.name, handle.id)}
	return &relationalDatabase{factory: f, store: store, name: state.name, version: state.version, handleID: handle.id}
}

func (f *relationalFactory) activeTransactionsLocked() int {
	var total int
	for _, state := range f.states {
		total += state.activeTx
	}
	return total
}

func (f *relationalFactory) loadStateLocked(ctx context.Context, name string) (*databaseState, error) {
	if state := f.states[name]; state != nil {
		if state.phase == dbPhaseIdle && state.blockerCount() == 0 {
			if err := f.refreshStateLocked(ctx, state); err != nil {
				return nil, err
			}
		}
		return state, nil
	}
	meta, err := f.loadDatabaseMetadata(ctx, name)
	if err != nil {
		return nil, err
	}
	state := &databaseState{name: name, namespace: f.logicalNamespace(name), handles: make(map[uint64]*databaseHandle)}
	if meta != nil {
		state.exists = true
		state.version = meta.version
		state.namespace = meta.namespace
	}
	f.states[name] = state
	return state, nil
}

func (f *relationalFactory) refreshStateLocked(ctx context.Context, state *databaseState) error {
	meta, err := f.loadDatabaseMetadata(ctx, state.name)
	if err != nil {
		return err
	}
	state.exists = false
	state.version = 0
	state.namespace = f.logicalNamespace(state.name)
	if meta != nil {
		state.exists = true
		state.version = meta.version
		state.namespace = meta.namespace
	}
	return nil
}

func (f *relationalFactory) notifyLocked() {
	close(f.waitCh)
	f.waitCh = make(chan struct{})
}

func (f *relationalFactory) resetPhase(name string, phase dbPhase, remove bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state := f.states[name]
	if state == nil || state.phase != phase {
		return
	}
	if remove {
		delete(f.states, name)
	} else {
		state.phase = dbPhaseIdle
	}
	f.notifyLocked()
}

func (s *databaseState) blockerCount() int {
	return s.openHandleCount() + s.activeTx
}

func (s *databaseState) openHandleCount() int {
	var count int
	for _, handle := range s.handles {
		if !handle.closed {
			count++
		}
	}
	return count
}

func (s *databaseState) blockedInfo(newVersion *uint64, reason VersionChangeReason) BlockedInfo {
	return BlockedInfo{
		Name:               s.name,
		OldVersion:         s.version,
		NewVersion:         newVersion,
		Reason:             reason,
		OpenConnections:    s.openHandleCount(),
		ActiveTransactions: s.activeTx,
	}
}

func (f *relationalFactory) storeLifecycleCheck(name string, handleID uint64) func(context.Context) error {
	return func(context.Context) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.closed {
			return status.Error(codes.FailedPrecondition, "factory is closed")
		}
		state := f.states[name]
		if state == nil || state.version == 0 {
			return status.Error(codes.NotFound, "database not found")
		}
		handle, ok := state.handles[handleID]
		if !ok || handle.closed {
			return status.Error(codes.FailedPrecondition, "database handle is closed")
		}
		if state.phase != dbPhaseIdle {
			return status.Error(codes.FailedPrecondition, "database has a pending version change or delete")
		}
		return nil
	}
}

func (s *databaseState) handleSnapshot() []versionChangeCallback {
	callbacks := make([]versionChangeCallback, 0, len(s.handles))
	for _, handle := range s.handles {
		if handle.closed {
			continue
		}
		callbacks = append(callbacks, handle.onVersionChange)
	}
	return callbacks
}

func (f *relationalFactory) handleSnapshotForDatabase(name string) []versionChangeCallback {
	factoryRegistry.Lock()
	peers := make([]*relationalFactory, 0, len(factoryRegistry.factories[f.registryKey]))
	for peer := range factoryRegistry.factories[f.registryKey] {
		peers = append(peers, peer)
	}
	factoryRegistry.Unlock()

	var callbacks []versionChangeCallback
	for _, peer := range peers {
		peer.mu.Lock()
		if !peer.closed {
			if state := peer.states[name]; state != nil {
				callbacks = append(callbacks, state.handleSnapshot()...)
			}
		}
		peer.mu.Unlock()
	}
	return callbacks
}

func (f *relationalFactory) ensureFactoryStorage(ctx context.Context) error {
	if _, err := execWithRetry(ctx, f.db, f.conn, f.q(databaseTableSQL(f.dialect, f.databaseTable()))); err != nil {
		return status.Errorf(codes.Internal, "ensure database metadata: %v", err)
	}
	if _, err := execWithRetry(ctx, f.db, f.conn, f.q(databaseConnectionsTableSQL(f.dialect, f.connectionsTable()))); err != nil {
		return status.Errorf(codes.Internal, "ensure database connection metadata: %v", err)
	}
	if err := f.ensureConnectionTableColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (f *relationalFactory) ensureConnectionTableColumns(ctx context.Context) error {
	columns, err := f.tableColumns(ctx, f.connectionsTable())
	if err != nil {
		return err
	}
	required := []struct {
		name string
		def  string
	}{
		{name: "closed", def: sqlType(f.dialect, 1, false) + " NOT NULL DEFAULT 0"},
		{name: "expires_at", def: sqlType(f.dialect, 1, false) + " NOT NULL DEFAULT 0"},
	}
	for _, col := range required {
		if _, ok := columns[col.name]; ok {
			continue
		}
		if _, err := execWithRetry(ctx, f.db, f.conn,
			f.q("ALTER TABLE "+quoteTableName(f.dialect, f.connectionsTable())+
				" ADD "+quoteIdent(f.dialect, col.name)+" "+col.def),
		); err != nil {
			return status.Errorf(codes.Internal, "migrate database connection metadata: %v", err)
		}
	}
	return nil
}

func (f *relationalFactory) tableColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	rows, err := queryWithRetry(ctx, f.db, f.conn, f.q("SELECT * FROM "+quoteTableName(f.dialect, table)+" WHERE 1 = 0"))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect table columns: %v", err)
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect table columns: %v", err)
	}
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		out[name] = struct{}{}
	}
	return out, nil
}

func (f *relationalFactory) registerConnection(ctx context.Context, name string, handleID uint64, version uint64) error {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "begin connection registration: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	txCtx := contextWithLifecycleBypass(contextWithTx(ctx, tx, nil))
	if err := f.lockDatabaseVersion(txCtx, tx, name, version); err != nil {
		return err
	}
	if err := f.insertConnection(txCtx, tx, name, handleID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return status.Errorf(codes.Internal, "commit connection registration: %v", err)
	}
	committed = true
	return nil
}

func (f *relationalFactory) insertConnectionAutoCommit(ctx context.Context, name string, handleID uint64) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("INSERT INTO "+quoteTableName(f.dialect, f.connectionsTable())+
			" ("+quoteIdent(f.dialect, "name")+", "+quoteIdent(f.dialect, "factory_id")+", "+quoteIdent(f.dialect, "handle_id")+", "+quoteIdent(f.dialect, "closed")+", "+quoteIdent(f.dialect, "active_transactions")+", "+quoteIdent(f.dialect, "expires_at")+") VALUES (?, ?, ?, ?, ?, ?)"),
		name,
		f.id,
		handleID,
		uint64(0),
		uint64(0),
		connectionLeaseExpiresAt(),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "register database connection: %v", err)
	}
	return nil
}

func (f *relationalFactory) insertConnection(ctx context.Context, tx *sql.Tx, name string, handleID uint64) error {
	if _, err := tx.ExecContext(ctx,
		f.q("INSERT INTO "+quoteTableName(f.dialect, f.connectionsTable())+
			" ("+quoteIdent(f.dialect, "name")+", "+quoteIdent(f.dialect, "factory_id")+", "+quoteIdent(f.dialect, "handle_id")+", "+quoteIdent(f.dialect, "closed")+", "+quoteIdent(f.dialect, "active_transactions")+", "+quoteIdent(f.dialect, "expires_at")+") VALUES (?, ?, ?, ?, ?, ?)"),
		name,
		f.id,
		handleID,
		uint64(0),
		uint64(0),
		connectionLeaseExpiresAt(),
	); err != nil {
		return status.Errorf(codes.Internal, "register database connection: %v", err)
	}
	return nil
}

func (f *relationalFactory) unregisterConnection(ctx context.Context, handleID uint64) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("DELETE FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ? AND "+quoteIdent(f.dialect, "handle_id")+" = ?"),
		f.id,
		handleID,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "unregister database connection: %v", err)
	}
	return nil
}

func (f *relationalFactory) cleanupInactiveConnections(ctx context.Context) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("DELETE FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ? AND "+quoteIdent(f.dialect, "active_transactions")+" = ?"),
		f.id,
		uint64(0),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "cleanup inactive database connections: %v", err)
	}
	return nil
}

func (f *relationalFactory) closeConnections(ctx context.Context) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("UPDATE "+quoteTableName(f.dialect, f.connectionsTable())+
			" SET "+quoteIdent(f.dialect, "closed")+" = ?"+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ?"),
		uint64(1),
		f.id,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "close database connections: %v", err)
	}
	return nil
}

func (f *relationalFactory) closeConnection(ctx context.Context, handleID uint64) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("UPDATE "+quoteTableName(f.dialect, f.connectionsTable())+
			" SET "+quoteIdent(f.dialect, "closed")+" = ?"+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ? AND "+quoteIdent(f.dialect, "handle_id")+" = ?"),
		uint64(1),
		f.id,
		handleID,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "close database connection: %v", err)
	}
	return nil
}

func (f *relationalFactory) updateActiveTransactions(ctx context.Context, handleID uint64, delta int) error {
	op := "+"
	value := delta
	if delta < 0 {
		op = "-"
		value = -delta
	}
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("UPDATE "+quoteTableName(f.dialect, f.connectionsTable())+
			" SET "+quoteIdent(f.dialect, "active_transactions")+" = "+quoteIdent(f.dialect, "active_transactions")+" "+op+" ?"+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ? AND "+quoteIdent(f.dialect, "handle_id")+" = ?"),
		value,
		f.id,
		handleID,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "update active transaction count: %v", err)
	}
	return nil
}

func (f *relationalFactory) heartbeatConnections() {
	ticker := time.NewTicker(connectionHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = f.refreshConnectionLeases(context.Background())
		case <-f.stopHeartbeat:
			return
		}
	}
}

func (f *relationalFactory) refreshConnectionLeases(ctx context.Context) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("UPDATE "+quoteTableName(f.dialect, f.connectionsTable())+
			" SET "+quoteIdent(f.dialect, "expires_at")+" = ?"+
			" WHERE "+quoteIdent(f.dialect, "factory_id")+" = ?"),
		connectionLeaseExpiresAt(),
		f.id,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "refresh connection leases: %v", err)
	}
	return nil
}

func (f *relationalFactory) cleanupExpiredConnections(ctx context.Context) error {
	_, err := execWithRetry(ctx, f.db, f.conn,
		f.q("DELETE FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+quoteIdent(f.dialect, "expires_at")+" < ?"),
		time.Now().UnixNano(),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "cleanup expired database connections: %v", err)
	}
	return nil
}

func connectionLeaseExpiresAt() int64 {
	return time.Now().Add(connectionLeaseDuration).UnixNano()
}

func (f *relationalFactory) sqlBlockedInfo(ctx context.Context, name string, oldVersion uint64, newVersion *uint64, reason VersionChangeReason, excludeHandleID uint64) (BlockedInfo, bool, error) {
	info := BlockedInfo{Name: name, OldVersion: oldVersion, NewVersion: newVersion, Reason: reason}
	if err := f.cleanupExpiredConnections(ctx); err != nil {
		if isDatabaseLockContention(err) {
			info.OpenConnections = 1
			return info, true, nil
		}
		return info, false, err
	}
	where := quoteIdent(f.dialect, "name") + " = ? AND " + quoteIdent(f.dialect, "expires_at") + " >= ?"
	args := []any{name, time.Now().UnixNano()}
	if excludeHandleID != 0 {
		where += " AND NOT (" + quoteIdent(f.dialect, "factory_id") + " = ? AND " + quoteIdent(f.dialect, "handle_id") + " = ?)"
		args = append(args, f.id, excludeHandleID)
	}
	err := queryRowScanWithRetry(ctx, f.db, f.conn,
		f.q("SELECT COALESCE(SUM(CASE WHEN "+quoteIdent(f.dialect, "closed")+" = 0 THEN 1 ELSE 0 END), 0), COALESCE(SUM("+quoteIdent(f.dialect, "active_transactions")+"), 0)"+
			" FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+where),
		args,
		&info.OpenConnections,
		&info.ActiveTransactions,
	)
	if err != nil {
		if isDatabaseLockContention(err) {
			info.OpenConnections = 1
			return info, true, nil
		}
		return info, false, status.Errorf(codes.Internal, "load database blockers: %v", err)
	}
	return info, info.OpenConnections > 0 || info.ActiveTransactions > 0, nil
}

func (f *relationalFactory) ensureNoSQLBlockersTx(ctx context.Context, tx *sql.Tx, name string, excludeHandleID uint64) error {
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx,
		f.q("DELETE FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+quoteIdent(f.dialect, "expires_at")+" < ?"),
		now,
	); err != nil {
		return status.Errorf(codes.Internal, "cleanup expired database connections: %v", err)
	}

	where := quoteIdent(f.dialect, "name") + " = ? AND " + quoteIdent(f.dialect, "expires_at") + " >= ?"
	args := []any{name, now}
	if excludeHandleID != 0 {
		where += " AND NOT (" + quoteIdent(f.dialect, "factory_id") + " = ? AND " + quoteIdent(f.dialect, "handle_id") + " = ?)"
		args = append(args, f.id, excludeHandleID)
	}

	var openConnections, activeTransactions int
	if err := tx.QueryRowContext(ctx,
		f.q("SELECT COALESCE(SUM(CASE WHEN "+quoteIdent(f.dialect, "closed")+" = 0 THEN 1 ELSE 0 END), 0), COALESCE(SUM("+quoteIdent(f.dialect, "active_transactions")+"), 0)"+
			" FROM "+quoteTableName(f.dialect, f.connectionsTable())+
			" WHERE "+where),
		args...,
	).Scan(&openConnections, &activeTransactions); err != nil {
		return status.Errorf(codes.Internal, "load database blockers: %v", err)
	}
	if openConnections > 0 || activeTransactions > 0 {
		return status.Error(codes.Aborted, "database blockers changed before migration")
	}
	return nil
}

func (f *relationalFactory) loadDatabaseMetadata(ctx context.Context, name string) (*databaseMetadata, error) {
	var meta databaseMetadata
	err := queryRowScanWithRetry(ctx, f.db, f.conn,
		f.q("SELECT "+quoteIdent(f.dialect, "name")+", "+quoteIdent(f.dialect, "version")+", "+quoteIdent(f.dialect, "namespace")+
			" FROM "+quoteTableName(f.dialect, f.databaseTable())+
			" WHERE "+quoteIdent(f.dialect, "name")+" = ?"),
		[]any{name},
		&meta.name,
		&meta.version,
		&meta.namespace,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load database metadata: %v", err)
	}
	if strings.TrimSpace(meta.namespace) == "" {
		meta.namespace = f.logicalNamespace(name)
	}
	return &meta, nil
}

func (f *relationalFactory) storeDatabaseVersion(ctx context.Context, tx *sql.Tx, name, namespace string, oldVersion, newVersion uint64) error {
	result, err := tx.ExecContext(ctx,
		f.q("UPDATE "+quoteTableName(f.dialect, f.databaseTable())+
			" SET "+quoteIdent(f.dialect, "version")+" = ?, "+quoteIdent(f.dialect, "namespace")+" = ?"+
			" WHERE "+quoteIdent(f.dialect, "name")+" = ? AND "+quoteIdent(f.dialect, "version")+" = ?"),
		newVersion,
		namespace,
		name,
		oldVersion,
	)
	if err != nil {
		return status.Errorf(codes.Internal, "store database version: %v", err)
	}
	rows, err := result.RowsAffected()
	if err == nil && rows != 1 {
		return status.Error(codes.Aborted, "database version changed during upgrade")
	}
	return nil
}

func (f *relationalFactory) insertInitialDatabaseVersion(ctx context.Context, tx *sql.Tx, name, namespace string) error {
	if _, err := tx.ExecContext(ctx,
		f.q("INSERT INTO "+quoteTableName(f.dialect, f.databaseTable())+
			" ("+quoteIdent(f.dialect, "name")+", "+quoteIdent(f.dialect, "version")+", "+quoteIdent(f.dialect, "namespace")+") VALUES (?, ?, ?)"),
		name,
		uint64(0),
		namespace,
	); err != nil {
		if isDuplicateErr(err) {
			return status.Error(codes.Aborted, "database version changed before migration")
		}
		return status.Errorf(codes.Internal, "reserve database metadata: %v", err)
	}
	return nil
}

func (f *relationalFactory) lockDatabaseVersion(ctx context.Context, tx *sql.Tx, name string, version uint64) error {
	var query string
	switch f.dialect {
	case dialectPostgres, dialectMySQL:
		query = "SELECT " + quoteIdent(f.dialect, "version") +
			" FROM " + quoteTableName(f.dialect, f.databaseTable()) +
			" WHERE " + quoteIdent(f.dialect, "name") + " = ? FOR UPDATE"
	case dialectSQLServer:
		query = "SELECT " + quoteIdent(f.dialect, "version") +
			" FROM " + quoteTableName(f.dialect, f.databaseTable()) + " WITH (UPDLOCK, HOLDLOCK)" +
			" WHERE " + quoteIdent(f.dialect, "name") + " = ?"
	default:
		if _, err := tx.ExecContext(ctx,
			f.q("UPDATE "+quoteTableName(f.dialect, f.databaseTable())+
				" SET "+quoteIdent(f.dialect, "version")+" = "+quoteIdent(f.dialect, "version")+
				" WHERE "+quoteIdent(f.dialect, "name")+" = ?"),
			name,
		); err != nil {
			return status.Errorf(codes.Internal, "lock database metadata: %v", err)
		}
		query = "SELECT " + quoteIdent(f.dialect, "version") +
			" FROM " + quoteTableName(f.dialect, f.databaseTable()) +
			" WHERE " + quoteIdent(f.dialect, "name") + " = ?"
	}

	var current uint64
	if err := tx.QueryRowContext(ctx, f.q(query), name).Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return status.Error(codes.Aborted, "database version changed before migration")
		}
		return status.Errorf(codes.Internal, "lock database metadata: %v", err)
	}
	if current != version {
		return status.Error(codes.Aborted, "database version changed before migration")
	}
	return nil
}

func (f *relationalFactory) runUpgrade(ctx context.Context, name, namespace string, oldVersion, newVersion, handleID uint64, connectionRegistered bool, upgrade func(context.Context, UpgradeContext) error, onVersionChange func(context.Context, VersionChangeInfo) error) (Database, error) {
	store, err := f.storeForNamespace(namespace)
	if err != nil {
		if connectionRegistered {
			_ = f.unregisterConnection(context.Background(), handleID)
		}
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		if connectionRegistered {
			_ = f.unregisterConnection(context.Background(), handleID)
		}
		return nil, status.Errorf(codes.Internal, "begin upgrade transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
			if connectionRegistered {
				_ = f.unregisterConnection(context.Background(), handleID)
			}
		}
	}()
	txCtx := contextWithLifecycleBypass(contextWithTx(ctx, tx, nil))
	if oldVersion == 0 {
		if err := f.insertInitialDatabaseVersion(txCtx, tx, name, namespace); err != nil {
			return nil, err
		}
	} else {
		if err := f.lockDatabaseVersion(txCtx, tx, name, oldVersion); err != nil {
			return nil, err
		}
	}
	if oldVersion > 0 {
		if err := f.ensureNoSQLBlockersTx(txCtx, tx, name, handleID); err != nil {
			return nil, err
		}
	}
	if upgrade != nil {
		up := &relationalUpgradeContext{
			db:         &upgradeDatabase{name: name, version: newVersion, store: store, ctx: txCtx},
			store:      store,
			ctx:        txCtx,
			oldVersion: oldVersion,
			newVersion: newVersion,
		}
		if err := safeUpgradeCallback(ctx, upgrade, up); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, status.Error(codes.FailedPrecondition, "factory is closed")
	}
	state := f.states[name]
	if state == nil || state.phase != dbPhaseUpgrading || state.version != oldVersion || state.namespace != namespace {
		return nil, status.Error(codes.Aborted, "database state changed during upgrade")
	}
	if err := f.storeDatabaseVersion(txCtx, tx, name, namespace, oldVersion, newVersion); err != nil {
		return nil, err
	}
	if !connectionRegistered {
		if err := f.insertConnection(txCtx, tx, name, handleID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit upgrade transaction: %v", err)
	}
	committed = true
	state.version = newVersion
	state.namespace = namespace
	state.exists = true
	state.phase = dbPhaseIdle
	db := f.openRegisteredDatabaseLocked(state, store, handleID, onVersionChange)
	f.notifyLocked()
	return db, nil
}

func (f *relationalFactory) deleteSQLDatabase(ctx context.Context, name, namespace string, expectedVersion uint64) error {
	store, err := f.storeForNamespace(namespace)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "begin delete transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	txCtx := contextWithLifecycleBypass(contextWithTx(ctx, tx, nil))
	if err := f.lockDatabaseVersion(txCtx, tx, name, expectedVersion); err != nil {
		return err
	}
	if err := f.ensureNoSQLBlockersTx(txCtx, tx, name, 0); err != nil {
		return err
	}
	metadataKeys, err := store.metadataKeysWithPrefix(txCtx, namespace)
	if err != nil {
		return err
	}
	storeNames := make([]string, 0, len(metadataKeys))
	for _, key := range metadataKeys {
		storeNames = append(storeNames, strings.TrimPrefix(key, namespace))
	}
	for _, storeName := range storeNames {
		if err := store.clearGenericStoreTables(txCtx, tx, storeName); err != nil {
			return err
		}
	}
	for _, key := range metadataKeys {
		if _, err := tx.ExecContext(txCtx,
			f.q("DELETE FROM "+quoteTableName(f.dialect, store.metadataTable())+
				" WHERE "+quoteIdent(f.dialect, "name")+" = ?"),
			key,
		); err != nil {
			return status.Errorf(codes.Internal, "delete object store metadata: %v", err)
		}
	}
	if _, err := tx.ExecContext(txCtx,
		f.q("DELETE FROM "+quoteTableName(f.dialect, f.databaseTable())+" WHERE "+quoteIdent(f.dialect, "name")+" = ?"),
		name,
	); err != nil {
		return status.Errorf(codes.Internal, "delete database metadata: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return status.Errorf(codes.Internal, "commit delete transaction: %v", err)
	}
	committed = true
	return nil
}

func (f *relationalFactory) storeForNamespace(namespace string) (*Store, error) {
	return newStoreWithDB(f.db, f.bind, f.dialect, storeOptions{
		TablePrefix:       namespace,
		Schema:            f.schemaName,
		Connection:        f.conn,
		MetadataKeyPrefix: true,
	}, false)
}

func (f *relationalFactory) databaseTable() string {
	return qualifyTableName(f.schemaName, f.tablePrefix+databaseTableName)
}

func (f *relationalFactory) connectionsTable() string {
	return qualifyTableName(f.schemaName, f.tablePrefix+databaseConnectionsTableName)
}

func (f *relationalFactory) logicalNamespace(name string) string {
	sum := sha256.Sum256([]byte(name))
	return strings.TrimSpace(f.tablePrefix) + "db_" + hex.EncodeToString(sum[:12]) + "_"
}

func (f *relationalFactory) q(query string) string {
	return rebind(f.bind, query)
}

func deleteOnBlocked(opts []DeleteOptions) func(context.Context, BlockedInfo) (BlockedAction, error) {
	if len(opts) == 0 {
		return nil
	}
	return opts[0].OnBlocked
}

func handleBlocked(ctx context.Context, fn func(context.Context, BlockedInfo) (BlockedAction, error), info BlockedInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return status.Error(codes.FailedPrecondition, "operation is blocked by open connections or active transactions")
	}
	action, err := safeBlockedCallback(ctx, fn, info)
	if err != nil {
		return status.Errorf(codes.Aborted, "blocked callback failed: %v", err)
	}
	switch action {
	case BlockedFail:
		return status.Error(codes.FailedPrecondition, "operation is blocked by open connections or active transactions")
	case BlockedWait:
		return nil
	default:
		return status.Error(codes.InvalidArgument, "invalid blocked action")
	}
}

func isDatabaseStateChanged(err error) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted {
		return false
	}
	msg := st.Message()
	return strings.Contains(msg, "database version changed") || strings.Contains(msg, "database blockers changed")
}

func isDatabaseLockContention(err error) bool {
	if err == nil {
		return false
	}
	if isRetryableDatabaseContentionError(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"database is locked",
		"database table is locked",
		"lock wait timeout exceeded",
		"deadlock found",
		"deadlock detected",
		"lock request time out period exceeded",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func safeBlockedCallback(ctx context.Context, fn func(context.Context, BlockedInfo) (BlockedAction, error), info BlockedInfo) (action BlockedAction, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return fn(ctx, info)
}

func runVersionChangeCallbacks(ctx context.Context, callbacks []versionChangeCallback, info VersionChangeInfo) {
	for _, callback := range callbacks {
		if callback == nil {
			continue
		}
		_ = safeVersionChangeCallback(ctx, callback, info)
	}
}

func safeVersionChangeCallback(ctx context.Context, fn versionChangeCallback, info VersionChangeInfo) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return fn(ctx, info)
}

func safeUpgradeCallback(ctx context.Context, fn func(context.Context, UpgradeContext) error, up UpgradeContext) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = status.Errorf(codes.Aborted, "upgrade callback panic: %v", recovered)
		}
	}()
	if err := fn(ctx, up); err != nil {
		return status.Errorf(codes.Aborted, "upgrade callback failed: %v", err)
	}
	return nil
}

func waitForFactoryChange(ctx context.Context, waitCh <-chan struct{}) error {
	select {
	case <-waitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForFactoryChangeOrPoll(ctx context.Context, waitCh <-chan struct{}) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-waitCh:
		return nil
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
