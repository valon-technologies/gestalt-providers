package relationaldb

import (
	"context"
	"net"
	"testing"
	"time"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const cursorTestBufSize = 1024 * 1024

type cursorHarness struct {
	store  *Store
	client proto.IndexedDBClient
}

func newCursorHarness(t *testing.T) *cursorHarness {
	t.Helper()

	store := testStore(t)
	listener := bufconn.Listen(cursorTestBufSize)
	server := grpc.NewServer()
	proto.RegisterIndexedDBServer(server, store)
	go func() {
		_ = server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	return &cursorHarness{
		store:  store,
		client: proto.NewIndexedDBClient(conn),
	}
}

func cursorItemsSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_status", KeyPath: []string{"status"}},
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: 0},
			{Name: "status", Type: 0},
			{Name: "email", Type: 0},
		},
	}
}

func makeCursorItem(id, name, status, email string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":     id,
		"name":   name,
		"status": status,
		"email":  email,
	})
	return record
}

func cursorNumberSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 1, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: 0},
		},
	}
}

func makeCursorNumberItem(id int64, name string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":   id,
		"name": name,
	})
	return record
}

func cursorBytesSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 5, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: 0},
		},
	}
}

func makeCursorBytesItem(id []byte, name string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":   id,
		"name": name,
	})
	return record
}

func seedCursorNumbers(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if _, err := store.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "numbers", Schema: cursorNumberSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore(numbers): %v", err)
	}

	for _, record := range []*proto.Record{
		makeCursorNumberItem(2, "two"),
		makeCursorNumberItem(10, "ten"),
		makeCursorNumberItem(1, "one"),
	} {
		if _, err := store.Add(ctx, &proto.RecordRequest{Store: "numbers", Record: record}); err != nil {
			t.Fatalf("Add(numbers): %v", err)
		}
	}
}

func seedCursorBytes(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if _, err := store.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "blobs", Schema: cursorBytesSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore(blobs): %v", err)
	}

	if _, err := store.Add(ctx, &proto.RecordRequest{
		Store:  "blobs",
		Record: makeCursorBytesItem([]byte("blob-1"), "Blob One"),
	}); err != nil {
		t.Fatalf("Add(blobs): %v", err)
	}
}

func seedCursorItems(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if _, err := store.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "items", Schema: cursorItemsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	for _, record := range []*proto.Record{
		makeCursorItem("a", "Alice", "active", "alice@test.com"),
		makeCursorItem("b", "Bob", "active", "bob@test.com"),
		makeCursorItem("c", "Carol", "inactive", "carol@test.com"),
		makeCursorItem("d", "Dave", "active", "dave@test.com"),
	} {
		if _, err := store.Add(ctx, &proto.RecordRequest{Store: "items", Record: record}); err != nil {
			t.Fatalf("Add(%q): %v", record.Fields["id"].GetStringValue(), err)
		}
	}
}

func openCursorStream(t *testing.T, client proto.IndexedDBClient, req *proto.OpenCursorRequest) proto.IndexedDB_OpenCursorClient {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	stream, err := client.OpenCursor(ctx)
	if err != nil {
		cancel()
		t.Fatalf("OpenCursor: %v", err)
	}
	t.Cleanup(func() {
		_ = closeCursorStream(stream)
		cancel()
	})

	if err := stream.Send(&proto.CursorClientMessage{
		Msg: &proto.CursorClientMessage_Open{Open: req},
	}); err != nil {
		t.Fatalf("Send(open): %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(open ack): %v", err)
	}
	done, ok := resp.GetResult().(*proto.CursorResponse_Done)
	if !ok || done.Done {
		t.Fatalf("open ack = %T %+v, want done=false", resp.GetResult(), resp)
	}

	return stream
}

func closeCursorStream(stream proto.IndexedDB_OpenCursorClient) error {
	if stream == nil {
		return nil
	}
	_ = stream.Send(&proto.CursorClientMessage{
		Msg: &proto.CursorClientMessage_Command{
			Command: &proto.CursorCommand{Command: &proto.CursorCommand_Close{Close: true}},
		},
	})
	return stream.CloseSend()
}

func sendCursorCommand(t *testing.T, stream proto.IndexedDB_OpenCursorClient, cmd *proto.CursorCommand) *proto.CursorResponse {
	t.Helper()

	if err := stream.Send(&proto.CursorClientMessage{
		Msg: &proto.CursorClientMessage_Command{Command: cmd},
	}); err != nil {
		t.Fatalf("Send(command): %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(command): %v", err)
	}
	return resp
}

func sendCursorCommandExpectError(t *testing.T, stream proto.IndexedDB_OpenCursorClient, cmd *proto.CursorCommand) error {
	t.Helper()

	if err := stream.Send(&proto.CursorClientMessage{
		Msg: &proto.CursorClientMessage_Command{Command: cmd},
	}); err != nil {
		t.Fatalf("Send(command): %v", err)
	}

	_, err := stream.Recv()
	if err == nil {
		t.Fatal("Recv(command) = nil, want error")
	}
	return err
}

func cursorEntryFromResponse(t *testing.T, resp *proto.CursorResponse) *proto.CursorEntry {
	t.Helper()

	entry, ok := resp.GetResult().(*proto.CursorResponse_Entry)
	if !ok {
		t.Fatalf("response = %T %+v, want entry", resp.GetResult(), resp)
	}
	return entry.Entry
}

func cursorDoneFromResponse(t *testing.T, resp *proto.CursorResponse) bool {
	t.Helper()

	done, ok := resp.GetResult().(*proto.CursorResponse_Done)
	if !ok {
		t.Fatalf("response = %T %+v, want done", resp.GetResult(), resp)
	}
	return done.Done
}

func TestOpenCursorAdvanceSkipsRequestedCount(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	resp := sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Advance{Advance: 2},
	})
	entry := cursorEntryFromResponse(t, resp)
	if got := entry.GetPrimaryKey(); got != "c" {
		t.Fatalf("Advance(2) primary key = %q, want %q", got, "c")
	}
}

func TestOpenCursorKeysOnlyOmitsRecord(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Direction: proto.CursorDirection_CURSOR_NEXT,
		KeysOnly:  true,
	})

	resp := sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	})
	entry := cursorEntryFromResponse(t, resp)
	if got := entry.GetPrimaryKey(); got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}
	if entry.GetRecord() != nil {
		t.Fatalf("keys-only cursor returned record: %+v", entry.GetRecord())
	}
}

func TestOpenCursorIndexRangeUsesIndexKeys(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: proto.CursorDirection_CURSOR_NEXT,
		Range: &proto.KeyRange{
			Lower: mustTypedValue(t, "active"),
			Upper: mustTypedValue(t, "active"),
		},
	})

	var keys []string
	for {
		resp := sendCursorCommand(t, stream, &proto.CursorCommand{
			Command: &proto.CursorCommand_Next{Next: true},
		})
		if _, ok := resp.GetResult().(*proto.CursorResponse_Done); ok {
			if !cursorDoneFromResponse(t, resp) {
				t.Fatal("expected exhausted cursor response")
			}
			break
		}

		entry := cursorEntryFromResponse(t, resp)
		keys = append(keys, entry.GetPrimaryKey())

		indexKey, err := gestalt.KeyValuesToAny(entry.GetKey())
		if err != nil {
			t.Fatalf("KeyValuesToAny: %v", err)
		}
		if len(indexKey) != 1 || indexKey[0] != "active" {
			t.Fatalf("index key = %#v, want [\"active\"]", indexKey)
		}
	}

	if len(keys) != 3 {
		t.Fatalf("active cursor count = %d, want 3", len(keys))
	}
}

func TestOpenCursorOrdersNumericPrimaryKeysByNativeType(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorNumbers(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "numbers",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	var gotKeys []int64
	var gotPrimaryKeys []string
	for {
		resp := sendCursorCommand(t, stream, &proto.CursorCommand{
			Command: &proto.CursorCommand_Next{Next: true},
		})
		if _, ok := resp.GetResult().(*proto.CursorResponse_Done); ok {
			if !cursorDoneFromResponse(t, resp) {
				t.Fatal("expected exhausted cursor response")
			}
			break
		}

		entry := cursorEntryFromResponse(t, resp)
		gotPrimaryKeys = append(gotPrimaryKeys, entry.GetPrimaryKey())
		key, err := gestalt.KeyValuesToAny(entry.GetKey())
		if err != nil {
			t.Fatalf("KeyValuesToAny: %v", err)
		}
		if len(key) != 1 {
			t.Fatalf("numeric cursor key len = %d, want 1", len(key))
		}
		intKey, ok := key[0].(int64)
		if !ok {
			t.Fatalf("numeric cursor key type = %T, want int64", key[0])
		}
		gotKeys = append(gotKeys, intKey)
	}

	if want := []int64{1, 2, 10}; len(gotKeys) != len(want) || gotKeys[0] != want[0] || gotKeys[1] != want[1] || gotKeys[2] != want[2] {
		t.Fatalf("numeric cursor keys = %#v, want %#v", gotKeys, want)
	}
	if want := []string{"1", "2", "10"}; len(gotPrimaryKeys) != len(want) || gotPrimaryKeys[0] != want[0] || gotPrimaryKeys[1] != want[1] || gotPrimaryKeys[2] != want[2] {
		t.Fatalf("numeric cursor primary keys = %#v, want %#v", gotPrimaryKeys, want)
	}
}

func TestOpenCursorRangeUsesNativeNumericPrimaryKeys(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorNumbers(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "numbers",
		Direction: proto.CursorDirection_CURSOR_NEXT,
		Range: &proto.KeyRange{
			Lower: mustTypedValue(t, int64(2)),
			Upper: mustTypedValue(t, int64(10)),
		},
	})

	var got []string
	for {
		resp := sendCursorCommand(t, stream, &proto.CursorCommand{
			Command: &proto.CursorCommand_Next{Next: true},
		})
		if _, ok := resp.GetResult().(*proto.CursorResponse_Done); ok {
			if !cursorDoneFromResponse(t, resp) {
				t.Fatal("expected exhausted cursor response")
			}
			break
		}

		entry := cursorEntryFromResponse(t, resp)
		got = append(got, entry.GetPrimaryKey())
	}

	if want := []string{"2", "10"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("numeric range cursor primary keys = %#v, want %#v", got, want)
	}
}

func TestOpenCursorDeleteAcknowledgesAndRemovesRecord(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	entry := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	if entry.GetPrimaryKey() != "a" {
		t.Fatalf("first primary key = %q, want %q", entry.GetPrimaryKey(), "a")
	}

	deleteResp := sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Delete{Delete: true},
	})
	if cursorDoneFromResponse(t, deleteResp) {
		t.Fatal("delete ack unexpectedly marked cursor exhausted")
	}

	if _, err := h.store.Get(context.Background(), &proto.ObjectStoreRequest{
		Store: "items",
		Id:    "a",
	}); err == nil {
		t.Fatal("deleted record still readable")
	}

	next := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	if got := next.GetPrimaryKey(); got != "b" {
		t.Fatalf("primary key after delete = %q, want %q", got, "b")
	}
}

func TestOpenCursorDeleteUsesNativePrimaryKeyValue(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorBytes(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "blobs",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	_ = cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))

	deleteResp := sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Delete{Delete: true},
	})
	if cursorDoneFromResponse(t, deleteResp) {
		t.Fatal("delete ack unexpectedly marked cursor exhausted")
	}

	countResp, err := h.store.Count(context.Background(), &proto.ObjectStoreRangeRequest{
		Store: "blobs",
	})
	if err != nil {
		t.Fatalf("Count(blobs): %v", err)
	}
	if got := countResp.GetCount(); got != 0 {
		t.Fatalf("blob count after delete = %d, want 0", got)
	}
}

func TestOpenCursorUpdatePersistsRecordWithCurrentPrimaryKey(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	entry := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	if entry.GetPrimaryKey() != "a" {
		t.Fatalf("first primary key = %q, want %q", entry.GetPrimaryKey(), "a")
	}

	updateRecord, err := gestalt.RecordToProto(map[string]any{
		"name":   "Updated",
		"status": "inactive",
		"email":  "updated@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto: %v", err)
	}

	updateResp := sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Update{Update: updateRecord},
	})
	updated := cursorEntryFromResponse(t, updateResp)
	if got := updated.GetPrimaryKey(); got != "a" {
		t.Fatalf("updated primary key = %q, want %q", got, "a")
	}

	record, err := gestalt.RecordFromProto(updated.GetRecord())
	if err != nil {
		t.Fatalf("RecordFromProto: %v", err)
	}
	if got := record["name"]; got != "Updated" {
		t.Fatalf("updated record name = %#v, want %q", got, "Updated")
	}

	stored, err := h.store.Get(context.Background(), &proto.ObjectStoreRequest{
		Store: "items",
		Id:    "a",
	})
	if err != nil {
		t.Fatalf("Get(updated): %v", err)
	}
	storedRecord, err := gestalt.RecordFromProto(stored.GetRecord())
	if err != nil {
		t.Fatalf("RecordFromProto(stored): %v", err)
	}
	if got := storedRecord["email"]; got != "updated@test.com" {
		t.Fatalf("stored email = %#v, want %q", got, "updated@test.com")
	}
}

func TestOpenCursorIndexUpdatePreservesSnapshotKeyOrder(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	first := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	firstKey, err := gestalt.KeyValuesToAny(first.GetKey())
	if err != nil {
		t.Fatalf("KeyValuesToAny(first): %v", err)
	}
	if got := first.GetPrimaryKey(); got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}
	if len(firstKey) != 1 || firstKey[0] != "active" {
		t.Fatalf("first key = %#v, want [\"active\"]", firstKey)
	}

	updateRecord, err := gestalt.RecordToProto(map[string]any{
		"name":   "Alice Updated",
		"status": "inactive",
		"email":  "alice-updated@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto(update): %v", err)
	}

	updated := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Update{Update: updateRecord},
	}))
	updatedKey, err := gestalt.KeyValuesToAny(updated.GetKey())
	if err != nil {
		t.Fatalf("KeyValuesToAny(updated): %v", err)
	}
	if got := updated.GetPrimaryKey(); got != "a" {
		t.Fatalf("updated primary key = %q, want %q", got, "a")
	}
	if len(updatedKey) != 1 || updatedKey[0] != "active" {
		t.Fatalf("updated key = %#v, want snapshot key [\"active\"]", updatedKey)
	}
	updatedRecord, err := gestalt.RecordFromProto(updated.GetRecord())
	if err != nil {
		t.Fatalf("RecordFromProto(updated): %v", err)
	}
	if got := updatedRecord["status"]; got != "inactive" {
		t.Fatalf("updated record status = %#v, want %q", got, "inactive")
	}

	second := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	secondKey, err := gestalt.KeyValuesToAny(second.GetKey())
	if err != nil {
		t.Fatalf("KeyValuesToAny(second): %v", err)
	}
	if got := second.GetPrimaryKey(); got != "b" {
		t.Fatalf("second primary key = %q, want %q", got, "b")
	}
	if len(secondKey) != 1 || secondKey[0] != "active" {
		t.Fatalf("second key = %#v, want [\"active\"]", secondKey)
	}
}

func TestOpenCursorIndexUpdateAllowsClearingIndexedField(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	stream := openCursorStream(t, h.client, &proto.OpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: proto.CursorDirection_CURSOR_NEXT,
	})

	first := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	if got := first.GetPrimaryKey(); got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}

	updateRecord, err := gestalt.RecordToProto(map[string]any{
		"name":  "Alice Missing Status",
		"email": "alice-missing-status@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto(update): %v", err)
	}

	updated := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Update{Update: updateRecord},
	}))
	updatedKey, err := gestalt.KeyValuesToAny(updated.GetKey())
	if err != nil {
		t.Fatalf("KeyValuesToAny(updated): %v", err)
	}
	if len(updatedKey) != 1 || updatedKey[0] != "active" {
		t.Fatalf("updated key = %#v, want snapshot key [\"active\"]", updatedKey)
	}

	stored, err := h.store.Get(context.Background(), &proto.ObjectStoreRequest{
		Store: "items",
		Id:    "a",
	})
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	storedRecord, err := gestalt.RecordFromProto(stored.GetRecord())
	if err != nil {
		t.Fatalf("RecordFromProto(stored): %v", err)
	}
	if got, ok := storedRecord["status"]; ok && got != nil {
		t.Fatalf("stored status = %#v, want nil", got)
	}
	if got := storedRecord["email"]; got != "alice-missing-status@test.com" {
		t.Fatalf("stored email = %#v, want %q", got, "alice-missing-status@test.com")
	}

	active, err := h.store.IndexGetAll(context.Background(), &proto.IndexQueryRequest{
		Store:  "items",
		Index:  "by_status",
		Values: []*proto.TypedValue{mustTypedValue(t, "active")},
	})
	if err != nil {
		t.Fatalf("IndexGetAll(active): %v", err)
	}
	if got := len(active.GetRecords()); got != 2 {
		t.Fatalf("active record count = %d, want 2", got)
	}

	next := cursorEntryFromResponse(t, sendCursorCommand(t, stream, &proto.CursorCommand{
		Command: &proto.CursorCommand_Next{Next: true},
	}))
	if got := next.GetPrimaryKey(); got != "b" {
		t.Fatalf("next primary key = %q, want %q", got, "b")
	}
}

func TestRelationalCursorCompoundIndexRangeUsesDecodedArrayKey(t *testing.T) {
	cursor := &relationalCursor{
		Snapshot: cursorutil.Snapshot{IndexCursor: true},
		index:    &proto.IndexSchema{KeyPath: []string{"status", "rank"}},
	}

	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active", int64(1)}, Record: makeCursorItem("a", "Alice", "active", "alice@test.com")},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"active", int64(2)}, Record: makeCursorItem("b", "Bob", "active", "bob@test.com")},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"inactive", int64(1)}, Record: makeCursorItem("c", "Carol", "inactive", "carol@test.com")},
	}

	filtered, err := cursor.ApplyRange(entries, &proto.KeyRange{
		Lower: mustTypedValue(t, []any{"active", int64(2)}),
		Upper: mustTypedValue(t, []any{"active", int64(2)}),
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("filtered count = %d, want 1", len(filtered))
	}
	if got := filtered[0].PrimaryKey; got != "b" {
		t.Fatalf("filtered primary key = %q, want %q", got, "b")
	}
}
