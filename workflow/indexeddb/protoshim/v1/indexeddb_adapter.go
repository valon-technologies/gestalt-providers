package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	internal "github.com/valon-technologies/gestalt/sdk/go/internal/gen/v1"
	"github.com/valon-technologies/gestalt/sdk/go/internal/indexeddbcodec"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type indexedDBProvider interface {
	CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error
	DeleteObjectStore(ctx context.Context, name string) error
	Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error)
	GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error)
	Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error
	Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error
	Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error
	Clear(ctx context.Context, store string) error
	GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error)
	GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error)
	Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error)
	DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error)
	IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error)
	IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error)
	IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error)
	IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error)
	IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error)
	IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error)
	OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error)
	BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error)
}

func NewIndexedDBServer(provider indexedDBProvider) IndexedDBServer {
	return indexedDBProviderServer{provider: provider}
}

type indexedDBProviderServer struct {
	internal.UnimplementedIndexedDBServer
	provider indexedDBProvider
}

func (s indexedDBProviderServer) CreateObjectStore(ctx context.Context, req *internal.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb create object store", s.provider.CreateObjectStore(ctx, req.GetName(), objectStoreSchemaFromProto(req.GetSchema())))
}

func (s indexedDBProviderServer) DeleteObjectStore(ctx context.Context, req *internal.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb delete object store", s.provider.DeleteObjectStore(ctx, req.GetName()))
}

func (s indexedDBProviderServer) Get(ctx context.Context, req *internal.ObjectStoreRequest) (*internal.RecordResponse, error) {
	record, err := s.provider.Get(ctx, objectStoreRequestFromProto(req))
	return recordResponseToProto("indexeddb get", record, err)
}

func (s indexedDBProviderServer) GetKey(ctx context.Context, req *internal.ObjectStoreRequest) (*internal.KeyResponse, error) {
	key, err := s.provider.GetKey(ctx, objectStoreRequestFromProto(req))
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb get key", err)
	}
	return &internal.KeyResponse{Key: key}, nil
}

func (s indexedDBProviderServer) Add(ctx context.Context, req *internal.RecordRequest) (*emptypb.Empty, error) {
	record, err := indexeddbcodec.RecordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal record: %v", err)
	}
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb add", s.provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}))
}

func (s indexedDBProviderServer) Put(ctx context.Context, req *internal.RecordRequest) (*emptypb.Empty, error) {
	record, err := indexeddbcodec.RecordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal record: %v", err)
	}
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb put", s.provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}))
}

func (s indexedDBProviderServer) Delete(ctx context.Context, req *internal.ObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb delete", s.provider.Delete(ctx, objectStoreRequestFromProto(req)))
}

func (s indexedDBProviderServer) Clear(ctx context.Context, req *internal.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBProviderRPCError("indexeddb clear", s.provider.Clear(ctx, req.GetStore()))
}

func (s indexedDBProviderServer) GetAll(ctx context.Context, req *internal.ObjectStoreRangeRequest) (*internal.RecordsResponse, error) {
	records, err := s.provider.GetAll(ctx, objectStoreRangeRequestFromProto(req))
	return recordsResponseToProto("indexeddb get all", records, err)
}

func (s indexedDBProviderServer) GetAllKeys(ctx context.Context, req *internal.ObjectStoreRangeRequest) (*internal.KeysResponse, error) {
	keys, err := s.provider.GetAllKeys(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb get all keys", err)
	}
	return &internal.KeysResponse{Keys: keys}, nil
}

func (s indexedDBProviderServer) Count(ctx context.Context, req *internal.ObjectStoreRangeRequest) (*internal.CountResponse, error) {
	count, err := s.provider.Count(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb count", err)
	}
	return &internal.CountResponse{Count: count}, nil
}

func (s indexedDBProviderServer) DeleteRange(ctx context.Context, req *internal.ObjectStoreRangeRequest) (*internal.DeleteResponse, error) {
	deleted, err := s.provider.DeleteRange(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb delete range", err)
	}
	return &internal.DeleteResponse{Deleted: deleted}, nil
}

func (s indexedDBProviderServer) IndexGet(ctx context.Context, req *internal.IndexQueryRequest) (*internal.RecordResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	record, err := s.provider.IndexGet(ctx, query)
	return recordResponseToProto("indexeddb index get", record, err)
}

func (s indexedDBProviderServer) IndexGetKey(ctx context.Context, req *internal.IndexQueryRequest) (*internal.KeyResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	key, err := s.provider.IndexGetKey(ctx, query)
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb index get key", err)
	}
	return &internal.KeyResponse{Key: key}, nil
}

func (s indexedDBProviderServer) IndexGetAll(ctx context.Context, req *internal.IndexQueryRequest) (*internal.RecordsResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	records, err := s.provider.IndexGetAll(ctx, query)
	return recordsResponseToProto("indexeddb index get all", records, err)
}

func (s indexedDBProviderServer) IndexGetAllKeys(ctx context.Context, req *internal.IndexQueryRequest) (*internal.KeysResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	keys, err := s.provider.IndexGetAllKeys(ctx, query)
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb index get all keys", err)
	}
	return &internal.KeysResponse{Keys: keys}, nil
}

func (s indexedDBProviderServer) IndexCount(ctx context.Context, req *internal.IndexQueryRequest) (*internal.CountResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	count, err := s.provider.IndexCount(ctx, query)
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb index count", err)
	}
	return &internal.CountResponse{Count: count}, nil
}

func (s indexedDBProviderServer) IndexDelete(ctx context.Context, req *internal.IndexQueryRequest) (*internal.DeleteResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	deleted, err := s.provider.IndexDelete(ctx, query)
	if err != nil {
		return nil, indexedDBProviderRPCError("indexeddb index delete", err)
	}
	return &internal.DeleteResponse{Deleted: deleted}, nil
}

func (s indexedDBProviderServer) OpenCursor(stream grpc.BidiStreamingServer[internal.CursorClientMessage, internal.CursorResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	openReq := first.GetOpen()
	if openReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be OpenCursorRequest")
	}
	req, err := openCursorRequestFromProto(openReq)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	cursor, err := s.provider.OpenCursor(stream.Context(), req)
	if err != nil {
		return indexedDBProviderRPCError("indexeddb open cursor", err)
	}
	defer cursor.Close()
	if err := stream.Send(cursorDoneResponse(false)); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		cmd := msg.GetCommand()
		if cmd == nil {
			return status.Error(codes.InvalidArgument, "expected CursorCommand after open")
		}
		switch v := cmd.GetCommand().(type) {
		case *internal.CursorCommand_Next:
			entry, err := cursor.Next(stream.Context())
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *internal.CursorCommand_ContinueToKey:
			target, err := cursorTargetFromProto(v.ContinueToKey.GetKey(), req.Index != "")
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "unmarshal cursor target: %v", err)
			}
			entry, err := cursor.ContinueToKey(stream.Context(), target)
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *internal.CursorCommand_Advance:
			entry, err := cursor.Advance(stream.Context(), int(v.Advance))
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *internal.CursorCommand_Delete:
			if err := cursor.Delete(stream.Context()); err != nil {
				return indexedDBProviderRPCError("indexeddb cursor delete", err)
			}
			if err := stream.Send(cursorDoneResponse(false)); err != nil {
				return err
			}
		case *internal.CursorCommand_Update:
			record, err := indexeddbcodec.RecordFromProto(v.Update)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "unmarshal cursor update: %v", err)
			}
			entry, err := cursor.Update(stream.Context(), record)
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *internal.CursorCommand_Close:
			return nil
		default:
			return status.Error(codes.InvalidArgument, "unknown cursor command")
		}
	}
}

func (s indexedDBProviderServer) Transaction(stream grpc.BidiStreamingServer[internal.TransactionClientMessage, internal.TransactionServerMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	beginReq := first.GetBegin()
	if beginReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be BeginTransactionRequest")
	}
	if len(beginReq.GetStores()) == 0 {
		return status.Error(codes.InvalidArgument, "invalid transaction: at least one object store is required")
	}
	req := gestalt.IndexedDBBeginTransactionRequest{
		Stores:         beginReq.GetStores(),
		Mode:           transactionModeFromProto(beginReq.GetMode()),
		DurabilityHint: durabilityHintFromProto(beginReq.GetDurabilityHint()),
	}
	tx, err := s.provider.BeginTransaction(stream.Context(), req)
	if err != nil {
		return indexedDBProviderRPCError("indexeddb begin transaction", err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Abort(stream.Context())
		}
	}()
	if err := stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Begin{Begin: &internal.TransactionBeginResponse{}}}); err != nil {
		return err
	}
	for {
		msg, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				finished = true
				_ = tx.Abort(stream.Context())
				return nil
			}
			return recvErr
		}
		switch body := msg.GetMsg().(type) {
		case *internal.TransactionClientMessage_Operation:
			opErr := readonlyOperationError(req.Mode, body.Operation)
			resp := (*internal.TransactionOperationResponse)(nil)
			if opErr == nil {
				resp, opErr = executeIndexedDBOperation(stream.Context(), tx, body.Operation)
			}
			if opErr != nil {
				finished = true
				abortErr := tx.Abort(stream.Context())
				if err := stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Operation{Operation: transactionOperationError(body.Operation.GetRequestId(), opErr)}}); err != nil {
					return err
				}
				if err := stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Abort{Abort: &internal.TransactionAbortResponse{Error: rpcStatusFromError(abortErr)}}}); err != nil {
					return err
				}
				return drainIndexedDBTransaction(stream)
			}
			if err := stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Operation{Operation: resp}}); err != nil {
				return err
			}
		case *internal.TransactionClientMessage_Commit:
			finished = true
			commitErr := tx.Commit(stream.Context())
			return stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Commit{Commit: &internal.TransactionCommitResponse{Error: rpcStatusFromError(commitErr)}}})
		case *internal.TransactionClientMessage_Abort:
			finished = true
			abortErr := tx.Abort(stream.Context())
			return stream.Send(&internal.TransactionServerMessage{Msg: &internal.TransactionServerMessage_Abort{Abort: &internal.TransactionAbortResponse{Error: rpcStatusFromError(abortErr)}}})
		default:
			finished = true
			_ = tx.Abort(stream.Context())
			return status.Error(codes.InvalidArgument, "expected transaction operation, commit, or abort")
		}
	}
}

func objectStoreSchemaFromProto(schema *internal.ObjectStoreSchema) gestalt.ObjectStoreSchema {
	if schema == nil {
		return gestalt.ObjectStoreSchema{}
	}
	out := gestalt.ObjectStoreSchema{
		Indexes: make([]gestalt.IndexSchema, len(schema.GetIndexes())),
		Columns: make([]gestalt.ColumnDef, len(schema.GetColumns())),
	}
	for i, idx := range schema.GetIndexes() {
		out.Indexes[i] = gestalt.IndexSchema{Name: idx.GetName(), KeyPath: idx.GetKeyPath(), Unique: idx.GetUnique()}
	}
	for i, col := range schema.GetColumns() {
		out.Columns[i] = gestalt.ColumnDef{
			Name:       col.GetName(),
			Type:       gestalt.ColumnType(col.GetType()),
			PrimaryKey: col.GetPrimaryKey(),
			NotNull:    col.GetNotNull(),
			Unique:     col.GetUnique(),
		}
	}
	return out
}

func objectStoreRequestFromProto(req *internal.ObjectStoreRequest) gestalt.IndexedDBObjectStoreRequest {
	return gestalt.IndexedDBObjectStoreRequest{Store: req.GetStore(), ID: req.GetId()}
}

func objectStoreRangeRequestFromProto(req *internal.ObjectStoreRangeRequest) gestalt.IndexedDBObjectStoreRangeRequest {
	return gestalt.IndexedDBObjectStoreRangeRequest{Store: req.GetStore(), Range: keyRangeFromProto(req.GetRange())}
}

func indexQueryRequestFromProto(req *internal.IndexQueryRequest) (gestalt.IndexedDBIndexQueryRequest, error) {
	values, err := indexeddbcodec.AnyFromTypedValues(req.GetValues())
	if err != nil {
		return gestalt.IndexedDBIndexQueryRequest{}, fmt.Errorf("unmarshal index values: %w", err)
	}
	return gestalt.IndexedDBIndexQueryRequest{Store: req.GetStore(), Index: req.GetIndex(), Values: values, Range: keyRangeFromProto(req.GetRange())}, nil
}

func openCursorRequestFromProto(req *internal.OpenCursorRequest) (gestalt.IndexedDBOpenCursorRequest, error) {
	values, err := indexeddbcodec.AnyFromTypedValues(req.GetValues())
	if err != nil {
		return gestalt.IndexedDBOpenCursorRequest{}, fmt.Errorf("unmarshal cursor values: %w", err)
	}
	return gestalt.IndexedDBOpenCursorRequest{
		Store:     req.GetStore(),
		Range:     keyRangeFromProto(req.GetRange()),
		Direction: cursorDirectionFromProto(req.GetDirection()),
		KeysOnly:  req.GetKeysOnly(),
		Index:     req.GetIndex(),
		Values:    values,
	}, nil
}

func keyRangeFromProto(r *internal.KeyRange) *gestalt.KeyRange {
	if r == nil {
		return nil
	}
	out := &gestalt.KeyRange{LowerOpen: r.GetLowerOpen(), UpperOpen: r.GetUpperOpen()}
	if r.GetLower() != nil {
		out.Lower, _ = indexeddbcodec.AnyFromTypedValue(r.GetLower())
	}
	if r.GetUpper() != nil {
		out.Upper, _ = indexeddbcodec.AnyFromTypedValue(r.GetUpper())
	}
	return out
}

func recordResponseToProto(operation string, record gestalt.Record, err error) (*internal.RecordResponse, error) {
	if err != nil {
		return nil, indexedDBProviderRPCError(operation, err)
	}
	pbRecord, err := indexeddbcodec.RecordToProto(record)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal record: %v", err)
	}
	return &internal.RecordResponse{Record: pbRecord}, nil
}

func recordsResponseToProto(operation string, records []gestalt.Record, err error) (*internal.RecordsResponse, error) {
	if err != nil {
		return nil, indexedDBProviderRPCError(operation, err)
	}
	pbRecords, err := indexeddbcodec.RecordsToProto(records)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal records: %v", err)
	}
	return &internal.RecordsResponse{Records: pbRecords}, nil
}

func sendCursorResult(stream grpc.BidiStreamingServer[internal.CursorClientMessage, internal.CursorResponse], entry *gestalt.IndexedDBCursorEntry, indexCursor bool, err error) error {
	if err != nil {
		return indexedDBProviderRPCError("indexeddb cursor", err)
	}
	if entry == nil {
		return stream.Send(cursorDoneResponse(true))
	}
	pbEntry, err := cursorEntryToProto(entry, indexCursor)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal cursor entry: %v", err)
	}
	return stream.Send(&internal.CursorResponse{Result: &internal.CursorResponse_Entry{Entry: pbEntry}})
}

func cursorEntryToProto(entry *gestalt.IndexedDBCursorEntry, indexCursor bool) (*internal.CursorEntry, error) {
	key, err := indexeddbcodec.CursorKeyToProto(entry.Key, indexCursor)
	if err != nil {
		return nil, err
	}
	out := &internal.CursorEntry{Key: key, PrimaryKey: entry.PrimaryKey}
	if entry.Record != nil {
		record, err := indexeddbcodec.RecordToProto(entry.Record)
		if err != nil {
			return nil, err
		}
		out.Record = record
	}
	return out, nil
}

func cursorDoneResponse(done bool) *internal.CursorResponse {
	return &internal.CursorResponse{Result: &internal.CursorResponse_Done{Done: done}}
}

func cursorTargetFromProto(kvs []*internal.KeyValue, indexCursor bool) (any, error) {
	if len(kvs) == 0 {
		return nil, fmt.Errorf("continue key is required")
	}
	parts, err := indexeddbcodec.KeyValuesToAny(kvs)
	if err != nil {
		return nil, err
	}
	if indexCursor {
		return parts, nil
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return parts, nil
}

func cursorDirectionFromProto(dir internal.CursorDirection) gestalt.CursorDirection {
	switch dir {
	case internal.CursorDirection_CURSOR_NEXT_UNIQUE:
		return gestalt.CursorNextUnique
	case internal.CursorDirection_CURSOR_PREV:
		return gestalt.CursorPrev
	case internal.CursorDirection_CURSOR_PREV_UNIQUE:
		return gestalt.CursorPrevUnique
	default:
		return gestalt.CursorNext
	}
}

func transactionModeFromProto(mode internal.TransactionMode) gestalt.TransactionMode {
	if mode == internal.TransactionMode_TRANSACTION_READWRITE {
		return gestalt.TransactionReadwrite
	}
	return gestalt.TransactionReadonly
}

func durabilityHintFromProto(hint internal.TransactionDurabilityHint) gestalt.TransactionDurabilityHint {
	switch hint {
	case internal.TransactionDurabilityHint_TRANSACTION_DURABILITY_STRICT:
		return gestalt.TransactionDurabilityStrict
	case internal.TransactionDurabilityHint_TRANSACTION_DURABILITY_RELAXED:
		return gestalt.TransactionDurabilityRelaxed
	default:
		return gestalt.TransactionDurabilityDefault
	}
}

func executeIndexedDBOperation(ctx context.Context, tx gestalt.IndexedDBTransaction, op *internal.TransactionOperation) (*internal.TransactionOperationResponse, error) {
	if op == nil {
		return nil, status.Error(codes.InvalidArgument, "transaction operation is required")
	}
	resp := &internal.TransactionOperationResponse{RequestId: op.GetRequestId()}
	switch body := op.GetOperation().(type) {
	case *internal.TransactionOperation_Get:
		record, err := tx.Get(ctx, objectStoreRequestFromProto(body.Get))
		if err != nil {
			return nil, err
		}
		pbRecord, err := indexeddbcodec.RecordToProto(record)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Record{Record: &internal.RecordResponse{Record: pbRecord}}
	case *internal.TransactionOperation_GetKey:
		key, err := tx.GetKey(ctx, objectStoreRequestFromProto(body.GetKey))
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Key{Key: &internal.KeyResponse{Key: key}}
	case *internal.TransactionOperation_Add:
		req, err := recordRequestFromProto(body.Add)
		if err != nil {
			return nil, err
		}
		if err := tx.Add(ctx, req); err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *internal.TransactionOperation_Put:
		req, err := recordRequestFromProto(body.Put)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(ctx, req); err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *internal.TransactionOperation_Delete:
		if err := tx.Delete(ctx, objectStoreRequestFromProto(body.Delete)); err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *internal.TransactionOperation_Clear:
		if err := tx.Clear(ctx, body.Clear.GetStore()); err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *internal.TransactionOperation_GetAll:
		records, err := tx.GetAll(ctx, objectStoreRangeRequestFromProto(body.GetAll))
		if err != nil {
			return nil, err
		}
		pbRecords, err := indexeddbcodec.RecordsToProto(records)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Records{Records: &internal.RecordsResponse{Records: pbRecords}}
	case *internal.TransactionOperation_GetAllKeys:
		keys, err := tx.GetAllKeys(ctx, objectStoreRangeRequestFromProto(body.GetAllKeys))
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Keys{Keys: &internal.KeysResponse{Keys: keys}}
	case *internal.TransactionOperation_Count:
		count, err := tx.Count(ctx, objectStoreRangeRequestFromProto(body.Count))
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Count{Count: &internal.CountResponse{Count: count}}
	case *internal.TransactionOperation_DeleteRange:
		deleted, err := tx.DeleteRange(ctx, objectStoreRangeRequestFromProto(body.DeleteRange))
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Delete{Delete: &internal.DeleteResponse{Deleted: deleted}}
	case *internal.TransactionOperation_IndexGet:
		query, err := indexQueryRequestFromProto(body.IndexGet)
		if err != nil {
			return nil, err
		}
		record, err := tx.IndexGet(ctx, query)
		if err != nil {
			return nil, err
		}
		pbRecord, err := indexeddbcodec.RecordToProto(record)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Record{Record: &internal.RecordResponse{Record: pbRecord}}
	case *internal.TransactionOperation_IndexGetKey:
		query, err := indexQueryRequestFromProto(body.IndexGetKey)
		if err != nil {
			return nil, err
		}
		key, err := tx.IndexGetKey(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Key{Key: &internal.KeyResponse{Key: key}}
	case *internal.TransactionOperation_IndexGetAll:
		query, err := indexQueryRequestFromProto(body.IndexGetAll)
		if err != nil {
			return nil, err
		}
		records, err := tx.IndexGetAll(ctx, query)
		if err != nil {
			return nil, err
		}
		pbRecords, err := indexeddbcodec.RecordsToProto(records)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Records{Records: &internal.RecordsResponse{Records: pbRecords}}
	case *internal.TransactionOperation_IndexGetAllKeys:
		query, err := indexQueryRequestFromProto(body.IndexGetAllKeys)
		if err != nil {
			return nil, err
		}
		keys, err := tx.IndexGetAllKeys(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Keys{Keys: &internal.KeysResponse{Keys: keys}}
	case *internal.TransactionOperation_IndexCount:
		query, err := indexQueryRequestFromProto(body.IndexCount)
		if err != nil {
			return nil, err
		}
		count, err := tx.IndexCount(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Count{Count: &internal.CountResponse{Count: count}}
	case *internal.TransactionOperation_IndexDelete:
		query, err := indexQueryRequestFromProto(body.IndexDelete)
		if err != nil {
			return nil, err
		}
		deleted, err := tx.IndexDelete(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &internal.TransactionOperationResponse_Delete{Delete: &internal.DeleteResponse{Deleted: deleted}}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown transaction operation")
	}
	return resp, nil
}

func recordRequestFromProto(req *internal.RecordRequest) (gestalt.IndexedDBRecordRequest, error) {
	record, err := indexeddbcodec.RecordFromProto(req.GetRecord())
	if err != nil {
		return gestalt.IndexedDBRecordRequest{}, fmt.Errorf("unmarshal record: %w", err)
	}
	return gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}, nil
}

func transactionOperationError(requestID uint64, err error) *internal.TransactionOperationResponse {
	return &internal.TransactionOperationResponse{RequestId: requestID, Error: rpcStatusFromError(err)}
}

func rpcStatusFromError(err error) *rpcstatus.Status {
	if err == nil {
		return nil
	}
	rpcErr := indexedDBProviderRPCError("indexeddb", err)
	st, ok := status.FromError(rpcErr)
	if !ok {
		return &rpcstatus.Status{Code: int32(codes.Internal), Message: rpcErr.Error()}
	}
	return &rpcstatus.Status{Code: int32(st.Code()), Message: st.Message()}
}

func readonlyOperationError(mode gestalt.TransactionMode, op *internal.TransactionOperation) error {
	if mode == gestalt.TransactionReadwrite || op == nil {
		return nil
	}
	if isWriteTransactionOperation(op) {
		return gestalt.FailedPrecondition("transaction is readonly")
	}
	return nil
}

func isWriteTransactionOperation(op *internal.TransactionOperation) bool {
	switch op.GetOperation().(type) {
	case *internal.TransactionOperation_Add,
		*internal.TransactionOperation_Put,
		*internal.TransactionOperation_Delete,
		*internal.TransactionOperation_Clear,
		*internal.TransactionOperation_DeleteRange,
		*internal.TransactionOperation_IndexDelete:
		return true
	default:
		return false
	}
}

func drainIndexedDBTransaction(stream grpc.BidiStreamingServer[internal.TransactionClientMessage, internal.TransactionServerMessage]) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				return nil
			}
			return err
		}
	}
}

func indexedDBProviderRPCError(operation string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, gestalt.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, gestalt.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, gestalt.ErrInvalidTransaction):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, gestalt.ErrReadOnly), errors.Is(err, gestalt.ErrTransactionDone):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	return status.Errorf(codes.Unknown, "%s: %v", operation, err)
}
