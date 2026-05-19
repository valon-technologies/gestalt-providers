package indexeddb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type indexedDBTestServer struct {
	proto.UnimplementedIndexedDBServer
	provider gestalt.IndexedDBProvider
}

func newIndexedDBTestServer(provider gestalt.IndexedDBProvider) proto.IndexedDBServer {
	return indexedDBTestServer{provider: provider}
}

func (s indexedDBTestServer) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb create object store", s.provider.CreateObjectStore(ctx, req.GetName(), objectStoreSchemaFromProto(req.GetSchema())))
}

func (s indexedDBTestServer) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb delete object store", s.provider.DeleteObjectStore(ctx, req.GetName()))
}

func (s indexedDBTestServer) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	record, err := s.provider.Get(ctx, objectStoreRequestFromProto(req))
	return recordResponseToProto("indexeddb get", record, err)
}

func (s indexedDBTestServer) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	key, err := s.provider.GetKey(ctx, objectStoreRequestFromProto(req))
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb get key", err)
	}
	return &proto.KeyResponse{Key: key}, nil
}

func (s indexedDBTestServer) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	record, err := recordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal record: %v", err)
	}
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb add", s.provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}))
}

func (s indexedDBTestServer) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	record, err := recordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal record: %v", err)
	}
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb put", s.provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}))
}

func (s indexedDBTestServer) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb delete", s.provider.Delete(ctx, objectStoreRequestFromProto(req)))
}

func (s indexedDBTestServer) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, indexedDBTestRPCError("indexeddb clear", s.provider.Clear(ctx, req.GetStore()))
}

func (s indexedDBTestServer) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	records, err := s.provider.GetAll(ctx, objectStoreRangeRequestFromProto(req))
	return recordsResponseToProto("indexeddb get all", records, err)
}

func (s indexedDBTestServer) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	keys, err := s.provider.GetAllKeys(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb get all keys", err)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (s indexedDBTestServer) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	count, err := s.provider.Count(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb count", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (s indexedDBTestServer) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	deleted, err := s.provider.DeleteRange(ctx, objectStoreRangeRequestFromProto(req))
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb delete range", err)
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

func (s indexedDBTestServer) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	record, err := s.provider.IndexGet(ctx, query)
	return recordResponseToProto("indexeddb index get", record, err)
}

func (s indexedDBTestServer) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	key, err := s.provider.IndexGetKey(ctx, query)
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb index get key", err)
	}
	return &proto.KeyResponse{Key: key}, nil
}

func (s indexedDBTestServer) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	records, err := s.provider.IndexGetAll(ctx, query)
	return recordsResponseToProto("indexeddb index get all", records, err)
}

func (s indexedDBTestServer) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	keys, err := s.provider.IndexGetAllKeys(ctx, query)
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb index get all keys", err)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (s indexedDBTestServer) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	count, err := s.provider.IndexCount(ctx, query)
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb index count", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (s indexedDBTestServer) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	query, err := indexQueryRequestFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	deleted, err := s.provider.IndexDelete(ctx, query)
	if err != nil {
		return nil, indexedDBTestRPCError("indexeddb index delete", err)
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

func (s indexedDBTestServer) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
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
		return indexedDBTestRPCError("indexeddb open cursor", err)
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
		case *proto.CursorCommand_Next:
			entry, err := cursor.Next(stream.Context())
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *proto.CursorCommand_ContinueToKey:
			target, err := cursorTargetFromProto(v.ContinueToKey.GetKey(), req.Index != "")
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "unmarshal cursor target: %v", err)
			}
			entry, err := cursor.ContinueToKey(stream.Context(), target)
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *proto.CursorCommand_Advance:
			entry, err := cursor.Advance(stream.Context(), int(v.Advance))
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *proto.CursorCommand_Delete:
			if err := cursor.Delete(stream.Context()); err != nil {
				return indexedDBTestRPCError("indexeddb cursor delete", err)
			}
			if err := stream.Send(cursorDoneResponse(false)); err != nil {
				return err
			}
		case *proto.CursorCommand_Update:
			record, err := recordFromProto(v.Update)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "unmarshal cursor update: %v", err)
			}
			entry, err := cursor.Update(stream.Context(), record)
			if err := sendCursorResult(stream, entry, req.Index != "", err); err != nil {
				return err
			}
		case *proto.CursorCommand_Close:
			return nil
		default:
			return status.Error(codes.InvalidArgument, "unknown cursor command")
		}
	}
}

func (s indexedDBTestServer) Transaction(stream proto.IndexedDB_TransactionServer) error {
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
		return indexedDBTestRPCError("indexeddb begin transaction", err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Abort(stream.Context())
		}
	}()
	if err := stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Begin{Begin: &proto.TransactionBeginResponse{}}}); err != nil {
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
		case *proto.TransactionClientMessage_Operation:
			opErr := readonlyOperationError(req.Mode, body.Operation)
			resp := (*proto.TransactionOperationResponse)(nil)
			if opErr == nil {
				resp, opErr = executeIndexedDBOperation(stream.Context(), tx, body.Operation)
			}
			if opErr != nil {
				finished = true
				abortErr := tx.Abort(stream.Context())
				if err := stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Operation{Operation: transactionOperationError(body.Operation.GetRequestId(), opErr)}}); err != nil {
					return err
				}
				if err := stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Abort{Abort: &proto.TransactionAbortResponse{Error: rpcStatusFromError(abortErr)}}}); err != nil {
					return err
				}
				return drainIndexedDBTransaction(stream)
			}
			if err := stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Operation{Operation: resp}}); err != nil {
				return err
			}
		case *proto.TransactionClientMessage_Commit:
			finished = true
			commitErr := tx.Commit(stream.Context())
			return stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Commit{Commit: &proto.TransactionCommitResponse{Error: rpcStatusFromError(commitErr)}}})
		case *proto.TransactionClientMessage_Abort:
			finished = true
			abortErr := tx.Abort(stream.Context())
			return stream.Send(&proto.TransactionServerMessage{Msg: &proto.TransactionServerMessage_Abort{Abort: &proto.TransactionAbortResponse{Error: rpcStatusFromError(abortErr)}}})
		default:
			finished = true
			_ = tx.Abort(stream.Context())
			return status.Error(codes.InvalidArgument, "expected transaction operation, commit, or abort")
		}
	}
}

func objectStoreSchemaFromProto(schema *proto.ObjectStoreSchema) gestalt.ObjectStoreSchema {
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

func objectStoreRequestFromProto(req *proto.ObjectStoreRequest) gestalt.IndexedDBObjectStoreRequest {
	return gestalt.IndexedDBObjectStoreRequest{Store: req.GetStore(), ID: req.GetId()}
}

func objectStoreRangeRequestFromProto(req *proto.ObjectStoreRangeRequest) gestalt.IndexedDBObjectStoreRangeRequest {
	return gestalt.IndexedDBObjectStoreRangeRequest{Store: req.GetStore(), Range: keyRangeFromProto(req.GetRange())}
}

func indexQueryRequestFromProto(req *proto.IndexQueryRequest) (gestalt.IndexedDBIndexQueryRequest, error) {
	values, err := proto.AnyFromTypedValues(req.GetValues())
	if err != nil {
		return gestalt.IndexedDBIndexQueryRequest{}, fmt.Errorf("unmarshal index values: %w", err)
	}
	return gestalt.IndexedDBIndexQueryRequest{Store: req.GetStore(), Index: req.GetIndex(), Values: values, Range: keyRangeFromProto(req.GetRange())}, nil
}

func openCursorRequestFromProto(req *proto.OpenCursorRequest) (gestalt.IndexedDBOpenCursorRequest, error) {
	values, err := proto.AnyFromTypedValues(req.GetValues())
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

func keyRangeFromProto(r *proto.KeyRange) *gestalt.KeyRange {
	if r == nil {
		return nil
	}
	out := &gestalt.KeyRange{LowerOpen: r.GetLowerOpen(), UpperOpen: r.GetUpperOpen()}
	if r.GetLower() != nil {
		out.Lower, _ = proto.AnyFromTypedValue(r.GetLower())
	}
	if r.GetUpper() != nil {
		out.Upper, _ = proto.AnyFromTypedValue(r.GetUpper())
	}
	return out
}

func recordResponseToProto(operation string, record gestalt.Record, err error) (*proto.RecordResponse, error) {
	if err != nil {
		return nil, indexedDBTestRPCError(operation, err)
	}
	pbRecord, err := recordToProto(record)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal record: %v", err)
	}
	return &proto.RecordResponse{Record: pbRecord}, nil
}

func recordsResponseToProto(operation string, records []gestalt.Record, err error) (*proto.RecordsResponse, error) {
	if err != nil {
		return nil, indexedDBTestRPCError(operation, err)
	}
	pbRecords, err := recordsToProto(records)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal records: %v", err)
	}
	return &proto.RecordsResponse{Records: pbRecords}, nil
}

func sendCursorResult(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse], entry *gestalt.IndexedDBCursorEntry, indexCursor bool, err error) error {
	if err != nil {
		return indexedDBTestRPCError("indexeddb cursor", err)
	}
	if entry == nil {
		return stream.Send(cursorDoneResponse(true))
	}
	pbEntry, err := cursorEntryToProto(entry, indexCursor)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal cursor entry: %v", err)
	}
	return stream.Send(&proto.CursorResponse{Result: &proto.CursorResponse_Entry{Entry: pbEntry}})
}

func cursorEntryToProto(entry *gestalt.IndexedDBCursorEntry, indexCursor bool) (*proto.CursorEntry, error) {
	key, err := proto.CursorKeyToProto(entry.Key, indexCursor)
	if err != nil {
		return nil, err
	}
	out := &proto.CursorEntry{Key: key, PrimaryKey: entry.PrimaryKey}
	if entry.Record != nil {
		record, err := recordToProto(entry.Record)
		if err != nil {
			return nil, err
		}
		out.Record = record
	}
	return out, nil
}

func cursorDoneResponse(done bool) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Done{Done: done}}
}

func cursorTargetFromProto(kvs []*proto.KeyValue, indexCursor bool) (any, error) {
	if len(kvs) == 0 {
		return nil, fmt.Errorf("continue key is required")
	}
	parts, err := proto.KeyValuesToAny(kvs)
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

func cursorDirectionFromProto(dir proto.CursorDirection) gestalt.CursorDirection {
	switch dir {
	case proto.CursorDirection_CURSOR_NEXT_UNIQUE:
		return gestalt.CursorNextUnique
	case proto.CursorDirection_CURSOR_PREV:
		return gestalt.CursorPrev
	case proto.CursorDirection_CURSOR_PREV_UNIQUE:
		return gestalt.CursorPrevUnique
	default:
		return gestalt.CursorNext
	}
}

func transactionModeFromProto(mode proto.TransactionMode) gestalt.TransactionMode {
	if mode == proto.TransactionMode_TRANSACTION_READWRITE {
		return gestalt.TransactionReadwrite
	}
	return gestalt.TransactionReadonly
}

func durabilityHintFromProto(hint proto.TransactionDurabilityHint) gestalt.TransactionDurabilityHint {
	switch hint {
	case proto.TransactionDurabilityHint_TRANSACTION_DURABILITY_STRICT:
		return gestalt.TransactionDurabilityStrict
	case proto.TransactionDurabilityHint_TRANSACTION_DURABILITY_RELAXED:
		return gestalt.TransactionDurabilityRelaxed
	default:
		return gestalt.TransactionDurabilityDefault
	}
}

func executeIndexedDBOperation(ctx context.Context, tx gestalt.IndexedDBTransaction, op *proto.TransactionOperation) (*proto.TransactionOperationResponse, error) {
	if op == nil {
		return nil, status.Error(codes.InvalidArgument, "transaction operation is required")
	}
	resp := &proto.TransactionOperationResponse{RequestId: op.GetRequestId()}
	switch body := op.GetOperation().(type) {
	case *proto.TransactionOperation_Get:
		record, err := tx.Get(ctx, objectStoreRequestFromProto(body.Get))
		if err != nil {
			return nil, err
		}
		pbRecord, err := recordToProto(record)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Record{Record: &proto.RecordResponse{Record: pbRecord}}
	case *proto.TransactionOperation_GetKey:
		key, err := tx.GetKey(ctx, objectStoreRequestFromProto(body.GetKey))
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Key{Key: &proto.KeyResponse{Key: key}}
	case *proto.TransactionOperation_Add:
		req, err := recordRequestFromProto(body.Add)
		if err != nil {
			return nil, err
		}
		if err := tx.Add(ctx, req); err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *proto.TransactionOperation_Put:
		req, err := recordRequestFromProto(body.Put)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(ctx, req); err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *proto.TransactionOperation_Delete:
		if err := tx.Delete(ctx, objectStoreRequestFromProto(body.Delete)); err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *proto.TransactionOperation_Clear:
		if err := tx.Clear(ctx, body.Clear.GetStore()); err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Empty{Empty: &emptypb.Empty{}}
	case *proto.TransactionOperation_GetAll:
		records, err := tx.GetAll(ctx, objectStoreRangeRequestFromProto(body.GetAll))
		if err != nil {
			return nil, err
		}
		pbRecords, err := recordsToProto(records)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Records{Records: &proto.RecordsResponse{Records: pbRecords}}
	case *proto.TransactionOperation_GetAllKeys:
		keys, err := tx.GetAllKeys(ctx, objectStoreRangeRequestFromProto(body.GetAllKeys))
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Keys{Keys: &proto.KeysResponse{Keys: keys}}
	case *proto.TransactionOperation_Count:
		count, err := tx.Count(ctx, objectStoreRangeRequestFromProto(body.Count))
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Count{Count: &proto.CountResponse{Count: count}}
	case *proto.TransactionOperation_DeleteRange:
		deleted, err := tx.DeleteRange(ctx, objectStoreRangeRequestFromProto(body.DeleteRange))
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Delete{Delete: &proto.DeleteResponse{Deleted: deleted}}
	case *proto.TransactionOperation_IndexGet:
		query, err := indexQueryRequestFromProto(body.IndexGet)
		if err != nil {
			return nil, err
		}
		record, err := tx.IndexGet(ctx, query)
		if err != nil {
			return nil, err
		}
		pbRecord, err := recordToProto(record)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Record{Record: &proto.RecordResponse{Record: pbRecord}}
	case *proto.TransactionOperation_IndexGetKey:
		query, err := indexQueryRequestFromProto(body.IndexGetKey)
		if err != nil {
			return nil, err
		}
		key, err := tx.IndexGetKey(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Key{Key: &proto.KeyResponse{Key: key}}
	case *proto.TransactionOperation_IndexGetAll:
		query, err := indexQueryRequestFromProto(body.IndexGetAll)
		if err != nil {
			return nil, err
		}
		records, err := tx.IndexGetAll(ctx, query)
		if err != nil {
			return nil, err
		}
		pbRecords, err := recordsToProto(records)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Records{Records: &proto.RecordsResponse{Records: pbRecords}}
	case *proto.TransactionOperation_IndexGetAllKeys:
		query, err := indexQueryRequestFromProto(body.IndexGetAllKeys)
		if err != nil {
			return nil, err
		}
		keys, err := tx.IndexGetAllKeys(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Keys{Keys: &proto.KeysResponse{Keys: keys}}
	case *proto.TransactionOperation_IndexCount:
		query, err := indexQueryRequestFromProto(body.IndexCount)
		if err != nil {
			return nil, err
		}
		count, err := tx.IndexCount(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Count{Count: &proto.CountResponse{Count: count}}
	case *proto.TransactionOperation_IndexDelete:
		query, err := indexQueryRequestFromProto(body.IndexDelete)
		if err != nil {
			return nil, err
		}
		deleted, err := tx.IndexDelete(ctx, query)
		if err != nil {
			return nil, err
		}
		resp.Result = &proto.TransactionOperationResponse_Delete{Delete: &proto.DeleteResponse{Deleted: deleted}}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown transaction operation")
	}
	return resp, nil
}

func recordRequestFromProto(req *proto.RecordRequest) (gestalt.IndexedDBRecordRequest, error) {
	record, err := recordFromProto(req.GetRecord())
	if err != nil {
		return gestalt.IndexedDBRecordRequest{}, fmt.Errorf("unmarshal record: %w", err)
	}
	return gestalt.IndexedDBRecordRequest{Store: req.GetStore(), Record: record}, nil
}

func transactionOperationError(requestID uint64, err error) *proto.TransactionOperationResponse {
	return &proto.TransactionOperationResponse{RequestId: requestID, Error: rpcStatusFromError(err)}
}

func rpcStatusFromError(err error) *rpcstatus.Status {
	if err == nil {
		return nil
	}
	rpcErr := indexedDBTestRPCError("indexeddb", err)
	st, ok := status.FromError(rpcErr)
	if !ok {
		return &rpcstatus.Status{Code: int32(codes.Internal), Message: rpcErr.Error()}
	}
	return &rpcstatus.Status{Code: int32(st.Code()), Message: st.Message()}
}

func readonlyOperationError(mode gestalt.TransactionMode, op *proto.TransactionOperation) error {
	if mode == gestalt.TransactionReadwrite || op == nil {
		return nil
	}
	if isWriteTransactionOperation(op) {
		return gestalt.FailedPrecondition("transaction is readonly")
	}
	return nil
}

func isWriteTransactionOperation(op *proto.TransactionOperation) bool {
	switch op.GetOperation().(type) {
	case *proto.TransactionOperation_Add,
		*proto.TransactionOperation_Put,
		*proto.TransactionOperation_Delete,
		*proto.TransactionOperation_Clear,
		*proto.TransactionOperation_DeleteRange,
		*proto.TransactionOperation_IndexDelete:
		return true
	default:
		return false
	}
}

func drainIndexedDBTransaction(stream proto.IndexedDB_TransactionServer) error {
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

func indexedDBTestRPCError(operation string, err error) error {
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

func recordToProto(record gestalt.Record) (*proto.Record, error) {
	return proto.RecordFromNative(record)
}

func recordFromProto(record *proto.Record) (gestalt.Record, error) {
	return proto.RecordToNative(record)
}

func recordsToProto(records []gestalt.Record) ([]*proto.Record, error) {
	return proto.RecordsFromNative(records)
}
