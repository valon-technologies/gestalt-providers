package indexeddb

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	stateRecordID       = "state"
	activeModelIDField  = "active_model_id"
	relationshipsBySubj = "by_subject"
	relationshipsByRes  = "by_resource"
	relationshipsByPair = "by_subject_resource"
	stateStoreName      = "state"
	modelsStoreName     = "models"
	relationsStoreName  = "relationships"
)

type store struct {
	client        *gestalt.IndexedDBClient
	stateName     string
	modelsName    string
	relationsName string
	state         *gestalt.ObjectStoreClient
	models        *gestalt.ObjectStoreClient
	relationships *gestalt.ObjectStoreClient
}

type storedModel struct {
	ref      *gestalt.AuthorizationModelRef
	model    *gestalt.AuthorizationModel
	compiled *compiledModel
}

func openStore(ctx context.Context, cfg config) (*store, error) {
	var (
		client *gestalt.IndexedDBClient
		err    error
	)
	if cfg.IndexedDB == "" {
		client, err = gestalt.IndexedDB()
	} else {
		client, err = gestalt.IndexedDB(cfg.IndexedDB)
	}
	if err != nil {
		return nil, fmt.Errorf("connect indexeddb: %w", err)
	}

	st := &store{
		client:        client,
		stateName:     stateStoreName,
		modelsName:    modelsStoreName,
		relationsName: relationsStoreName,
	}
	st.state = client.ObjectStore(st.stateName)
	st.models = client.ObjectStore(st.modelsName)
	st.relationships = client.ObjectStore(st.relationsName)
	if err := st.ensure(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return st, nil
}

func (s *store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *store) ensure(ctx context.Context) error {
	definitions := []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: s.stateName, schema: gestalt.ObjectStoreSchema{}},
		{name: s.modelsName, schema: gestalt.ObjectStoreSchema{}},
		{
			name: s.relationsName,
			schema: gestalt.ObjectStoreSchema{
				Indexes: []gestalt.IndexSchema{
					{Name: relationshipsBySubj, KeyPath: []string{"subject_type", "subject_id"}},
					{Name: relationshipsByRes, KeyPath: []string{"resource_type", "resource_id"}},
					{Name: relationshipsByPair, KeyPath: []string{"subject_type", "subject_id", "resource_type", "resource_id"}},
				},
			},
		},
	}
	for _, definition := range definitions {
		if err := s.client.CreateObjectStore(ctx, definition.name, definition.schema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			return fmt.Errorf("create object store %q: %w", definition.name, err)
		}
	}
	return nil
}

func (s *store) activeModelID(ctx context.Context) (string, error) {
	record, err := s.state.Get(ctx, stateRecordID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	value, _ := record[activeModelIDField].(string)
	return strings.TrimSpace(value), nil
}

func (s *store) setActiveModelID(ctx context.Context, modelID string) error {
	return s.state.Put(ctx, gestalt.Record{
		"id":               stateRecordID,
		activeModelIDField: strings.TrimSpace(modelID),
	})
}

func (s *store) writeModel(ctx context.Context, ref *gestalt.AuthorizationModelRef, modelJSON string) error {
	if ref == nil {
		return status.Error(codes.InvalidArgument, "model ref is required")
	}
	record := gestalt.Record{
		"id":         ref.GetId(),
		"version":    ref.GetVersion(),
		"created_at": ref.GetCreatedAt().AsTime().UTC(),
		"model_json": modelJSON,
	}
	if err := s.models.Put(ctx, record); err != nil {
		return err
	}
	return s.setActiveModelID(ctx, ref.GetId())
}

func (s *store) loadModel(ctx context.Context, modelID string) (*storedModel, error) {
	record, err := s.models.Get(ctx, modelID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "authorization model %q was not found", modelID)
		}
		return nil, err
	}
	return modelFromRecord(record)
}

func (s *store) listModels(ctx context.Context) ([]*gestalt.AuthorizationModelRef, error) {
	records, err := s.models.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	refs := make([]*gestalt.AuthorizationModelRef, 0, len(records))
	for _, record := range records {
		model, err := modelFromRecord(record)
		if err != nil {
			return nil, err
		}
		refs = append(refs, model.ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		left := refs[i].GetCreatedAt().AsTime()
		right := refs[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.After(right)
		}
		return refs[i].GetId() < refs[j].GetId()
	})
	return refs, nil
}

func modelFromRecord(record gestalt.Record) (*storedModel, error) {
	id, _ := record["id"].(string)
	if strings.TrimSpace(id) == "" {
		return nil, status.Error(codes.Internal, "stored model is missing id")
	}
	version, _ := record["version"].(string)
	modelJSON, _ := record["model_json"].(string)
	createdAt, err := recordTime(record["created_at"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stored model %q has invalid created_at: %v", id, err)
	}
	model, err := unmarshalStoredModel(modelJSON)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stored model %q is invalid: %v", id, err)
	}
	compiled, normalized, err := compileAuthorizationModel(model)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stored model %q is invalid: %v", id, err)
	}
	if strings.TrimSpace(version) == "" {
		version = modelVersionString(normalized)
	}
	return &storedModel{
		ref: &gestalt.AuthorizationModelRef{
			Id:        id,
			Version:   version,
			CreatedAt: timestamppb.New(createdAt.UTC()),
		},
		model:    normalized,
		compiled: compiled,
	}, nil
}

func recordTime(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return time.Time{}, err
		}
		return parsed, nil
	default:
		return time.Time{}, fmt.Errorf("expected time, got %T", value)
	}
}

func (s *store) putRelationship(ctx context.Context, relationship *gestalt.Relationship) error {
	record, err := relationshipRecord(relationship)
	if err != nil {
		return err
	}
	return s.relationships.Put(ctx, record)
}

func (s *store) deleteRelationship(ctx context.Context, key *gestalt.RelationshipKey) error {
	id, err := relationshipKeyID(key)
	if err != nil {
		return err
	}
	err = s.relationships.Delete(ctx, id)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	return err
}

func (s *store) relationshipExists(ctx context.Context, subjectType, subjectID, relation, resourceType, resourceID string) (bool, error) {
	_, err := s.relationships.Get(ctx, relationshipTupleID(subjectType, subjectID, relation, resourceType, resourceID))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) candidateRelationships(ctx context.Context, subject *gestalt.AuthorizationSubject, resource *gestalt.AuthorizationResource) ([]*gestalt.Relationship, error) {
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case subject != nil && resource != nil:
		records, err = s.relationships.Index(relationshipsByPair).GetAll(ctx, nil, subject.GetType(), subject.GetId(), resource.GetType(), resource.GetId())
	case subject != nil:
		records, err = s.relationships.Index(relationshipsBySubj).GetAll(ctx, nil, subject.GetType(), subject.GetId())
	case resource != nil:
		records, err = s.relationships.Index(relationshipsByRes).GetAll(ctx, nil, resource.GetType(), resource.GetId())
	default:
		records, err = s.relationships.GetAll(ctx, nil)
	}
	if err != nil {
		return nil, err
	}
	relationships := make([]*gestalt.Relationship, 0, len(records))
	for _, record := range records {
		relationship, err := relationshipFromRecord(record)
		if err != nil {
			return nil, err
		}
		relationships = append(relationships, relationship)
	}
	return relationships, nil
}

func relationshipRecord(relationship *gestalt.Relationship) (gestalt.Record, error) {
	if relationship == nil {
		return nil, status.Error(codes.InvalidArgument, "relationship is required")
	}
	subject := relationship.GetSubject()
	resource := relationship.GetResource()
	if err := validateSubject(subject); err != nil {
		return nil, err
	}
	if err := validateResource(resource); err != nil {
		return nil, err
	}
	relation := strings.TrimSpace(relationship.GetRelation())
	if relation == "" {
		return nil, status.Error(codes.InvalidArgument, "relationship relation is required")
	}
	return gestalt.Record{
		"id":                  relationshipTupleID(subject.GetType(), subject.GetId(), relation, resource.GetType(), resource.GetId()),
		"subject_type":        subject.GetType(),
		"subject_id":          subject.GetId(),
		"subject_properties":  structAsMap(subject.GetProperties()),
		"relation":            relation,
		"resource_type":       resource.GetType(),
		"resource_id":         resource.GetId(),
		"resource_properties": structAsMap(resource.GetProperties()),
		"properties":          structAsMap(relationship.GetProperties()),
	}, nil
}

func relationshipFromRecord(record gestalt.Record) (*gestalt.Relationship, error) {
	subjectProperties, err := mapAsStruct(record["subject_properties"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode subject properties: %v", err)
	}
	resourceProperties, err := mapAsStruct(record["resource_properties"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode resource properties: %v", err)
	}
	relationshipProperties, err := mapAsStruct(record["properties"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode relationship properties: %v", err)
	}

	subjectType, _ := record["subject_type"].(string)
	subjectID, _ := record["subject_id"].(string)
	resourceType, _ := record["resource_type"].(string)
	resourceID, _ := record["resource_id"].(string)
	relation, _ := record["relation"].(string)
	if subjectType == "" || subjectID == "" || resourceType == "" || resourceID == "" || relation == "" {
		return nil, status.Error(codes.Internal, "stored relationship is incomplete")
	}

	return &gestalt.Relationship{
		Subject: &gestalt.AuthorizationSubject{
			Type:       subjectType,
			Id:         subjectID,
			Properties: subjectProperties,
		},
		Relation: relation,
		Resource: &gestalt.AuthorizationResource{
			Type:       resourceType,
			Id:         resourceID,
			Properties: resourceProperties,
		},
		Properties: relationshipProperties,
	}, nil
}

func structAsMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	out := value.AsMap()
	if len(out) == 0 {
		return nil
	}
	return out
}

func mapAsStruct(value any) (*structpb.Struct, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		if len(typed) == 0 {
			return nil, nil
		}
		return structpb.NewStruct(typed)
	default:
		return nil, fmt.Errorf("expected map[string]any, got %T", value)
	}
}

func relationshipKeyID(key *gestalt.RelationshipKey) (string, error) {
	if key == nil {
		return "", status.Error(codes.InvalidArgument, "relationship key is required")
	}
	if err := validateSubject(key.GetSubject()); err != nil {
		return "", err
	}
	if err := validateResource(key.GetResource()); err != nil {
		return "", err
	}
	relation := strings.TrimSpace(key.GetRelation())
	if relation == "" {
		return "", status.Error(codes.InvalidArgument, "relationship relation is required")
	}
	return relationshipTupleID(key.GetSubject().GetType(), key.GetSubject().GetId(), relation, key.GetResource().GetType(), key.GetResource().GetId()), nil
}

func relationshipTupleID(subjectType, subjectID, relation, resourceType, resourceID string) string {
	parts := []string{subjectType, subjectID, relation, resourceType, resourceID}
	encoded := make([]string, len(parts))
	for i, part := range parts {
		encoded[i] = base64.RawURLEncoding.EncodeToString([]byte(part))
	}
	return strings.Join(encoded, ".")
}

func newModelID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate model id: %w", err)
	}
	return "mdl_" + hex.EncodeToString(buf[:]), nil
}

func validateSubject(subject *gestalt.AuthorizationSubject) error {
	if subject == nil {
		return status.Error(codes.InvalidArgument, "subject is required")
	}
	if strings.TrimSpace(subject.GetType()) == "" {
		return status.Error(codes.InvalidArgument, "subject type is required")
	}
	if strings.TrimSpace(subject.GetId()) == "" {
		return status.Error(codes.InvalidArgument, "subject id is required")
	}
	return nil
}

func validateResource(resource *gestalt.AuthorizationResource) error {
	if resource == nil {
		return status.Error(codes.InvalidArgument, "resource is required")
	}
	if strings.TrimSpace(resource.GetType()) == "" {
		return status.Error(codes.InvalidArgument, "resource type is required")
	}
	if strings.TrimSpace(resource.GetId()) == "" {
		return status.Error(codes.InvalidArgument, "resource id is required")
	}
	return nil
}
