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
	client        indexedDBClient
	stateName     string
	modelsName    string
	relationsName string
	state         objectStore
	models        objectStore
	relationships objectStore
}

type storedModel struct {
	ref      *gestalt.AuthorizationModelRef
	model    *gestalt.AuthorizationModel
	compiled *compiledModel
}

func openStore(ctx context.Context, cfg config) (*store, error) {
	client, err := connectIndexedDB(cfg.IndexedDB)
	if err != nil {
		return nil, fmt.Errorf("connect indexeddb: %w", err)
	}
	return openStoreWithConn(ctx, client)
}

func openStoreWithConn(ctx context.Context, client indexedDBClient) (*store, error) {
	if err := ensureAuthorizationStores(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
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
	return st, nil
}

func ensureAuthorizationStores(ctx context.Context, client indexedDBClient) error {
	if client == nil {
		return nil
	}
	if err := client.CreateObjectStore(ctx, stateStoreName, gestalt.ObjectStoreSchema{}); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create authorization state store: %w", err)
	}
	if err := client.CreateObjectStore(ctx, modelsStoreName, gestalt.ObjectStoreSchema{}); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create authorization models store: %w", err)
	}
	if err := client.CreateObjectStore(ctx, relationsStoreName, authorizationRelationshipsSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create authorization relationships store: %w", err)
	}
	return nil
}

func authorizationRelationshipsSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: relationshipsBySubj, KeyPath: []string{"subject_type", "subject_id"}},
			{Name: relationshipsByRes, KeyPath: []string{"resource_type", "resource_id"}},
			{Name: relationshipsByPair, KeyPath: []string{"subject_type", "subject_id", "resource_type", "resource_id"}},
		},
	}
}

func (s *store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
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
		"created_at": ref.GetCreatedAt().UTC(),
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
		left := refs[i].GetCreatedAt()
		right := refs[j].GetCreatedAt()
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
		ref:      gestalt.NewAuthorizationModelRef(id, version, createdAt.UTC()),
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

func (s *store) directRelationshipExists(ctx context.Context, target compiledRelationshipTarget, relation string, resource *gestalt.AuthorizationResource) (bool, error) {
	_, err := s.relationships.Get(ctx, relationshipTargetTupleID(target, relation, resource.GetType(), resource.GetId()))
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
	target, subject, err := normalizedRelationshipTarget(relationship.GetTarget(), relationship.GetSubject())
	if err != nil {
		return nil, err
	}
	resource := relationship.GetResource()
	if err := validateResource(resource); err != nil {
		return nil, err
	}
	relation := strings.TrimSpace(relationship.GetRelation())
	if relation == "" {
		return nil, status.Error(codes.InvalidArgument, "relationship relation is required")
	}
	record := gestalt.Record{
		"id":                  relationshipTargetTupleID(target, relation, resource.GetType(), resource.GetId()),
		"target_kind":         target.Kind,
		"relation":            relation,
		"resource_type":       resource.GetType(),
		"resource_id":         resource.GetId(),
		"resource_properties": nilIfEmptyMap(resource.GetProperties()),
		"properties":          nilIfEmptyMap(relationship.GetProperties()),
	}
	if subject != nil {
		record["subject_type"] = subject.GetType()
		record["subject_id"] = subject.GetId()
		record["subject_properties"] = nilIfEmptyMap(subject.GetProperties())
	}
	switch target.Kind {
	case "subject":
		record["target_subject_type"] = target.SubjectType
		record["target_subject_id"] = target.SubjectID
	case "resource":
		record["target_resource_type"] = target.ResourceType
		record["target_resource_id"] = target.ResourceID
		record["target_resource_properties"] = nilIfEmptyMap(relationship.GetTarget().GetResource().GetProperties())
	case "subject_set":
		record["target_subject_set_resource_type"] = target.SubjectSetResourceType
		record["target_subject_set_resource_id"] = target.SubjectSetResourceID
		record["target_subject_set_relation"] = target.SubjectSetRelation
		record["target_subject_set_resource_properties"] = nilIfEmptyMap(relationship.GetTarget().GetSubjectSet().GetResource().GetProperties())
	}
	return record, nil
}

func relationshipFromRecord(record gestalt.Record) (*gestalt.Relationship, error) {
	resourceProperties, err := propertiesFromRecord(record["resource_properties"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode resource properties: %v", err)
	}
	relationshipProperties, err := propertiesFromRecord(record["properties"])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode relationship properties: %v", err)
	}

	_, target, subject, err := relationshipTargetFromRecord(record)
	if err != nil {
		return nil, err
	}
	resourceType, _ := record["resource_type"].(string)
	resourceID, _ := record["resource_id"].(string)
	relation, _ := record["relation"].(string)
	if resourceType == "" || resourceID == "" || relation == "" {
		return nil, status.Error(codes.Internal, "stored relationship is incomplete")
	}

	return &gestalt.Relationship{
		Subject:  subject,
		Target:   target,
		Relation: relation,
		Resource: &gestalt.AuthorizationResource{
			Type:       resourceType,
			Id:         resourceID,
			Properties: resourceProperties,
		},
		Properties: relationshipProperties,
	}, nil
}

func normalizedRelationshipTarget(target *gestalt.AuthorizationRelationshipTarget, subject *gestalt.AuthorizationSubject) (compiledRelationshipTarget, *gestalt.AuthorizationSubject, error) {
	if target == nil {
		if err := validateSubject(subject); err != nil {
			return compiledRelationshipTarget{}, nil, err
		}
		return compiledRelationshipTarget{Kind: "subject", SubjectType: subject.GetType(), SubjectID: subject.GetId()}, subject, nil
	}
	if targetSubject := target.GetSubject(); targetSubject != nil {
		if err := validateSubject(targetSubject); err != nil {
			return compiledRelationshipTarget{}, nil, err
		}
		if subject != nil && (subject.GetType() != targetSubject.GetType() || subject.GetId() != targetSubject.GetId()) {
			return compiledRelationshipTarget{}, nil, status.Error(codes.InvalidArgument, "relationship subject and target subject do not match")
		}
		return compiledRelationshipTarget{Kind: "subject", SubjectType: targetSubject.GetType(), SubjectID: targetSubject.GetId()}, targetSubject, nil
	}
	if subject != nil {
		return compiledRelationshipTarget{}, nil, status.Error(codes.InvalidArgument, "relationship subject cannot be set with a non-subject target")
	}
	if targetResource := target.GetResource(); targetResource != nil {
		if err := validateResource(targetResource); err != nil {
			return compiledRelationshipTarget{}, nil, err
		}
		return compiledRelationshipTarget{Kind: "resource", ResourceType: targetResource.GetType(), ResourceID: targetResource.GetId()}, nil, nil
	}
	if targetSet := target.GetSubjectSet(); targetSet != nil {
		resource := targetSet.GetResource()
		if err := validateResource(resource); err != nil {
			return compiledRelationshipTarget{}, nil, err
		}
		relation := strings.TrimSpace(targetSet.GetRelation())
		if relation == "" {
			return compiledRelationshipTarget{}, nil, status.Error(codes.InvalidArgument, "subject set relation is required")
		}
		return compiledRelationshipTarget{Kind: "subject_set", SubjectSetResourceType: resource.GetType(), SubjectSetResourceID: resource.GetId(), SubjectSetRelation: relation}, nil, nil
	}
	return compiledRelationshipTarget{}, nil, status.Error(codes.InvalidArgument, "relationship target kind is required")
}

func relationshipTargetFromRecord(record gestalt.Record) (compiledRelationshipTarget, *gestalt.AuthorizationRelationshipTarget, *gestalt.AuthorizationSubject, error) {
	kind, _ := record["target_kind"].(string)
	if kind == "" {
		kind = "subject"
	}
	subjectProperties, err := propertiesFromRecord(record["subject_properties"])
	if err != nil {
		return compiledRelationshipTarget{}, nil, nil, status.Errorf(codes.Internal, "decode subject properties: %v", err)
	}
	subjectType, _ := record["subject_type"].(string)
	subjectID, _ := record["subject_id"].(string)
	switch kind {
	case "subject":
		targetSubjectType, _ := record["target_subject_type"].(string)
		targetSubjectID, _ := record["target_subject_id"].(string)
		if targetSubjectType == "" {
			targetSubjectType = subjectType
		}
		if targetSubjectID == "" {
			targetSubjectID = subjectID
		}
		if targetSubjectType == "" || targetSubjectID == "" {
			return compiledRelationshipTarget{}, nil, nil, status.Error(codes.Internal, "stored relationship subject target is incomplete")
		}
		subject := &gestalt.AuthorizationSubject{Type: targetSubjectType, Id: targetSubjectID, Properties: subjectProperties}
		return compiledRelationshipTarget{Kind: "subject", SubjectType: targetSubjectType, SubjectID: targetSubjectID}, gestalt.NewAuthorizationSubjectTarget(subject), subject, nil
	case "resource":
		resourceType, _ := record["target_resource_type"].(string)
		resourceID, _ := record["target_resource_id"].(string)
		if resourceType == "" || resourceID == "" {
			return compiledRelationshipTarget{}, nil, nil, status.Error(codes.Internal, "stored relationship resource target is incomplete")
		}
		properties, err := propertiesFromRecord(record["target_resource_properties"])
		if err != nil {
			return compiledRelationshipTarget{}, nil, nil, status.Errorf(codes.Internal, "decode target resource properties: %v", err)
		}
		resource := &gestalt.AuthorizationResource{Type: resourceType, Id: resourceID, Properties: properties}
		return compiledRelationshipTarget{Kind: "resource", ResourceType: resourceType, ResourceID: resourceID}, gestalt.NewAuthorizationResourceTarget(resource), nil, nil
	case "subject_set":
		resourceType, _ := record["target_subject_set_resource_type"].(string)
		resourceID, _ := record["target_subject_set_resource_id"].(string)
		relation, _ := record["target_subject_set_relation"].(string)
		if resourceType == "" || resourceID == "" || relation == "" {
			return compiledRelationshipTarget{}, nil, nil, status.Error(codes.Internal, "stored relationship subject-set target is incomplete")
		}
		properties, err := propertiesFromRecord(record["target_subject_set_resource_properties"])
		if err != nil {
			return compiledRelationshipTarget{}, nil, nil, status.Errorf(codes.Internal, "decode target subject-set resource properties: %v", err)
		}
		resource := &gestalt.AuthorizationResource{Type: resourceType, Id: resourceID, Properties: properties}
		return compiledRelationshipTarget{Kind: "subject_set", SubjectSetResourceType: resourceType, SubjectSetResourceID: resourceID, SubjectSetRelation: relation}, gestalt.NewAuthorizationSubjectSetTarget(resource, relation), nil, nil
	default:
		return compiledRelationshipTarget{}, nil, nil, status.Errorf(codes.Internal, "stored relationship has unsupported target kind %q", kind)
	}
}

func nilIfEmptyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func propertiesFromRecord(value any) (map[string]any, error) {
	value = nilIfEmptyRecordMap(value)
	if value == nil {
		return nil, nil
	}
	if typed, ok := value.(map[string]any); ok {
		return nilIfEmptyMap(typed), nil
	}
	if typed, ok := value.(map[string]string); ok {
		out := make(map[string]any, len(typed))
		for key, raw := range typed {
			out[key] = raw
		}
		return nilIfEmptyMap(out), nil
	}
	return nil, fmt.Errorf("expected properties map, got %T", value)
}

func nilIfEmptyRecordMap(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		if len(typed) == 0 {
			return nil
		}
		return typed
	default:
		return value
	}
}

func relationshipKeyID(key *gestalt.RelationshipKey) (string, error) {
	if key == nil {
		return "", status.Error(codes.InvalidArgument, "relationship key is required")
	}
	target, _, err := normalizedRelationshipTarget(key.GetTarget(), key.GetSubject())
	if err != nil {
		return "", err
	}
	if err := validateResource(key.GetResource()); err != nil {
		return "", err
	}
	relation := strings.TrimSpace(key.GetRelation())
	if relation == "" {
		return "", status.Error(codes.InvalidArgument, "relationship relation is required")
	}
	return relationshipTargetTupleID(target, relation, key.GetResource().GetType(), key.GetResource().GetId()), nil
}

func relationshipTupleID(subjectType, subjectID, relation, resourceType, resourceID string) string {
	parts := []string{subjectType, subjectID, relation, resourceType, resourceID}
	encoded := make([]string, len(parts))
	for i, part := range parts {
		encoded[i] = base64.RawURLEncoding.EncodeToString([]byte(part))
	}
	return strings.Join(encoded, ".")
}

func relationshipTargetTupleID(target compiledRelationshipTarget, relation, resourceType, resourceID string) string {
	if target.Kind == "subject" {
		return relationshipTupleID(target.SubjectType, target.SubjectID, relation, resourceType, resourceID)
	}
	parts := []string{
		"target", target.Kind,
		target.SubjectType, target.SubjectID,
		target.ResourceType, target.ResourceID,
		target.SubjectSetResourceType, target.SubjectSetResourceID, target.SubjectSetRelation,
		relation, resourceType, resourceID,
	}
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
