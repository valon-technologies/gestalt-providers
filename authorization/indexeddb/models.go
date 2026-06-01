package indexeddb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type stateKeys struct {
	activeModel string
}

func getStateKeys() stateKeys {
	return stateKeys{
		activeModel: "active_model",
	}
}

func putModel(ctx context.Context, store indexeddb.ObjectStore, model *AuthorizationModel) error {
	record, err := modelToRecord(model)
	if err != nil {
		return err
	}
	return store.Put(ctx, record)
}

func getModel(ctx context.Context, store indexeddb.ObjectStore, id string) (*AuthorizationModel, error) {
	record, err := store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return modelFromRecord(record)
}

func putActiveModelRef(ctx context.Context, store indexeddb.ObjectStore, key string, ref *AuthorizationModelRef) error {
	record, err := modelRefToRecord(key, ref)
	if err != nil {
		return err
	}
	return store.Put(ctx, record)
}

func getActiveModelRef(ctx context.Context, store indexeddb.ObjectStore, key string) (*AuthorizationModelRef, error) {
	record, err := store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return modelRefFromRecord(record)
}

func modelToRecord(model *AuthorizationModel) (indexeddb.Record, error) {
	resourceTypes, err := jsonValue(model.ResourceTypes)
	if err != nil {
		return nil, fmt.Errorf("encode resource types: %w", err)
	}
	return indexeddb.Record{
		"id":             model.Id,
		"version":        model.Version,
		"resource_types": resourceTypes,
	}, nil
}

func modelFromRecord(record indexeddb.Record) (*AuthorizationModel, error) {
	resourceTypes, err := resourceTypesFromAny(record["resource_types"])
	if err != nil {
		return nil, err
	}
	return &AuthorizationModel{
		Id:            stringField(record, "id"),
		Version:       stringField(record, "version"),
		ResourceTypes: resourceTypes,
	}, nil
}

func modelRefToRecord(id string, ref *AuthorizationModelRef) (indexeddb.Record, error) {
	value, err := jsonValue(ref)
	if err != nil {
		return nil, fmt.Errorf("encode model ref: %w", err)
	}
	return indexeddb.Record{
		"id":    id,
		"value": value,
	}, nil
}

func modelRefFromRecord(record indexeddb.Record) (*AuthorizationModelRef, error) {
	var ref AuthorizationModelRef
	if err := decodeJSONValue(record["value"], &ref); err != nil {
		return nil, fmt.Errorf("decode model ref: %w", err)
	}
	if strings.TrimSpace(ref.Id) == "" {
		return nil, nil
	}
	return &ref, nil
}

func resourceTypesFromAny(value any) ([]*AuthorizationModelResourceType, error) {
	var resourceTypes []*AuthorizationModelResourceType
	if err := decodeJSONValue(value, &resourceTypes); err != nil {
		return nil, fmt.Errorf("decode resource types: %w", err)
	}
	return resourceTypes, nil
}

func jsonValue(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeJSONValue(value any, out any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func stringField(record indexeddb.Record, key string) string {
	value, _ := record[key].(string)
	return strings.TrimSpace(value)
}

func (m *AuthorizationModel) toRef(createdAt time.Time) *AuthorizationModelRef {
	if m == nil {
		return nil
	}
	return &AuthorizationModelRef{
		Id:        m.Id,
		Version:   m.Version,
		CreatedAt: createdAt,
	}
}

func cloneAuthorizationModel(model *AuthorizationModel) *AuthorizationModel {
	if model == nil {
		return nil
	}
	return &AuthorizationModel{
		Id:            model.Id,
		Version:       model.Version,
		ResourceTypes: cloneAuthorizationModelResourceTypes(model.ResourceTypes),
	}
}

func cloneAuthorizationModelRef(ref *AuthorizationModelRef) *AuthorizationModelRef {
	if ref == nil {
		return nil
	}
	return &AuthorizationModelRef{
		Id:        ref.Id,
		Version:   ref.Version,
		CreatedAt: ref.CreatedAt,
	}
}

func cloneAuthorizationModelResourceTypes(resourceTypes []*AuthorizationModelResourceType) []*AuthorizationModelResourceType {
	if resourceTypes == nil {
		return nil
	}
	out := make([]*AuthorizationModelResourceType, 0, len(resourceTypes))
	for _, resourceType := range resourceTypes {
		cloned := cloneAuthorizationModelResourceType(resourceType)
		if cloned != nil {
			out = append(out, cloned)
		}
	}
	return out
}

func cloneAuthorizationModelResourceType(resourceType *AuthorizationModelResourceType) *AuthorizationModelResourceType {
	if resourceType == nil {
		return nil
	}
	return &AuthorizationModelResourceType{
		Name:        resourceType.Name,
		Relations:   cloneAuthorizationModelRelations(resourceType.Relations),
		Actions:     cloneAuthorizationModelActions(resourceType.Actions),
		SourceLayer: resourceType.SourceLayer,
	}
}

func filterAuthorizationModelResourceTypes(resourceTypes []*AuthorizationModelResourceType, filter *AuthorizationModelResourceTypeFilter) []*AuthorizationModelResourceType {
	if filter == nil {
		return cloneAuthorizationModelResourceTypes(resourceTypes)
	}
	name := strings.TrimSpace(filter.Name)
	out := make([]*AuthorizationModelResourceType, 0, len(resourceTypes))
	for _, resourceType := range resourceTypes {
		if resourceType == nil {
			continue
		}
		if name != "" && resourceType.Name != name {
			continue
		}
		if filter.SourceLayer != SourceLayerUnspecified && resourceType.SourceLayer != filter.SourceLayer {
			continue
		}
		out = append(out, cloneAuthorizationModelResourceType(resourceType))
	}
	return out
}

func cloneAuthorizationModelRelations(relations []*AuthorizationModelRelation) []*AuthorizationModelRelation {
	if relations == nil {
		return nil
	}
	out := make([]*AuthorizationModelRelation, 0, len(relations))
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		out = append(out, &AuthorizationModelRelation{
			Name:           relation.Name,
			AllowedTargets: cloneAuthorizationModelAllowedTargets(relation.AllowedTargets),
		})
	}
	return out
}

func cloneAuthorizationModelActions(actions []*AuthorizationModelAction) []*AuthorizationModelAction {
	if actions == nil {
		return nil
	}
	out := make([]*AuthorizationModelAction, 0, len(actions))
	for _, action := range actions {
		if action == nil {
			continue
		}
		out = append(out, &AuthorizationModelAction{
			Name:      action.Name,
			Relations: append([]string(nil), action.Relations...),
		})
	}
	return out
}

func cloneAuthorizationModelAllowedTargets(targets []*AuthorizationModelAllowedTarget) []*AuthorizationModelAllowedTarget {
	if targets == nil {
		return nil
	}
	out := make([]*AuthorizationModelAllowedTarget, 0, len(targets))
	for _, target := range targets {
		if target == nil {
			continue
		}
		out = append(out, &AuthorizationModelAllowedTarget{
			SubjectType:    target.SubjectType,
			ResourceType:   target.ResourceType,
			SubjectSetType: cloneSubjectSetType(target.SubjectSetType),
		})
	}
	return out
}

func cloneSubjectSetType(subjectSetType *SubjectSetType) *SubjectSetType {
	if subjectSetType == nil {
		return nil
	}
	return &SubjectSetType{
		ResourceType: subjectSetType.ResourceType,
		Relation:     subjectSetType.Relation,
	}
}
