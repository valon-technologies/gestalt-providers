package indexeddb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const defaultModelResourceTypePageSize = 100

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
	resourceTypes, err := authorizationModelResourceTypesToJSONValue(model.ResourceTypes)
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
	value, err := authorizationModelRefToJSONValue(ref)
	if err != nil {
		return nil, fmt.Errorf("encode model ref: %w", err)
	}
	return indexeddb.Record{
		"id":    id,
		"value": value,
	}, nil
}

func modelRefFromRecord(record indexeddb.Record) (*AuthorizationModelRef, error) {
	ref, err := authorizationModelRefFromJSONValue(record["value"])
	if err != nil {
		return nil, fmt.Errorf("decode model ref: %w", err)
	}
	if strings.TrimSpace(ref.Id) == "" {
		return nil, nil
	}
	return ref, nil
}

func resourceTypesFromAny(value any) ([]*AuthorizationModelResourceType, error) {
	resourceTypes, err := authorizationModelResourceTypesFromJSONValue(value)
	if err != nil {
		return nil, fmt.Errorf("decode resource types: %w", err)
	}
	return resourceTypes, nil
}

func parseModelResourceTypePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("offset must be non-negative")
	}
	return offset, nil
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

func normalizeAuthorizationModel(model *AuthorizationModel) error {
	if model == nil {
		return fmt.Errorf("model is required")
	}
	model.Id = strings.TrimSpace(model.Id)
	if model.Id == "" {
		return fmt.Errorf("model id is required")
	}
	model.Version = strings.TrimSpace(model.Version)
	for i, resourceType := range model.ResourceTypes {
		if err := normalizeAuthorizationModelResourceType(resourceType); err != nil {
			return fmt.Errorf("resource_types[%d]: %w", i, err)
		}
	}
	return nil
}

func normalizeAuthorizationModelResourceType(resourceType *AuthorizationModelResourceType) error {
	if resourceType == nil {
		return fmt.Errorf("resource type is required")
	}
	resourceType.Name = strings.TrimSpace(resourceType.Name)
	if resourceType.Name == "" {
		return fmt.Errorf("name is required")
	}
	if err := normalizeDefaultAccessPolicy(resourceType.DefaultAccessPolicy); err != nil {
		return err
	}
	for i, relation := range resourceType.Relations {
		if err := normalizeAuthorizationModelRelation(relation); err != nil {
			return fmt.Errorf("relations[%d]: %w", i, err)
		}
	}
	for i, action := range resourceType.Actions {
		if err := normalizeAuthorizationModelAction(action); err != nil {
			return fmt.Errorf("actions[%d]: %w", i, err)
		}
	}
	return nil
}

func normalizeDefaultAccessPolicy(policy DefaultAccessPolicy) error {
	switch policy {
	case DefaultAccessPolicyDeny, DefaultAccessPolicyAllow:
		return nil
	default:
		return fmt.Errorf("default access policy is invalid")
	}
}

func normalizeAuthorizationModelRelation(relation *AuthorizationModelRelation) error {
	if relation == nil {
		return fmt.Errorf("relation is required")
	}
	relation.Name = strings.TrimSpace(relation.Name)
	if relation.Name == "" {
		return fmt.Errorf("name is required")
	}
	for i, target := range relation.AllowedTargets {
		if err := normalizeAuthorizationModelAllowedTarget(target); err != nil {
			return fmt.Errorf("allowed_targets[%d]: %w", i, err)
		}
	}
	return nil
}

func normalizeAuthorizationModelAction(action *AuthorizationModelAction) error {
	if action == nil {
		return fmt.Errorf("action is required")
	}
	action.Name = strings.TrimSpace(action.Name)
	if action.Name == "" {
		return fmt.Errorf("name is required")
	}
	for i, relation := range action.Relations {
		action.Relations[i] = strings.TrimSpace(relation)
		if action.Relations[i] == "" {
			return fmt.Errorf("relations[%d]: relation is required", i)
		}
	}
	return nil
}

func normalizeAuthorizationModelAllowedTarget(target *AuthorizationModelAllowedTarget) error {
	if target == nil {
		return fmt.Errorf("allowed target is required")
	}
	target.SubjectType = strings.TrimSpace(target.SubjectType)
	target.ResourceType = strings.TrimSpace(target.ResourceType)

	kinds := 0
	if target.SubjectType != "" {
		kinds++
	}
	if target.ResourceType != "" {
		kinds++
	}
	if target.SubjectSetType != nil {
		kinds++
		if err := normalizeSubjectSetType(target.SubjectSetType); err != nil {
			return err
		}
	}
	if kinds != 1 {
		return fmt.Errorf("allowed target must contain exactly one kind")
	}
	return nil
}

func normalizeSubjectSetType(subjectSetType *SubjectSetType) error {
	if subjectSetType == nil {
		return fmt.Errorf("subject set type is required")
	}
	subjectSetType.ResourceType = strings.TrimSpace(subjectSetType.ResourceType)
	subjectSetType.Relation = strings.TrimSpace(subjectSetType.Relation)
	if subjectSetType.ResourceType == "" {
		return fmt.Errorf("subject set resource type is required")
	}
	if subjectSetType.Relation == "" {
		return fmt.Errorf("subject set relation is required")
	}
	return nil
}

func authorizationModelToRef(model *AuthorizationModel, createdAt time.Time) *AuthorizationModelRef {
	if model == nil {
		return nil
	}
	return &AuthorizationModelRef{
		Id:        model.Id,
		Version:   model.Version,
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
		Name:                resourceType.Name,
		DefaultAccessPolicy: resourceType.DefaultAccessPolicy,
		Relations:           cloneAuthorizationModelRelations(resourceType.Relations),
		Actions:             cloneAuthorizationModelActions(resourceType.Actions),
		SourceLayer:         resourceType.SourceLayer,
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
