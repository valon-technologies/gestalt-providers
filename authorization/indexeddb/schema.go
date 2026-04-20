package indexeddb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type compiledModel struct {
	Version       string
	ResourceTypes map[string]compiledResourceType
}

type compiledResourceType struct {
	Relations map[string]compiledRelation
	Actions   map[string][]string
}

type compiledRelation struct {
	SubjectTypes map[string]struct{}
}

func compileAuthorizationModel(raw *gestalt.AuthorizationModel) (*compiledModel, *gestalt.AuthorizationModel, error) {
	if raw == nil {
		return nil, nil, fmt.Errorf("model is required")
	}

	version := raw.GetVersion()
	if version == 0 {
		version = 1
	}
	if version != 1 {
		return nil, nil, fmt.Errorf("unsupported model version %d", version)
	}
	if len(raw.GetResourceTypes()) == 0 {
		return nil, nil, fmt.Errorf("model must define at least one resource type")
	}

	model := &compiledModel{
		Version:       strconv.Itoa(int(version)),
		ResourceTypes: make(map[string]compiledResourceType, len(raw.GetResourceTypes())),
	}
	normalized := &gestalt.AuthorizationModel{Version: version}
	seenResourceTypes := make(map[string]struct{}, len(raw.GetResourceTypes()))
	for _, rawResource := range raw.GetResourceTypes() {
		resourceType := strings.TrimSpace(rawResource.GetName())
		if resourceType == "" {
			return nil, nil, fmt.Errorf("resource type names must be non-empty")
		}
		if _, exists := seenResourceTypes[resourceType]; exists {
			return nil, nil, fmt.Errorf("resource type %q is defined more than once", resourceType)
		}
		seenResourceTypes[resourceType] = struct{}{}

		resource, normalizedResource, err := compileAuthorizationResourceType(resourceType, rawResource)
		if err != nil {
			return nil, nil, err
		}
		model.ResourceTypes[resourceType] = resource
		normalized.ResourceTypes = append(normalized.ResourceTypes, normalizedResource)
	}
	slices.SortFunc(normalized.ResourceTypes, func(left, right *gestalt.AuthorizationModelResourceType) int {
		return strings.Compare(left.GetName(), right.GetName())
	})
	return model, normalized, nil
}

func compileAuthorizationResourceType(resourceType string, raw *gestalt.AuthorizationModelResourceType) (compiledResourceType, *gestalt.AuthorizationModelResourceType, error) {
	if raw == nil {
		return compiledResourceType{}, nil, fmt.Errorf("resource type %q is nil", resourceType)
	}
	if len(raw.GetRelations()) == 0 {
		return compiledResourceType{}, nil, fmt.Errorf("resource type %q must define at least one relation", resourceType)
	}

	compiled := compiledResourceType{
		Relations: make(map[string]compiledRelation, len(raw.GetRelations())),
		Actions:   make(map[string][]string, len(raw.GetActions())),
	}
	normalized := &gestalt.AuthorizationModelResourceType{Name: resourceType}
	seenRelations := make(map[string]struct{}, len(raw.GetRelations()))
	for _, rawRelation := range raw.GetRelations() {
		relation := strings.TrimSpace(rawRelation.GetName())
		if relation == "" {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q has an empty relation name", resourceType)
		}
		if _, exists := seenRelations[relation]; exists {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q relation %q is defined more than once", resourceType, relation)
		}
		seenRelations[relation] = struct{}{}

		subjectTypes, err := normalizeStringList(rawRelation.GetSubjectTypes(), "subject types")
		if err != nil {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q relation %q: %w", resourceType, relation, err)
		}
		allowed := make(map[string]struct{}, len(subjectTypes))
		for _, subjectType := range subjectTypes {
			allowed[subjectType] = struct{}{}
		}
		compiled.Relations[relation] = compiledRelation{SubjectTypes: allowed}
		normalized.Relations = append(normalized.Relations, &gestalt.AuthorizationModelRelation{
			Name:         relation,
			SubjectTypes: subjectTypes,
		})
	}
	slices.SortFunc(normalized.Relations, func(left, right *gestalt.AuthorizationModelRelation) int {
		return strings.Compare(left.GetName(), right.GetName())
	})

	seenActions := make(map[string]struct{}, len(raw.GetActions()))
	for _, rawAction := range raw.GetActions() {
		action := strings.TrimSpace(rawAction.GetName())
		if action == "" {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q has an empty action name", resourceType)
		}
		if _, exists := seenActions[action]; exists {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q action %q is defined more than once", resourceType, action)
		}
		seenActions[action] = struct{}{}

		relations, err := normalizeStringList(rawAction.GetRelations(), "relations")
		if err != nil {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q action %q: %w", resourceType, action, err)
		}
		for _, relation := range relations {
			if _, ok := compiled.Relations[relation]; !ok {
				return compiledResourceType{}, nil, fmt.Errorf("resource type %q action %q references unknown relation %q", resourceType, action, relation)
			}
		}
		compiled.Actions[action] = relations
		normalized.Actions = append(normalized.Actions, &gestalt.AuthorizationModelAction{
			Name:      action,
			Relations: relations,
		})
	}
	slices.SortFunc(normalized.Actions, func(left, right *gestalt.AuthorizationModelAction) int {
		return strings.Compare(left.GetName(), right.GetName())
	})

	return compiled, normalized, nil
}

func normalizeStringList(values []string, field string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must not be empty", field)
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			return nil, fmt.Errorf("%s must not contain empty values", field)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	slices.Sort(normalized)
	return normalized, nil
}

func modelIDForDefinition(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(model)
	if err != nil {
		return "", fmt.Errorf("marshal model: %w", err)
	}
	sum := sha256.Sum256(bytes)
	return "model-" + hex.EncodeToString(sum[:]), nil
}

func marshalStoredModel(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(model)
	if err != nil {
		return "", fmt.Errorf("marshal stored model: %w", err)
	}
	return string(bytes), nil
}

func unmarshalStoredModel(raw string) (*gestalt.AuthorizationModel, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("stored model is empty")
	}
	var model gestalt.AuthorizationModel
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(raw), &model); err != nil {
		return nil, fmt.Errorf("parse stored model: %w", err)
	}
	return &model, nil
}

func modelVersionString(model *gestalt.AuthorizationModel) string {
	if model == nil || model.GetVersion() == 0 {
		return "1"
	}
	return strconv.Itoa(int(model.GetVersion()))
}

func (m *compiledModel) actionRelations(resourceType, action string) []string {
	if m == nil {
		return nil
	}
	resource, ok := m.ResourceTypes[resourceType]
	if !ok {
		return nil
	}
	relations := resource.Actions[action]
	return append([]string(nil), relations...)
}

func (m *compiledModel) validateRelationship(subjectType, relation, resourceType string) error {
	if m == nil {
		return fmt.Errorf("model is required")
	}
	resource, ok := m.ResourceTypes[resourceType]
	if !ok {
		return fmt.Errorf("resource type %q is not defined by the model", resourceType)
	}
	compiledRelation, ok := resource.Relations[relation]
	if !ok {
		return fmt.Errorf("relation %q is not defined for resource type %q", relation, resourceType)
	}
	if _, ok := compiledRelation.SubjectTypes[subjectType]; !ok {
		return fmt.Errorf("subject type %q is not allowed for %s#%s", subjectType, resourceType, relation)
	}
	return nil
}

func (m *compiledModel) resourceType(resourceType string) (compiledResourceType, bool) {
	if m == nil {
		return compiledResourceType{}, false
	}
	resource, ok := m.ResourceTypes[resourceType]
	return resource, ok
}
