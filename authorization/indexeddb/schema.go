package indexeddb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
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

type modelDocument struct {
	Version       int                             `yaml:"version"`
	ResourceTypes map[string]resourceTypeDocument `yaml:"resource_types"`
}

type resourceTypeDocument struct {
	Relations map[string]any `yaml:"relations"`
	Actions   map[string]any `yaml:"actions"`
}

func parseModelSchema(schema string) (*compiledModel, error) {
	var doc modelDocument
	if err := yaml.Unmarshal([]byte(schema), &doc); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}

	version := doc.Version
	if version == 0 {
		version = 1
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported schema version %d", version)
	}
	if len(doc.ResourceTypes) == 0 {
		return nil, fmt.Errorf("schema must define at least one resource type")
	}

	model := &compiledModel{
		Version:       strconv.Itoa(version),
		ResourceTypes: make(map[string]compiledResourceType, len(doc.ResourceTypes)),
	}
	for rawResourceType, rawResource := range doc.ResourceTypes {
		resourceType := strings.TrimSpace(rawResourceType)
		if resourceType == "" {
			return nil, fmt.Errorf("resource type names must be non-empty")
		}
		resource, err := compileResourceType(resourceType, rawResource)
		if err != nil {
			return nil, err
		}
		model.ResourceTypes[resourceType] = resource
	}
	return model, nil
}

func compileResourceType(resourceType string, raw resourceTypeDocument) (compiledResourceType, error) {
	if len(raw.Relations) == 0 {
		return compiledResourceType{}, fmt.Errorf("resource type %q must define at least one relation", resourceType)
	}

	compiled := compiledResourceType{
		Relations: make(map[string]compiledRelation, len(raw.Relations)),
		Actions:   make(map[string][]string, len(raw.Actions)),
	}
	for rawRelation, rawValue := range raw.Relations {
		relation := strings.TrimSpace(rawRelation)
		if relation == "" {
			return compiledResourceType{}, fmt.Errorf("resource type %q has an empty relation name", resourceType)
		}
		subjectTypes, err := subjectTypesFromValue(rawValue)
		if err != nil {
			return compiledResourceType{}, fmt.Errorf("resource type %q relation %q: %w", resourceType, relation, err)
		}
		allowed := make(map[string]struct{}, len(subjectTypes))
		for _, subjectType := range subjectTypes {
			allowed[subjectType] = struct{}{}
		}
		compiled.Relations[relation] = compiledRelation{SubjectTypes: allowed}
	}

	for rawAction, rawValue := range raw.Actions {
		action := strings.TrimSpace(rawAction)
		if action == "" {
			return compiledResourceType{}, fmt.Errorf("resource type %q has an empty action name", resourceType)
		}
		relations, err := relationsFromValue(rawValue)
		if err != nil {
			return compiledResourceType{}, fmt.Errorf("resource type %q action %q: %w", resourceType, action, err)
		}
		for _, relation := range relations {
			if _, ok := compiled.Relations[relation]; !ok {
				return compiledResourceType{}, fmt.Errorf("resource type %q action %q references unknown relation %q", resourceType, action, relation)
			}
		}
		compiled.Actions[action] = relations
	}
	return compiled, nil
}

func subjectTypesFromValue(raw any) ([]string, error) {
	switch value := raw.(type) {
	case map[string]any:
		subjectTypes, ok := value["subject_types"]
		if !ok {
			return nil, fmt.Errorf(`expected "subject_types"`)
		}
		return normalizedStringList(subjectTypes)
	default:
		return normalizedStringList(raw)
	}
}

func relationsFromValue(raw any) ([]string, error) {
	switch value := raw.(type) {
	case map[string]any:
		relations, ok := value["relations"]
		if !ok {
			return nil, fmt.Errorf(`expected "relations"`)
		}
		return normalizedStringList(relations)
	default:
		return normalizedStringList(raw)
	}
}

func normalizedStringList(raw any) ([]string, error) {
	switch value := raw.(type) {
	case string:
		item := strings.TrimSpace(value)
		if item == "" {
			return nil, fmt.Errorf("values must be non-empty")
		}
		return []string{item}, nil
	case []string:
		items := make([]string, 0, len(value))
		seen := make(map[string]struct{}, len(value))
		for _, item := range value {
			trimmed := strings.TrimSpace(item)
			if trimmed == "" {
				return nil, fmt.Errorf("values must be non-empty")
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			items = append(items, trimmed)
		}
		sort.Strings(items)
		return items, nil
	case []any:
		items := make([]string, 0, len(value))
		seen := make(map[string]struct{}, len(value))
		for _, item := range value {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("values must be strings")
			}
			trimmed := strings.TrimSpace(str)
			if trimmed == "" {
				return nil, fmt.Errorf("values must be non-empty")
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			items = append(items, trimmed)
		}
		sort.Strings(items)
		return items, nil
	default:
		return nil, fmt.Errorf("expected a string or list of strings")
	}
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
