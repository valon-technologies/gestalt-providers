package indexeddb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type compiledModel struct {
	Version       string
	ResourceTypes map[string]compiledResourceType
}

type compiledResourceType struct {
	Relations map[string]compiledRelation
	Actions   map[string]compiledAction
}

type compiledRelation struct {
	SubjectTypes   map[string]struct{}
	AllowedTargets []compiledAllowedTarget
	Rewrite        *gestalt.AuthorizationModelRewrite
}

type compiledAction struct {
	Relations []string
	Rewrite   *gestalt.AuthorizationModelRewrite
}

type compiledAllowedTarget struct {
	Kind                   string
	SubjectType            string
	ResourceType           string
	SubjectSetResourceType string
	SubjectSetRelation     string
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
		Actions:   make(map[string]compiledAction, len(raw.GetActions())),
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

		subjectTypes, err := normalizeOptionalStringList(rawRelation.GetSubjectTypes(), "subject types")
		if err != nil {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q relation %q: %w", resourceType, relation, err)
		}
		allowed := make(map[string]struct{}, len(subjectTypes))
		for _, subjectType := range subjectTypes {
			allowed[subjectType] = struct{}{}
		}
		allowedTargets, normalizedTargets, err := compileAllowedTargets(subjectTypes, rawRelation.GetAllowedTargets())
		if err != nil {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q relation %q: %w", resourceType, relation, err)
		}
		if len(subjectTypes) == 0 && len(allowedTargets) == 0 {
			return compiledResourceType{}, nil, fmt.Errorf("resource type %q relation %q must allow at least one target", resourceType, relation)
		}
		compiled.Relations[relation] = compiledRelation{
			SubjectTypes:   allowed,
			AllowedTargets: allowedTargets,
			Rewrite:        cloneRewrite(rawRelation.GetRewrite()),
		}
		normalized.Relations = append(normalized.Relations, &gestalt.AuthorizationModelRelation{
			Name:           relation,
			SubjectTypes:   subjectTypes,
			AllowedTargets: normalizedTargets,
			Rewrite:        cloneRewrite(rawRelation.GetRewrite()),
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
		compiled.Actions[action] = compiledAction{
			Relations: relations,
			Rewrite:   cloneRewrite(rawAction.GetRewrite()),
		}
		normalized.Actions = append(normalized.Actions, &gestalt.AuthorizationModelAction{
			Name:      action,
			Relations: relations,
			Rewrite:   cloneRewrite(rawAction.GetRewrite()),
		})
	}
	slices.SortFunc(normalized.Actions, func(left, right *gestalt.AuthorizationModelAction) int {
		return strings.Compare(left.GetName(), right.GetName())
	})

	return compiled, normalized, nil
}

func compileAllowedTargets(subjectTypes []string, raw []*gestalt.AuthorizationModelAllowedTarget) ([]compiledAllowedTarget, []*gestalt.AuthorizationModelAllowedTarget, error) {
	out := make([]compiledAllowedTarget, 0, len(subjectTypes)+len(raw))
	normalized := make([]*gestalt.AuthorizationModelAllowedTarget, 0, len(raw))
	seen := map[string]struct{}{}
	seenNormalized := map[string]struct{}{}
	appendCompiled := func(target compiledAllowedTarget) {
		key := compiledAllowedTargetKey(target)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, target)
	}
	appendNormalized := func(target compiledAllowedTarget, protoTarget *gestalt.AuthorizationModelAllowedTarget) {
		key := compiledAllowedTargetKey(target)
		if _, ok := seenNormalized[key]; ok {
			return
		}
		seenNormalized[key] = struct{}{}
		normalized = append(normalized, protoTarget)
	}
	for _, subjectType := range subjectTypes {
		appendCompiled(compiledAllowedTarget{Kind: "subject", SubjectType: subjectType})
	}
	for _, target := range raw {
		switch {
		case target.GetSubjectType() != "":
			subjectType := strings.TrimSpace(target.GetSubjectType())
			if subjectType == "" {
				return nil, nil, fmt.Errorf("allowed target subject type must be non-empty")
			}
			compiled := compiledAllowedTarget{Kind: "subject", SubjectType: subjectType}
			appendCompiled(compiled)
			appendNormalized(compiled, gestalt.NewAuthorizationModelSubjectTypeTarget(subjectType))
		case target.GetResourceType() != "":
			resourceType := strings.TrimSpace(target.GetResourceType())
			if resourceType == "" {
				return nil, nil, fmt.Errorf("allowed target resource type must be non-empty")
			}
			compiled := compiledAllowedTarget{Kind: "resource", ResourceType: resourceType}
			appendCompiled(compiled)
			appendNormalized(compiled, gestalt.NewAuthorizationModelResourceTypeTarget(resourceType))
		case target.GetSubjectSet() != nil:
			subjectSet := target.GetSubjectSet()
			resourceType := strings.TrimSpace(subjectSet.GetResourceType())
			relation := strings.TrimSpace(subjectSet.GetRelation())
			if resourceType == "" || relation == "" {
				return nil, nil, fmt.Errorf("allowed target subject set must include resource type and relation")
			}
			compiled := compiledAllowedTarget{Kind: "subject_set", SubjectSetResourceType: resourceType, SubjectSetRelation: relation}
			appendCompiled(compiled)
			appendNormalized(compiled, gestalt.NewAuthorizationModelSubjectSetAllowedTarget(resourceType, relation))
		default:
			return nil, nil, fmt.Errorf("allowed target kind is required")
		}
	}
	return out, normalized, nil
}

func compiledAllowedTargetKey(target compiledAllowedTarget) string {
	return strings.Join([]string{target.Kind, target.SubjectType, target.ResourceType, target.SubjectSetResourceType, target.SubjectSetRelation}, "\x00")
}

func normalizeStringList(values []string, field string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must not be empty", field)
	}
	return normalizeOptionalStringList(values, field)
}

func normalizeOptionalStringList(values []string, field string) ([]string, error) {
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

func cloneRewrite(rewrite *gestalt.AuthorizationModelRewrite) *gestalt.AuthorizationModelRewrite {
	if rewrite == nil {
		return nil
	}
	bytes, err := gestalt.MarshalProtoDeterministic(rewrite)
	if err != nil {
		panic(fmt.Sprintf("clone authorization rewrite: %v", err))
	}
	var cloned gestalt.AuthorizationModelRewrite
	if err := gestalt.UnmarshalProto(bytes, &cloned); err != nil {
		panic(fmt.Sprintf("clone authorization rewrite: %v", err))
	}
	return &cloned
}

func modelIDForDefinition(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := gestalt.MarshalProtoDeterministic(model)
	if err != nil {
		return "", fmt.Errorf("marshal model: %w", err)
	}
	sum := sha256.Sum256(bytes)
	return "model-" + hex.EncodeToString(sum[:]), nil
}

func marshalStoredModel(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := gestalt.MarshalProtoJSON(model, gestalt.ProtoJSONMarshalOptions{UseProtoNames: true})
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
	if err := gestalt.UnmarshalProtoJSON([]byte(raw), &model, gestalt.ProtoJSONUnmarshalOptions{DiscardUnknown: false}); err != nil {
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
	actionDef := resource.Actions[action]
	return append([]string(nil), actionDef.Relations...)
}

func (m *compiledModel) actionRewrite(resourceType, action string) *gestalt.AuthorizationModelRewrite {
	if m == nil {
		return nil
	}
	resource, ok := m.ResourceTypes[resourceType]
	if !ok {
		return nil
	}
	return cloneRewrite(resource.Actions[action].Rewrite)
}

func (m *compiledModel) relationRewrite(resourceType, relation string) *gestalt.AuthorizationModelRewrite {
	if m == nil {
		return nil
	}
	resource, ok := m.ResourceTypes[resourceType]
	if !ok {
		return nil
	}
	return cloneRewrite(resource.Relations[relation].Rewrite)
}

func (m *compiledModel) validateRelationship(subjectType, relation, resourceType string) error {
	return m.validateRelationshipTarget(compiledRelationshipTarget{Kind: "subject", SubjectType: subjectType}, relation, resourceType)
}

type compiledRelationshipTarget struct {
	Kind                   string
	SubjectType            string
	SubjectID              string
	ResourceType           string
	ResourceID             string
	SubjectSetResourceType string
	SubjectSetResourceID   string
	SubjectSetRelation     string
}

func (m *compiledModel) validateRelationshipTarget(target compiledRelationshipTarget, relation, resourceType string) error {
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
	for _, allowed := range compiledRelation.AllowedTargets {
		if allowedRelationshipTarget(target, allowed) {
			return nil
		}
	}
	return fmt.Errorf("target %s is not allowed for %s#%s", relationshipTargetDescription(target), resourceType, relation)
}

func allowedRelationshipTarget(target compiledRelationshipTarget, allowed compiledAllowedTarget) bool {
	switch target.Kind {
	case "subject":
		return allowed.Kind == "subject" && allowed.SubjectType == target.SubjectType
	case "resource":
		return allowed.Kind == "resource" && allowed.ResourceType == target.ResourceType
	case "subject_set":
		return allowed.Kind == "subject_set" &&
			allowed.SubjectSetResourceType == target.SubjectSetResourceType &&
			allowed.SubjectSetRelation == target.SubjectSetRelation
	default:
		return false
	}
}

func relationshipTargetDescription(target compiledRelationshipTarget) string {
	switch target.Kind {
	case "subject":
		return "subject:" + target.SubjectType
	case "resource":
		return "resource:" + target.ResourceType
	case "subject_set":
		return "subject_set:" + target.SubjectSetResourceType + "#" + target.SubjectSetRelation
	default:
		return "unknown"
	}
}

func sameRelationshipTarget(left, right compiledRelationshipTarget) bool {
	return compiledRelationshipTargetKey(left) == compiledRelationshipTargetKey(right)
}

func compiledRelationshipTargetKey(target compiledRelationshipTarget) string {
	return strings.Join([]string{
		target.Kind,
		target.SubjectType,
		target.SubjectID,
		target.ResourceType,
		target.ResourceID,
		target.SubjectSetResourceType,
		target.SubjectSetResourceID,
		target.SubjectSetRelation,
	}, "\x00")
}

func (m *compiledModel) resourceType(resourceType string) (compiledResourceType, bool) {
	if m == nil {
		return compiledResourceType{}, false
	}
	resource, ok := m.ResourceTypes[resourceType]
	return resource, ok
}
