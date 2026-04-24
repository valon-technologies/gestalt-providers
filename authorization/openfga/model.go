package openfga

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	fga "github.com/openfga/go-sdk"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const openFGASchemaVersion = "1.1"

type storedModel struct {
	ref                  *gestalt.AuthorizationModelRef
	authorizationModelID string
	writtenAt            time.Time
	model                *gestalt.AuthorizationModel
	compiled             *compiledModel
	translated           *translatedModel
	digest               string
}

type translatedModel struct {
	typeDefinitions []fga.TypeDefinition
	resourceTypes   map[string]translatedResourceType
}

type translatedResourceType struct {
	actions map[string]translatedAction
}

type translatedAction struct {
	requiredRelations []string
}

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

func compileAndTranslateAuthorizationModel(raw *gestalt.AuthorizationModel) (*storedModel, *fga.WriteAuthorizationModelRequest, error) {
	compiled, normalized, err := compileAuthorizationModel(raw)
	if err != nil {
		return nil, nil, err
	}
	digest, err := modelDigest(normalized)
	if err != nil {
		return nil, nil, err
	}
	translated, request, err := translateAuthorizationModel(normalized, compiled)
	if err != nil {
		return nil, nil, err
	}
	return &storedModel{
		model:      normalized,
		compiled:   compiled,
		translated: translated,
		digest:     digest,
	}, request, nil
}

func storedModelFromOpenFGA(model *fga.AuthorizationModel) (*storedModel, error) {
	if model == nil {
		return nil, fmt.Errorf("authorization model is required")
	}
	authorizationModelID := strings.TrimSpace(model.GetId())
	if authorizationModelID == "" {
		return nil, fmt.Errorf("authorization model id is required")
	}
	parsedID, err := ulid.Parse(authorizationModelID)
	if err != nil {
		return nil, fmt.Errorf("parse authorization model id %q: %w", authorizationModelID, err)
	}
	writtenAt := timeFromULID(parsedID)
	raw, err := reverseTranslateAuthorizationModel(model)
	if err != nil {
		return nil, err
	}
	compiled, normalized, err := compileAuthorizationModel(raw)
	if err != nil {
		return nil, err
	}
	digest, err := modelDigest(normalized)
	if err != nil {
		return nil, err
	}
	translated, _, err := translateAuthorizationModel(normalized, compiled)
	if err != nil {
		return nil, err
	}
	ref, err := modelRefFromDigest(digest, modelVersionString(normalized), writtenAt)
	if err != nil {
		return nil, err
	}
	return &storedModel{
		ref:                  ref,
		authorizationModelID: authorizationModelID,
		writtenAt:            writtenAt,
		model:                normalized,
		compiled:             compiled,
		translated:           translated,
		digest:               digest,
	}, nil
}

func translateAuthorizationModel(model *gestalt.AuthorizationModel, compiled *compiledModel) (*translatedModel, *fga.WriteAuthorizationModelRequest, error) {
	if model == nil || compiled == nil {
		return nil, nil, fmt.Errorf("authorization model is required")
	}

	subjectTypes := make(map[string]struct{})
	for _, resource := range model.GetResourceTypes() {
		resourceName := resource.GetName()
		for _, relation := range resource.GetRelations() {
			for _, subjectType := range relation.GetSubjectTypes() {
				subjectTypes[subjectType] = struct{}{}
			}
		}
		if _, ok := subjectTypes[resourceName]; ok {
			delete(subjectTypes, resourceName)
		}
	}

	typeNames := make([]string, 0, len(compiled.ResourceTypes)+len(subjectTypes))
	for resourceType := range compiled.ResourceTypes {
		typeNames = append(typeNames, resourceType)
	}
	for subjectType := range subjectTypes {
		if _, ok := compiled.ResourceTypes[subjectType]; ok {
			continue
		}
		typeNames = append(typeNames, subjectType)
	}
	slices.Sort(typeNames)

	typeDefinitions := make([]fga.TypeDefinition, 0, len(typeNames))
	resourceTypes := make(map[string]translatedResourceType, len(compiled.ResourceTypes))
	for _, typeName := range typeNames {
		resource, ok := compiled.ResourceTypes[typeName]
		if !ok {
			typeDefinitions = append(typeDefinitions, *fga.NewTypeDefinition(typeName))
			continue
		}

		relations := make(map[string]fga.Userset, len(resource.Relations)+len(resource.Actions))
		relationMetadata := make(map[string]fga.RelationMetadata, len(resource.Relations))
		relationNames := make([]string, 0, len(resource.Relations))
		for relationName := range resource.Relations {
			relationNames = append(relationNames, relationName)
		}
		slices.Sort(relationNames)

		for _, relationName := range relationNames {
			relation := resource.Relations[relationName]
			directUserset := fga.NewUserset()
			directUserset.SetThis(map[string]interface{}{})
			relations[relationName] = *directUserset

			subjectTypeNames := make([]string, 0, len(relation.SubjectTypes))
			for subjectType := range relation.SubjectTypes {
				subjectTypeNames = append(subjectTypeNames, subjectType)
			}
			slices.Sort(subjectTypeNames)

			refs := make([]fga.RelationReference, 0, len(subjectTypeNames))
			for _, subjectType := range subjectTypeNames {
				refs = append(refs, *fga.NewRelationReference(subjectType))
			}
			meta := fga.NewRelationMetadata()
			meta.SetDirectlyRelatedUserTypes(refs)
			relationMetadata[relationName] = *meta
		}

		actionNames := make([]string, 0, len(resource.Actions))
		translatedResource := translatedResourceType{actions: make(map[string]translatedAction, len(resource.Actions))}
		for actionName := range resource.Actions {
			actionNames = append(actionNames, actionName)
		}
		slices.Sort(actionNames)
		for _, actionName := range actionNames {
			if _, exists := resource.Relations[actionName]; exists {
				return nil, nil, fmt.Errorf("resource type %q uses %q as both a relation and an action, which OpenFGA cannot represent", typeName, actionName)
			}
			requiredRelations := append([]string(nil), resource.Actions[actionName]...)
			userset, err := actionUserset(requiredRelations)
			if err != nil {
				return nil, nil, fmt.Errorf("translate action %s.%s: %w", typeName, actionName, err)
			}
			relations[actionName] = userset
			translatedResource.actions[actionName] = translatedAction{requiredRelations: requiredRelations}
		}

		typeDef := fga.NewTypeDefinition(typeName)
		typeDef.SetRelations(relations)
		if len(relationMetadata) > 0 {
			metadata := fga.NewMetadata()
			metadata.SetRelations(relationMetadata)
			typeDef.SetMetadata(*metadata)
		}
		typeDefinitions = append(typeDefinitions, *typeDef)
		resourceTypes[typeName] = translatedResource
	}

	request := fga.NewWriteAuthorizationModelRequest(typeDefinitions, openFGASchemaVersion)
	return &translatedModel{
		typeDefinitions: typeDefinitions,
		resourceTypes:   resourceTypes,
	}, request, nil
}

func reverseTranslateAuthorizationModel(model *fga.AuthorizationModel) (*gestalt.AuthorizationModel, error) {
	if model == nil {
		return nil, fmt.Errorf("authorization model is required")
	}

	out := &gestalt.AuthorizationModel{Version: 1}
	typeDefinitions := append([]fga.TypeDefinition(nil), model.GetTypeDefinitions()...)
	sort.Slice(typeDefinitions, func(i, j int) bool {
		return typeDefinitions[i].GetType() < typeDefinitions[j].GetType()
	})

	for _, typeDefinition := range typeDefinitions {
		relationsMap := typeDefinition.GetRelations()
		if len(relationsMap) == 0 {
			continue
		}

		relationNames := make([]string, 0, len(relationsMap))
		for relationName := range relationsMap {
			relationNames = append(relationNames, relationName)
		}
		slices.Sort(relationNames)

		directRelations := make(map[string][]string)
		for _, relationName := range relationNames {
			userset := relationsMap[relationName]
			if !userset.HasThis() {
				continue
			}
			meta, ok := relationMetadata(typeDefinition, relationName)
			if !ok {
				return nil, fmt.Errorf("resource type %q relation %q is missing directly related user types metadata", typeDefinition.GetType(), relationName)
			}
			subjectTypes := make([]string, 0, len(meta.GetDirectlyRelatedUserTypes()))
			for _, ref := range meta.GetDirectlyRelatedUserTypes() {
				if ref.HasRelation() || ref.HasWildcard() || ref.HasCondition() {
					return nil, fmt.Errorf("resource type %q relation %q uses unsupported subject reference metadata", typeDefinition.GetType(), relationName)
				}
				subjectTypes = append(subjectTypes, ref.GetType())
			}
			subjectTypes, err := normalizeStringList(subjectTypes, "subject types")
			if err != nil {
				return nil, fmt.Errorf("resource type %q relation %q: %w", typeDefinition.GetType(), relationName, err)
			}
			directRelations[relationName] = subjectTypes
		}
		if len(directRelations) == 0 {
			return nil, fmt.Errorf("resource type %q does not define any direct relations", typeDefinition.GetType())
		}

		resource := &gestalt.AuthorizationModelResourceType{Name: typeDefinition.GetType()}
		for _, relationName := range relationNames {
			userset := relationsMap[relationName]
			if userset.HasThis() {
				resource.Relations = append(resource.Relations, &gestalt.AuthorizationModelRelation{
					Name:         relationName,
					SubjectTypes: append([]string(nil), directRelations[relationName]...),
				})
				continue
			}

			requiredRelations, err := reverseTranslateAction(userset, directRelations)
			if err != nil {
				return nil, fmt.Errorf("resource type %q action %q: %w", typeDefinition.GetType(), relationName, err)
			}
			resource.Actions = append(resource.Actions, &gestalt.AuthorizationModelAction{
				Name:      relationName,
				Relations: requiredRelations,
			})
		}

		slices.SortFunc(resource.Relations, func(left, right *gestalt.AuthorizationModelRelation) int {
			return strings.Compare(left.GetName(), right.GetName())
		})
		slices.SortFunc(resource.Actions, func(left, right *gestalt.AuthorizationModelAction) int {
			return strings.Compare(left.GetName(), right.GetName())
		})
		out.ResourceTypes = append(out.ResourceTypes, resource)
	}

	return out, nil
}

func relationMetadata(typeDefinition fga.TypeDefinition, relationName string) (fga.RelationMetadata, bool) {
	metadata, ok := typeDefinition.GetMetadataOk()
	if !ok || metadata == nil {
		return fga.RelationMetadata{}, false
	}
	relations := metadata.GetRelations()
	relation, ok := relations[relationName]
	return relation, ok
}

func reverseTranslateAction(userset fga.Userset, directRelations map[string][]string) ([]string, error) {
	directRelationNames := make([]string, 0, len(directRelations))
	for relationName := range directRelations {
		directRelationNames = append(directRelationNames, relationName)
	}
	allowed := make(map[string]struct{}, len(directRelationNames))
	for _, relationName := range directRelationNames {
		allowed[relationName] = struct{}{}
	}

	switch {
	case userset.HasComputedUserset():
		relation, err := computedUsersetRelation(userset.GetComputedUserset(), allowed)
		if err != nil {
			return nil, err
		}
		return []string{relation}, nil
	case userset.HasUnion():
		union := userset.GetUnion()
		children := union.Child
		if len(children) == 0 {
			return nil, fmt.Errorf("union must contain at least one computed userset")
		}
		relations := make([]string, 0, len(children))
		for _, child := range children {
			if !child.HasComputedUserset() {
				return nil, fmt.Errorf("union contains unsupported child userset")
			}
			relation, err := computedUsersetRelation(child.GetComputedUserset(), allowed)
			if err != nil {
				return nil, err
			}
			relations = append(relations, relation)
		}
		return normalizeStringList(relations, "relations")
	default:
		return nil, fmt.Errorf("action uses an unsupported OpenFGA userset expression")
	}
}

func computedUsersetRelation(objectRelation fga.ObjectRelation, allowed map[string]struct{}) (string, error) {
	if objectRelation.HasObject() {
		return "", fmt.Errorf("computed userset with object references is unsupported")
	}
	relation := strings.TrimSpace(objectRelation.GetRelation())
	if relation == "" {
		return "", fmt.Errorf("computed userset relation is required")
	}
	if _, ok := allowed[relation]; !ok {
		return "", fmt.Errorf("computed userset references unknown direct relation %q", relation)
	}
	return relation, nil
}

func actionUserset(requiredRelations []string) (fga.Userset, error) {
	switch len(requiredRelations) {
	case 0:
		return fga.Userset{}, fmt.Errorf("action must reference at least one relation")
	case 1:
		out := fga.NewUserset()
		computed := fga.NewObjectRelation()
		computed.SetRelation(requiredRelations[0])
		out.SetComputedUserset(*computed)
		return *out, nil
	default:
		children := make([]fga.Userset, 0, len(requiredRelations))
		for _, relationName := range requiredRelations {
			child := fga.NewUserset()
			computed := fga.NewObjectRelation()
			computed.SetRelation(relationName)
			child.SetComputedUserset(*computed)
			children = append(children, *child)
		}
		out := fga.NewUserset()
		out.SetUnion(*fga.NewUsersets(children))
		return *out, nil
	}
}

func modelRefFromDigest(id, version string, createdAt time.Time) (*gestalt.AuthorizationModelRef, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("authorization model id is required")
	}
	return &gestalt.AuthorizationModelRef{
		Id:        id,
		Version:   version,
		CreatedAt: timestamppb.New(createdAt),
	}, nil
}

func modelDigest(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(model)
	if err != nil {
		return "", fmt.Errorf("marshal model: %w", err)
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:]), nil
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
	return append([]string(nil), resource.Actions[action]...)
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
