package indexeddb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	switch {
	case rewrite.GetThis() != nil:
		return gestalt.NewAuthorizationModelThisRewrite()
	case rewrite.GetComputedUserset() != nil:
		return gestalt.NewAuthorizationModelComputedUsersetRewrite(rewrite.GetComputedUserset().GetRelation())
	case rewrite.GetTupleToUserset() != nil:
		tuple := rewrite.GetTupleToUserset()
		return gestalt.NewAuthorizationModelTupleToUsersetRewrite(tuple.GetTuplesetRelation(), tuple.GetComputedRelation())
	case rewrite.GetUnion() != nil:
		children := make([]*gestalt.AuthorizationModelRewrite, 0, len(rewrite.GetUnion().GetChildren()))
		for _, child := range rewrite.GetUnion().GetChildren() {
			if cloned := cloneRewrite(child); cloned != nil {
				children = append(children, cloned)
			}
		}
		return gestalt.NewAuthorizationModelUnionRewrite(children...)
	default:
		return &gestalt.AuthorizationModelRewrite{}
	}
}

func modelIDForDefinition(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := json.Marshal(authorizationModelJSONFromNative(model))
	if err != nil {
		return "", fmt.Errorf("marshal model: %w", err)
	}
	sum := sha256.Sum256(bytes)
	return "model-" + hex.EncodeToString(sum[:]), nil
}

func marshalStoredModel(model *gestalt.AuthorizationModel) (string, error) {
	bytes, err := json.Marshal(authorizationModelJSONFromNative(model))
	if err != nil {
		return "", fmt.Errorf("marshal stored model: %w", err)
	}
	return string(bytes), nil
}

func unmarshalStoredModel(raw string) (*gestalt.AuthorizationModel, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("stored model is empty")
	}
	var model authorizationModelJSON
	if err := json.Unmarshal([]byte(raw), &model); err != nil {
		return nil, fmt.Errorf("parse stored model: %w", err)
	}
	return authorizationModelFromJSON(model), nil
}

type authorizationModelJSON struct {
	Version       int32                                `json:"version,omitempty"`
	ResourceTypes []authorizationModelResourceTypeJSON `json:"resource_types,omitempty"`
}

type authorizationModelResourceTypeJSON struct {
	Name      string                           `json:"name,omitempty"`
	Relations []authorizationModelRelationJSON `json:"relations,omitempty"`
	Actions   []authorizationModelActionJSON   `json:"actions,omitempty"`
}

type authorizationModelRelationJSON struct {
	Name           string                                `json:"name,omitempty"`
	SubjectTypes   []string                              `json:"subject_types,omitempty"`
	AllowedTargets []authorizationModelAllowedTargetJSON `json:"allowed_targets,omitempty"`
	Rewrite        *authorizationModelRewriteJSON        `json:"rewrite,omitempty"`
}

type authorizationModelActionJSON struct {
	Name      string                         `json:"name,omitempty"`
	Relations []string                       `json:"relations,omitempty"`
	Rewrite   *authorizationModelRewriteJSON `json:"rewrite,omitempty"`
}

type authorizationModelAllowedTargetJSON struct {
	SubjectType  string                                  `json:"subject_type,omitempty"`
	ResourceType string                                  `json:"resource_type,omitempty"`
	SubjectSet   *authorizationModelSubjectSetTargetJSON `json:"subject_set,omitempty"`
}

type authorizationModelSubjectSetTargetJSON struct {
	ResourceType string `json:"resource_type,omitempty"`
	Relation     string `json:"relation,omitempty"`
}

type authorizationModelRewriteJSON struct {
	This            *struct{}                              `json:"this,omitempty"`
	ComputedUserset *authorizationModelComputedUsersetJSON `json:"computed_userset,omitempty"`
	TupleToUserset  *authorizationModelTupleToUsersetJSON  `json:"tuple_to_userset,omitempty"`
	Union           *authorizationModelRewriteUnionJSON    `json:"union,omitempty"`
}

type authorizationModelComputedUsersetJSON struct {
	Relation string `json:"relation,omitempty"`
}

type authorizationModelTupleToUsersetJSON struct {
	TuplesetRelation string `json:"tupleset_relation,omitempty"`
	ComputedRelation string `json:"computed_relation,omitempty"`
}

type authorizationModelRewriteUnionJSON struct {
	Children []*authorizationModelRewriteJSON `json:"children,omitempty"`
}

func authorizationModelJSONFromNative(model *gestalt.AuthorizationModel) authorizationModelJSON {
	if model == nil {
		return authorizationModelJSON{}
	}
	out := authorizationModelJSON{
		Version:       model.GetVersion(),
		ResourceTypes: make([]authorizationModelResourceTypeJSON, 0, len(model.GetResourceTypes())),
	}
	for _, resourceType := range model.GetResourceTypes() {
		out.ResourceTypes = append(out.ResourceTypes, authorizationModelResourceTypeJSONFromNative(resourceType))
	}
	return out
}

func authorizationModelResourceTypeJSONFromNative(resourceType *gestalt.AuthorizationModelResourceType) authorizationModelResourceTypeJSON {
	if resourceType == nil {
		return authorizationModelResourceTypeJSON{}
	}
	out := authorizationModelResourceTypeJSON{
		Name:      resourceType.GetName(),
		Relations: make([]authorizationModelRelationJSON, 0, len(resourceType.GetRelations())),
		Actions:   make([]authorizationModelActionJSON, 0, len(resourceType.GetActions())),
	}
	for _, relation := range resourceType.GetRelations() {
		out.Relations = append(out.Relations, authorizationModelRelationJSONFromNative(relation))
	}
	for _, action := range resourceType.GetActions() {
		out.Actions = append(out.Actions, authorizationModelActionJSONFromNative(action))
	}
	return out
}

func authorizationModelRelationJSONFromNative(relation *gestalt.AuthorizationModelRelation) authorizationModelRelationJSON {
	if relation == nil {
		return authorizationModelRelationJSON{}
	}
	out := authorizationModelRelationJSON{
		Name:           relation.GetName(),
		SubjectTypes:   append([]string(nil), relation.GetSubjectTypes()...),
		AllowedTargets: make([]authorizationModelAllowedTargetJSON, 0, len(relation.GetAllowedTargets())),
		Rewrite:        authorizationModelRewriteJSONFromNative(relation.GetRewrite()),
	}
	for _, target := range relation.GetAllowedTargets() {
		out.AllowedTargets = append(out.AllowedTargets, authorizationModelAllowedTargetJSONFromNative(target))
	}
	return out
}

func authorizationModelActionJSONFromNative(action *gestalt.AuthorizationModelAction) authorizationModelActionJSON {
	if action == nil {
		return authorizationModelActionJSON{}
	}
	return authorizationModelActionJSON{
		Name:      action.GetName(),
		Relations: append([]string(nil), action.GetRelations()...),
		Rewrite:   authorizationModelRewriteJSONFromNative(action.GetRewrite()),
	}
}

func authorizationModelAllowedTargetJSONFromNative(target *gestalt.AuthorizationModelAllowedTarget) authorizationModelAllowedTargetJSON {
	if target == nil {
		return authorizationModelAllowedTargetJSON{}
	}
	switch {
	case target.GetSubjectType() != "":
		return authorizationModelAllowedTargetJSON{SubjectType: target.GetSubjectType()}
	case target.GetResourceType() != "":
		return authorizationModelAllowedTargetJSON{ResourceType: target.GetResourceType()}
	case target.GetSubjectSet() != nil:
		return authorizationModelAllowedTargetJSON{
			SubjectSet: &authorizationModelSubjectSetTargetJSON{
				ResourceType: target.GetSubjectSet().GetResourceType(),
				Relation:     target.GetSubjectSet().GetRelation(),
			},
		}
	default:
		return authorizationModelAllowedTargetJSON{}
	}
}

func authorizationModelRewriteJSONFromNative(rewrite *gestalt.AuthorizationModelRewrite) *authorizationModelRewriteJSON {
	if rewrite == nil {
		return nil
	}
	switch {
	case rewrite.GetThis() != nil:
		return &authorizationModelRewriteJSON{This: &struct{}{}}
	case rewrite.GetComputedUserset() != nil:
		return &authorizationModelRewriteJSON{
			ComputedUserset: &authorizationModelComputedUsersetJSON{Relation: rewrite.GetComputedUserset().GetRelation()},
		}
	case rewrite.GetTupleToUserset() != nil:
		tuple := rewrite.GetTupleToUserset()
		return &authorizationModelRewriteJSON{
			TupleToUserset: &authorizationModelTupleToUsersetJSON{
				TuplesetRelation: tuple.GetTuplesetRelation(),
				ComputedRelation: tuple.GetComputedRelation(),
			},
		}
	case rewrite.GetUnion() != nil:
		children := make([]*authorizationModelRewriteJSON, 0, len(rewrite.GetUnion().GetChildren()))
		for _, child := range rewrite.GetUnion().GetChildren() {
			if out := authorizationModelRewriteJSONFromNative(child); out != nil {
				children = append(children, out)
			}
		}
		return &authorizationModelRewriteJSON{Union: &authorizationModelRewriteUnionJSON{Children: children}}
	default:
		return &authorizationModelRewriteJSON{}
	}
}

func authorizationModelFromJSON(model authorizationModelJSON) *gestalt.AuthorizationModel {
	out := &gestalt.AuthorizationModel{
		Version:       model.Version,
		ResourceTypes: make([]*gestalt.AuthorizationModelResourceType, 0, len(model.ResourceTypes)),
	}
	for _, resourceType := range model.ResourceTypes {
		out.ResourceTypes = append(out.ResourceTypes, authorizationModelResourceTypeFromJSON(resourceType))
	}
	return out
}

func authorizationModelResourceTypeFromJSON(resourceType authorizationModelResourceTypeJSON) *gestalt.AuthorizationModelResourceType {
	out := &gestalt.AuthorizationModelResourceType{
		Name:      resourceType.Name,
		Relations: make([]*gestalt.AuthorizationModelRelation, 0, len(resourceType.Relations)),
		Actions:   make([]*gestalt.AuthorizationModelAction, 0, len(resourceType.Actions)),
	}
	for _, relation := range resourceType.Relations {
		out.Relations = append(out.Relations, authorizationModelRelationFromJSON(relation))
	}
	for _, action := range resourceType.Actions {
		out.Actions = append(out.Actions, authorizationModelActionFromJSON(action))
	}
	return out
}

func authorizationModelRelationFromJSON(relation authorizationModelRelationJSON) *gestalt.AuthorizationModelRelation {
	out := &gestalt.AuthorizationModelRelation{
		Name:           relation.Name,
		SubjectTypes:   append([]string(nil), relation.SubjectTypes...),
		AllowedTargets: make([]*gestalt.AuthorizationModelAllowedTarget, 0, len(relation.AllowedTargets)),
		Rewrite:        authorizationModelRewriteFromJSON(relation.Rewrite),
	}
	for _, target := range relation.AllowedTargets {
		out.AllowedTargets = append(out.AllowedTargets, authorizationModelAllowedTargetFromJSON(target))
	}
	return out
}

func authorizationModelActionFromJSON(action authorizationModelActionJSON) *gestalt.AuthorizationModelAction {
	return &gestalt.AuthorizationModelAction{
		Name:      action.Name,
		Relations: append([]string(nil), action.Relations...),
		Rewrite:   authorizationModelRewriteFromJSON(action.Rewrite),
	}
}

func authorizationModelAllowedTargetFromJSON(target authorizationModelAllowedTargetJSON) *gestalt.AuthorizationModelAllowedTarget {
	switch {
	case target.SubjectType != "":
		return gestalt.NewAuthorizationModelSubjectTypeTarget(target.SubjectType)
	case target.ResourceType != "":
		return gestalt.NewAuthorizationModelResourceTypeTarget(target.ResourceType)
	case target.SubjectSet != nil:
		return gestalt.NewAuthorizationModelSubjectSetAllowedTarget(target.SubjectSet.ResourceType, target.SubjectSet.Relation)
	default:
		return &gestalt.AuthorizationModelAllowedTarget{}
	}
}

func authorizationModelRewriteFromJSON(rewrite *authorizationModelRewriteJSON) *gestalt.AuthorizationModelRewrite {
	if rewrite == nil {
		return nil
	}
	switch {
	case rewrite.This != nil:
		return gestalt.NewAuthorizationModelThisRewrite()
	case rewrite.ComputedUserset != nil:
		return gestalt.NewAuthorizationModelComputedUsersetRewrite(rewrite.ComputedUserset.Relation)
	case rewrite.TupleToUserset != nil:
		return gestalt.NewAuthorizationModelTupleToUsersetRewrite(rewrite.TupleToUserset.TuplesetRelation, rewrite.TupleToUserset.ComputedRelation)
	case rewrite.Union != nil:
		children := make([]*gestalt.AuthorizationModelRewrite, 0, len(rewrite.Union.Children))
		for _, child := range rewrite.Union.Children {
			if out := authorizationModelRewriteFromJSON(child); out != nil {
				children = append(children, out)
			}
		}
		return gestalt.NewAuthorizationModelUnionRewrite(children...)
	default:
		return &gestalt.AuthorizationModelRewrite{}
	}
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
