package indexeddb

import (
	"encoding/json"
	"time"
)

type storedEntity struct {
	Type       string         `json:"type"`
	Id         string         `json:"id"`
	Properties map[string]any `json:"properties,omitempty"`
}

type storedRelationship struct {
	Tuple       *storedRelationshipTuple `json:"tuple"`
	Properties  map[string]any           `json:"properties,omitempty"`
	SourceLayer jsonSourceLayer          `json:"source_layer"`
}

type storedRelationshipTuple struct {
	Target   *storedRelationshipTarget `json:"target"`
	Relation string                    `json:"relation"`
	Resource *storedEntity             `json:"resource"`
}

type storedRelationshipTarget struct {
	Subject    *storedEntity     `json:"subject,omitempty"`
	Resource   *storedEntity     `json:"resource,omitempty"`
	SubjectSet *storedSubjectSet `json:"subject_set,omitempty"`
}

type storedSubjectSet struct {
	Resource *storedEntity `json:"resource"`
	Relation string        `json:"relation"`
}

type storedAuthorizationModelResourceType struct {
	Name                string                              `json:"name"`
	DefaultAccessPolicy jsonDefaultAccessPolicy             `json:"default_access_policy"`
	Relations           []*storedAuthorizationModelRelation `json:"relations,omitempty"`
	Actions             []*storedAuthorizationModelAction   `json:"actions,omitempty"`
	SourceLayer         jsonSourceLayer                     `json:"source_layer"`
}

type storedAuthorizationModelRelation struct {
	Name           string                                   `json:"name"`
	AllowedTargets []*storedAuthorizationModelAllowedTarget `json:"allowed_targets,omitempty"`
}

type storedAuthorizationModelAction struct {
	Name      string   `json:"name"`
	Relations []string `json:"relations,omitempty"`
}

type storedAuthorizationModelAllowedTarget struct {
	SubjectType    string                `json:"subject_type,omitempty"`
	ResourceType   string                `json:"resource_type,omitempty"`
	SubjectSetType *storedSubjectSetType `json:"subject_set_type,omitempty"`
}

type storedSubjectSetType struct {
	ResourceType string `json:"resource_type"`
	Relation     string `json:"relation"`
}

type storedAuthorizationModelRef struct {
	Id        string    `json:"id"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

type jsonSourceLayer SourceLayer
type jsonDefaultAccessPolicy DefaultAccessPolicy

func (l jsonSourceLayer) MarshalJSON() ([]byte, error) {
	return jsonString(sourceLayerString(SourceLayer(l)))
}

func (l *jsonSourceLayer) UnmarshalJSON(data []byte) error {
	layer, err := unmarshalSourceLayer(data)
	if err != nil {
		return err
	}
	*l = jsonSourceLayer(layer)
	return nil
}

func (p jsonDefaultAccessPolicy) MarshalJSON() ([]byte, error) {
	return jsonString(defaultAccessPolicyString(DefaultAccessPolicy(p)))
}

func (p *jsonDefaultAccessPolicy) UnmarshalJSON(data []byte) error {
	policy, err := unmarshalDefaultAccessPolicy(data)
	if err != nil {
		return err
	}
	*p = jsonDefaultAccessPolicy(policy)
	return nil
}

func jsonString(value string) ([]byte, error) {
	return json.Marshal(value)
}

func unmarshalSourceLayer(data []byte) (SourceLayer, error) {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		return parseSourceLayer(text), nil
	}
	var value int32
	if err := json.Unmarshal(data, &value); err != nil {
		return SourceLayerUnspecified, err
	}
	return SourceLayer(value), nil
}

func unmarshalDefaultAccessPolicy(data []byte) (DefaultAccessPolicy, error) {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		return parseDefaultAccessPolicy(text), nil
	}
	var value int32
	if err := json.Unmarshal(data, &value); err != nil {
		return DefaultAccessPolicyInvalid, err
	}
	return DefaultAccessPolicy(value), nil
}

func relationshipToJSONValue(relationship *Relationship) (any, error) {
	return jsonValue(relationshipToStored(relationship))
}

func relationshipFromJSONValue(value any) (*Relationship, error) {
	var stored storedRelationship
	if err := decodeJSONValue(value, &stored); err != nil {
		return nil, err
	}
	return relationshipFromStored(&stored), nil
}

func relationshipTupleToJSONValue(tuple *RelationshipTuple) (any, error) {
	return jsonValue(relationshipTupleToStored(tuple))
}

func authorizationModelResourceTypesToJSONValue(resourceTypes []*AuthorizationModelResourceType) (any, error) {
	return jsonValue(authorizationModelResourceTypesToStored(resourceTypes))
}

func authorizationModelResourceTypesFromJSONValue(value any) ([]*AuthorizationModelResourceType, error) {
	var stored []*storedAuthorizationModelResourceType
	if err := decodeJSONValue(value, &stored); err != nil {
		return nil, err
	}
	return authorizationModelResourceTypesFromStored(stored), nil
}

func authorizationModelRefToJSONValue(ref *AuthorizationModelRef) (any, error) {
	if ref == nil {
		return nil, nil
	}
	return jsonValue(&storedAuthorizationModelRef{
		Id:        ref.Id,
		Version:   ref.Version,
		CreatedAt: ref.CreatedAt,
	})
}

func authorizationModelRefFromJSONValue(value any) (*AuthorizationModelRef, error) {
	var stored storedAuthorizationModelRef
	if err := decodeJSONValue(value, &stored); err != nil {
		return nil, err
	}
	return &AuthorizationModelRef{
		Id:        stored.Id,
		Version:   stored.Version,
		CreatedAt: stored.CreatedAt,
	}, nil
}

func relationshipToStored(relationship *Relationship) *storedRelationship {
	if relationship == nil {
		return nil
	}
	return &storedRelationship{
		Tuple:       relationshipTupleToStored(relationship.Tuple),
		Properties:  cloneMap(relationship.Properties),
		SourceLayer: jsonSourceLayer(relationship.SourceLayer),
	}
}

func relationshipFromStored(stored *storedRelationship) *Relationship {
	if stored == nil {
		return nil
	}
	return &Relationship{
		Tuple:       relationshipTupleFromStored(stored.Tuple),
		Properties:  cloneMap(stored.Properties),
		SourceLayer: SourceLayer(stored.SourceLayer),
	}
}

func relationshipTupleToStored(tuple *RelationshipTuple) *storedRelationshipTuple {
	if tuple == nil {
		return nil
	}
	return &storedRelationshipTuple{
		Target:   relationshipTargetToStored(tuple.Target),
		Relation: tuple.Relation,
		Resource: entityToStored(tuple.Resource),
	}
}

func relationshipTupleFromStored(stored *storedRelationshipTuple) *RelationshipTuple {
	if stored == nil {
		return nil
	}
	return &RelationshipTuple{
		Target:   relationshipTargetFromStored(stored.Target),
		Relation: stored.Relation,
		Resource: entityFromStored(stored.Resource),
	}
}

func relationshipTargetToStored(target *RelationshipTarget) *storedRelationshipTarget {
	if target == nil {
		return nil
	}
	return &storedRelationshipTarget{
		Subject:    subjectToStored(target.Subject),
		Resource:   entityToStored(target.Resource),
		SubjectSet: subjectSetToStored(target.SubjectSet),
	}
}

func relationshipTargetFromStored(stored *storedRelationshipTarget) *RelationshipTarget {
	if stored == nil {
		return nil
	}
	return &RelationshipTarget{
		Subject:    subjectFromStored(stored.Subject),
		Resource:   entityFromStored(stored.Resource),
		SubjectSet: subjectSetFromStored(stored.SubjectSet),
	}
}

func subjectSetToStored(subjectSet *SubjectSet) *storedSubjectSet {
	if subjectSet == nil {
		return nil
	}
	return &storedSubjectSet{
		Resource: entityToStored(subjectSet.Resource),
		Relation: subjectSet.Relation,
	}
}

func subjectSetFromStored(stored *storedSubjectSet) *SubjectSet {
	if stored == nil {
		return nil
	}
	return &SubjectSet{
		Resource: entityFromStored(stored.Resource),
		Relation: stored.Relation,
	}
}

func entityToStored(entity *Resource) *storedEntity {
	if entity == nil {
		return nil
	}
	return &storedEntity{
		Type:       entity.Type,
		Id:         entity.Id,
		Properties: cloneMap(entity.Properties),
	}
}

func entityFromStored(stored *storedEntity) *Resource {
	if stored == nil {
		return nil
	}
	return &Resource{
		Type:       stored.Type,
		Id:         stored.Id,
		Properties: cloneMap(stored.Properties),
	}
}

func subjectToStored(subject *Subject) *storedEntity {
	if subject == nil {
		return nil
	}
	return &storedEntity{
		Type:       subject.Type,
		Id:         subject.Id,
		Properties: cloneMap(subject.Properties),
	}
}

func subjectFromStored(stored *storedEntity) *Subject {
	if stored == nil {
		return nil
	}
	return &Subject{
		Type:       stored.Type,
		Id:         stored.Id,
		Properties: cloneMap(stored.Properties),
	}
}

func authorizationModelResourceTypesToStored(resourceTypes []*AuthorizationModelResourceType) []*storedAuthorizationModelResourceType {
	if resourceTypes == nil {
		return nil
	}
	out := make([]*storedAuthorizationModelResourceType, 0, len(resourceTypes))
	for _, resourceType := range resourceTypes {
		if resourceType == nil {
			continue
		}
		out = append(out, &storedAuthorizationModelResourceType{
			Name:                resourceType.Name,
			DefaultAccessPolicy: jsonDefaultAccessPolicy(resourceType.DefaultAccessPolicy),
			Relations:           authorizationModelRelationsToStored(resourceType.Relations),
			Actions:             authorizationModelActionsToStored(resourceType.Actions),
			SourceLayer:         jsonSourceLayer(resourceType.SourceLayer),
		})
	}
	return out
}

func authorizationModelResourceTypesFromStored(stored []*storedAuthorizationModelResourceType) []*AuthorizationModelResourceType {
	if stored == nil {
		return nil
	}
	out := make([]*AuthorizationModelResourceType, 0, len(stored))
	for _, resourceType := range stored {
		if resourceType == nil {
			continue
		}
		out = append(out, &AuthorizationModelResourceType{
			Name:                resourceType.Name,
			DefaultAccessPolicy: DefaultAccessPolicy(resourceType.DefaultAccessPolicy),
			Relations:           authorizationModelRelationsFromStored(resourceType.Relations),
			Actions:             authorizationModelActionsFromStored(resourceType.Actions),
			SourceLayer:         SourceLayer(resourceType.SourceLayer),
		})
	}
	return out
}

func authorizationModelRelationsToStored(relations []*AuthorizationModelRelation) []*storedAuthorizationModelRelation {
	if relations == nil {
		return nil
	}
	out := make([]*storedAuthorizationModelRelation, 0, len(relations))
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		out = append(out, &storedAuthorizationModelRelation{
			Name:           relation.Name,
			AllowedTargets: authorizationModelAllowedTargetsToStored(relation.AllowedTargets),
		})
	}
	return out
}

func authorizationModelRelationsFromStored(stored []*storedAuthorizationModelRelation) []*AuthorizationModelRelation {
	if stored == nil {
		return nil
	}
	out := make([]*AuthorizationModelRelation, 0, len(stored))
	for _, relation := range stored {
		if relation == nil {
			continue
		}
		out = append(out, &AuthorizationModelRelation{
			Name:           relation.Name,
			AllowedTargets: authorizationModelAllowedTargetsFromStored(relation.AllowedTargets),
		})
	}
	return out
}

func authorizationModelActionsToStored(actions []*AuthorizationModelAction) []*storedAuthorizationModelAction {
	if actions == nil {
		return nil
	}
	out := make([]*storedAuthorizationModelAction, 0, len(actions))
	for _, action := range actions {
		if action == nil {
			continue
		}
		out = append(out, &storedAuthorizationModelAction{
			Name:      action.Name,
			Relations: append([]string(nil), action.Relations...),
		})
	}
	return out
}

func authorizationModelActionsFromStored(stored []*storedAuthorizationModelAction) []*AuthorizationModelAction {
	if stored == nil {
		return nil
	}
	out := make([]*AuthorizationModelAction, 0, len(stored))
	for _, action := range stored {
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

func authorizationModelAllowedTargetsToStored(targets []*AuthorizationModelAllowedTarget) []*storedAuthorizationModelAllowedTarget {
	if targets == nil {
		return nil
	}
	out := make([]*storedAuthorizationModelAllowedTarget, 0, len(targets))
	for _, target := range targets {
		if target == nil {
			continue
		}
		out = append(out, &storedAuthorizationModelAllowedTarget{
			SubjectType:    target.SubjectType,
			ResourceType:   target.ResourceType,
			SubjectSetType: subjectSetTypeToStored(target.SubjectSetType),
		})
	}
	return out
}

func authorizationModelAllowedTargetsFromStored(stored []*storedAuthorizationModelAllowedTarget) []*AuthorizationModelAllowedTarget {
	if stored == nil {
		return nil
	}
	out := make([]*AuthorizationModelAllowedTarget, 0, len(stored))
	for _, target := range stored {
		if target == nil {
			continue
		}
		out = append(out, &AuthorizationModelAllowedTarget{
			SubjectType:    target.SubjectType,
			ResourceType:   target.ResourceType,
			SubjectSetType: subjectSetTypeFromStored(target.SubjectSetType),
		})
	}
	return out
}

func subjectSetTypeToStored(subjectSetType *SubjectSetType) *storedSubjectSetType {
	if subjectSetType == nil {
		return nil
	}
	return &storedSubjectSetType{
		ResourceType: subjectSetType.ResourceType,
		Relation:     subjectSetType.Relation,
	}
}

func subjectSetTypeFromStored(stored *storedSubjectSetType) *SubjectSetType {
	if stored == nil {
		return nil
	}
	return &SubjectSetType{
		ResourceType: stored.ResourceType,
		Relation:     stored.Relation,
	}
}
