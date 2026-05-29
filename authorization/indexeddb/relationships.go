package indexeddb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const defaultRelationshipPageSize = 100

func relationshipToRecord(relationship *Relationship) (indexeddb.Record, error) {
	value, err := jsonValue(relationship)
	if err != nil {
		return nil, fmt.Errorf("encode relationship: %w", err)
	}
	return indexeddb.Record{
		"id":    relationshipID(relationship.Tuple),
		"value": value,
	}, nil
}

func relationshipFromRecord(record indexeddb.Record) (*Relationship, error) {
	var relationship Relationship
	if err := decodeJSONValue(record["value"], &relationship); err != nil {
		return nil, fmt.Errorf("decode relationship: %w", err)
	}
	if err := normalizeRelationship(&relationship); err != nil {
		return nil, err
	}
	return &relationship, nil
}

func normalizeRelationship(relationship *Relationship) error {
	if relationship == nil {
		return fmt.Errorf("relationship is required")
	}
	if relationship.Tuple == nil {
		return fmt.Errorf("tuple is required")
	}
	relationship.Tuple.Relation = strings.TrimSpace(relationship.Tuple.Relation)
	if relationship.Tuple.Relation == "" {
		return fmt.Errorf("relation is required")
	}
	if err := normalizeRelationshipTarget(relationship.Tuple.Target); err != nil {
		return err
	}
	return normalizeResource(relationship.Tuple.Resource, "resource")
}

func normalizeRelationshipTarget(target *RelationshipTarget) error {
	if target == nil {
		return fmt.Errorf("target is required")
	}
	targets := 0
	if target.Subject != nil {
		targets++
		if err := normalizeSubject(target.Subject); err != nil {
			return err
		}
	}
	if target.Resource != nil {
		targets++
		if err := normalizeResource(target.Resource, "target resource"); err != nil {
			return err
		}
	}
	if target.SubjectSet != nil {
		targets++
		if err := normalizeSubjectSet(target.SubjectSet); err != nil {
			return err
		}
	}
	if targets != 1 {
		return fmt.Errorf("target must contain exactly one kind")
	}
	return nil
}

func normalizeSubject(subject *Subject) error {
	subject.Type = strings.TrimSpace(subject.Type)
	subject.Id = strings.TrimSpace(subject.Id)
	if subject.Type == "" {
		return fmt.Errorf("subject type is required")
	}
	if subject.Id == "" {
		return fmt.Errorf("subject id is required")
	}
	return nil
}

func normalizeResource(resource *Resource, name string) error {
	if resource == nil {
		return fmt.Errorf("%s is required", name)
	}
	resource.Type = strings.TrimSpace(resource.Type)
	resource.Id = strings.TrimSpace(resource.Id)
	if resource.Type == "" {
		return fmt.Errorf("%s type is required", name)
	}
	if resource.Id == "" {
		return fmt.Errorf("%s id is required", name)
	}
	return nil
}

func normalizeSubjectSet(subjectSet *SubjectSet) error {
	if subjectSet == nil {
		return fmt.Errorf("subject set is required")
	}
	subjectSet.Relation = strings.TrimSpace(subjectSet.Relation)
	if subjectSet.Relation == "" {
		return fmt.Errorf("subject set relation is required")
	}
	return normalizeResource(subjectSet.Resource, "subject set resource")
}

func relationshipMatchesFilter(relationship *Relationship, filter *RelationshipFilter) bool {
	if filter == nil {
		return true
	}
	if filter.Target != nil && !relationshipTargetsEqual(relationship.Tuple.Target, filter.Target) {
		return false
	}
	if strings.TrimSpace(filter.Relation) != "" && relationship.Tuple.Relation != strings.TrimSpace(filter.Relation) {
		return false
	}
	if filter.Resource != nil && !resourcesEqual(relationship.Tuple.Resource, filter.Resource) {
		return false
	}
	if filter.TargetType != RelationshipTargetTypeUnspecified && relationshipTargetType(relationship.Tuple.Target) != filter.TargetType {
		return false
	}
	if strings.TrimSpace(filter.TargetEntityType) != "" && relationshipTargetEntityType(relationship.Tuple.Target) != strings.TrimSpace(filter.TargetEntityType) {
		return false
	}
	if strings.TrimSpace(filter.ResourceType) != "" && relationship.Tuple.Resource.Type != strings.TrimSpace(filter.ResourceType) {
		return false
	}
	if filter.SourceLayer != SourceLayerUnspecified && relationship.SourceLayer != filter.SourceLayer {
		return false
	}
	return true
}

func parseRelationshipPageToken(token string) (int, error) {
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

func (l SourceLayer) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

func (l *SourceLayer) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*l = parseSourceLayer(text)
		return nil
	}
	var value int32
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*l = SourceLayer(value)
	return nil
}

func (l SourceLayer) String() string {
	switch l {
	case SourceLayerStaticConfig:
		return "static_config"
	case SourceLayerRuntime:
		return "runtime"
	default:
		return "unspecified"
	}
}

func parseSourceLayer(value string) SourceLayer {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "static_config":
		return SourceLayerStaticConfig
	case "runtime":
		return SourceLayerRuntime
	default:
		return SourceLayerUnspecified
	}
}

func relationshipID(tuple *RelationshipTuple) string {
	data, err := json.Marshal(tuple)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(data)
	return "relationship/" + hex.EncodeToString(sum[:])
}

func relationshipTargetType(target *RelationshipTarget) RelationshipTargetType {
	switch {
	case target == nil:
		return RelationshipTargetTypeUnspecified
	case target.Subject != nil:
		return RelationshipTargetTypeSubject
	case target.Resource != nil:
		return RelationshipTargetTypeResource
	case target.SubjectSet != nil:
		return RelationshipTargetTypeSubjectSet
	default:
		return RelationshipTargetTypeUnspecified
	}
}

func relationshipTargetEntityType(target *RelationshipTarget) string {
	switch {
	case target == nil:
		return ""
	case target.Subject != nil:
		return target.Subject.Type
	case target.Resource != nil:
		return target.Resource.Type
	case target.SubjectSet != nil && target.SubjectSet.Resource != nil:
		return target.SubjectSet.Resource.Type
	default:
		return ""
	}
}

func relationshipTargetsEqual(a, b *RelationshipTarget) bool {
	switch {
	case a == nil || b == nil:
		return a == b
	case a.Subject != nil || b.Subject != nil:
		return subjectsEqual(a.Subject, b.Subject)
	case a.Resource != nil || b.Resource != nil:
		return resourcesEqual(a.Resource, b.Resource)
	case a.SubjectSet != nil || b.SubjectSet != nil:
		return subjectSetsEqual(a.SubjectSet, b.SubjectSet)
	default:
		return true
	}
}

func subjectsEqual(a, b *Subject) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Type == strings.TrimSpace(b.Type) && a.Id == strings.TrimSpace(b.Id)
}

func resourcesEqual(a, b *Resource) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Type == strings.TrimSpace(b.Type) && a.Id == strings.TrimSpace(b.Id)
}

func subjectSetsEqual(a, b *SubjectSet) bool {
	if a == nil || b == nil {
		return a == b
	}
	return resourcesEqual(a.Resource, b.Resource) && a.Relation == strings.TrimSpace(b.Relation)
}

func cloneRelationships(relationships []*Relationship) []*Relationship {
	if relationships == nil {
		return nil
	}
	out := make([]*Relationship, 0, len(relationships))
	for _, relationship := range relationships {
		if relationship == nil {
			continue
		}
		out = append(out, cloneRelationship(relationship))
	}
	return out
}

func cloneRelationship(relationship *Relationship) *Relationship {
	if relationship == nil {
		return nil
	}
	return &Relationship{
		Tuple:       cloneRelationshipTuple(relationship.Tuple),
		Properties:  cloneMap(relationship.Properties),
		SourceLayer: relationship.SourceLayer,
	}
}

func cloneRelationshipTuple(tuple *RelationshipTuple) *RelationshipTuple {
	if tuple == nil {
		return nil
	}
	return &RelationshipTuple{
		Target:   cloneRelationshipTarget(tuple.Target),
		Relation: tuple.Relation,
		Resource: cloneResource(tuple.Resource),
	}
}

func cloneRelationshipTarget(target *RelationshipTarget) *RelationshipTarget {
	if target == nil {
		return nil
	}
	return &RelationshipTarget{
		Subject:    cloneSubject(target.Subject),
		Resource:   cloneResource(target.Resource),
		SubjectSet: cloneSubjectSet(target.SubjectSet),
	}
}

func cloneSubject(subject *Subject) *Subject {
	if subject == nil {
		return nil
	}
	return &Subject{
		Type:       subject.Type,
		Id:         subject.Id,
		Properties: cloneMap(subject.Properties),
	}
}

func cloneResource(resource *Resource) *Resource {
	if resource == nil {
		return nil
	}
	return &Resource{
		Type:       resource.Type,
		Id:         resource.Id,
		Properties: cloneMap(resource.Properties),
	}
}

func cloneSubjectSet(subjectSet *SubjectSet) *SubjectSet {
	if subjectSet == nil {
		return nil
	}
	return &SubjectSet{
		Resource: cloneResource(subjectSet.Resource),
		Relation: subjectSet.Relation,
	}
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	value, err := jsonValue(in)
	if err != nil {
		panic(err)
	}
	out, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return out
}
