package indexeddb

import (
	"context"
	"fmt"
	"strings"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorizationSnapshot struct {
	model                   *AuthorizationModel
	relationshipsByResource map[resourceRelation][]*Relationship
}

type resourceRelation struct {
	resourceType string
	resourceID   string
	relation     string
}

func (p *Provider) loadAuthorizationSnapshot(ctx context.Context, checks ...*CheckAccessRequest) (*authorizationSnapshot, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	keys := getStateKeys()
	tx, err := db.Transaction(ctx, stores.all(), indexeddb.TransactionReadonly, indexeddb.TransactionOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start authorization snapshot: %v", err)
	}
	defer func() {
		_ = tx.Abort(ctx)
	}()

	ref, err := getActiveModelRef(ctx, tx.ObjectStore(stores.state), keys.activeModel)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model ref: %v", err)
	}
	if ref == nil {
		return nil, status.Error(codes.NotFound, "active model is not set")
	}

	model, err := getModel(ctx, tx.ObjectStore(stores.models), ref.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model %q: %v", ref.Id, err)
	}
	if model == nil {
		return nil, status.Errorf(codes.NotFound, "model %q not found", ref.Id)
	}

	relationships, err := loadRelationships(ctx, tx.ObjectStore(stores.relationships), relationshipRoots(model, checks))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list relationships: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit authorization snapshot: %v", err)
	}

	return &authorizationSnapshot{
		model:                   model,
		relationshipsByResource: relationships,
	}, nil
}

func (snapshot *authorizationSnapshot) relationshipsFor(resource *Resource, relation string) []*Relationship {
	if snapshot == nil || resource == nil {
		return nil
	}
	return snapshot.relationshipsByResource[resourceRelation{
		resourceType: resource.Type,
		resourceID:   resource.Id,
		relation:     relation,
	}]
}

func relationshipRoots(model *AuthorizationModel, checks []*CheckAccessRequest) []resourceRelation {
	var roots []resourceRelation
	for _, check := range checks {
		_, action, resource, err := normalizeCheckAccessRequest(check)
		if err != nil {
			continue
		}
		_, modelAction := modelActionFor(model, action, resource)
		if modelAction == nil {
			continue
		}
		for relation := range modelActionAllowedRelations(modelAction) {
			roots = append(roots, resourceRelation{resource.Type, resource.Id, relation})
		}
	}
	return roots
}

func loadRelationships(ctx context.Context, store indexeddb.TransactionObjectStore, pending []resourceRelation) (map[resourceRelation][]*Relationship, error) {
	relationships := make(map[resourceRelation][]*Relationship)
	seen := make(map[resourceRelation]struct{})
	index := store.Index("by_resource_relation_source")
	for len(pending) > 0 {
		queries := make([]any, 0, len(pending))
		for _, key := range pending {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			prefix := []any{key.resourceType, key.resourceID, key.relation}
			// Arrays sort after scalar IndexedDB keys, so this range includes every source layer.
			queries = append(queries, indexeddb.Bound(prefix, append(prefix, []any{}), false, false))
		}
		if len(queries) == 0 {
			break
		}
		records, err := index.GetAll(ctx, indexeddb.AnyOf(queries[0], queries[1:]...))
		if err != nil {
			return nil, err
		}
		pending = nil
		for _, record := range records {
			relationship, err := relationshipFromRecord(record)
			if err != nil {
				return nil, err
			}
			resource := relationship.Tuple.Resource
			key := resourceRelation{resource.Type, resource.Id, relationship.Tuple.Relation}
			relationships[key] = append(relationships[key], relationship)
			if subjectSet := relationship.Tuple.Target.SubjectSet; subjectSet != nil {
				pending = append(pending, resourceRelation{
					subjectSet.Resource.Type,
					subjectSet.Resource.Id,
					subjectSet.Relation,
				})
			}
		}
	}
	return relationships, nil
}

func evaluateAccess(snapshot *authorizationSnapshot, req *CheckAccessRequest) (*CheckAccessResponse, error) {
	if snapshot == nil || snapshot.model == nil {
		return nil, status.Error(codes.Internal, "authorization snapshot is required")
	}
	subject, action, resource, err := normalizeCheckAccessRequest(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	modelID := snapshot.model.Id
	if scope := bearerScopeFromSubject(subject); scope != "" {
		if !scopeAllowsAccess(scope, resource.Type, action.Name) {
			return &CheckAccessResponse{Allowed: false, ModelId: modelID}, nil
		}
	}

	resourceType, modelAction := modelActionFor(snapshot.model, action, resource)
	if resourceType == nil {
		return &CheckAccessResponse{Allowed: false, ModelId: modelID}, nil
	}
	if modelAction == nil {
		return &CheckAccessResponse{Allowed: false, ModelId: modelID}, nil
	}

	allowedRelations := modelActionAllowedRelations(modelAction)
	if len(allowedRelations) == 0 {
		return &CheckAccessResponse{Allowed: false, ModelId: modelID}, nil
	}

	matchedRelations := make(map[string]struct{})
	for relation := range allowedRelations {
		for _, relationship := range snapshot.relationshipsFor(resource, relation) {
			if relationshipTargetMatchesSubject(subject, relationship.Tuple.Target, snapshot, make(map[string]struct{})) {
				matchedRelations[relation] = struct{}{}
			}
		}
	}
	if len(matchedRelations) > 0 {
		return &CheckAccessResponse{
			Allowed:          true,
			ModelId:          modelID,
			MatchedRelations: orderedRelations(modelAction, matchedRelations),
		}, nil
	}
	if defaultRole := strings.TrimSpace(resourceType.DefaultRole); defaultRole != "" {
		if _, ok := allowedRelations[defaultRole]; ok {
			return &CheckAccessResponse{
				Allowed:          true,
				ModelId:          modelID,
				MatchedRelations: []string{defaultRole},
			}, nil
		}
	}

	return &CheckAccessResponse{Allowed: false, ModelId: modelID}, nil
}

func normalizeCheckAccessRequest(req *CheckAccessRequest) (*Subject, *Action, *Resource, error) {
	if req == nil {
		return nil, nil, nil, fmt.Errorf("request is required")
	}
	subject := cloneSubject(req.Subject)
	if subject == nil {
		return nil, nil, nil, fmt.Errorf("subject is required")
	}
	if err := normalizeSubject(subject); err != nil {
		return nil, nil, nil, err
	}

	action := &Action{}
	if req.Action != nil {
		action.Name = strings.TrimSpace(req.Action.Name)
		action.Properties = cloneMap(req.Action.Properties)
	}
	if action.Name == "" {
		return nil, nil, nil, fmt.Errorf("action name is required")
	}

	resource := cloneResource(req.Resource)
	if resource == nil {
		return nil, nil, nil, fmt.Errorf("resource is required")
	}
	if err := normalizeResource(resource, "resource"); err != nil {
		return nil, nil, nil, err
	}

	return subject, action, resource, nil
}

func findModelResourceType(model *AuthorizationModel, name string) *AuthorizationModelResourceType {
	name = strings.TrimSpace(name)
	for _, resourceType := range model.ResourceTypes {
		if resourceType != nil && strings.TrimSpace(resourceType.Name) == name {
			return resourceType
		}
	}
	return nil
}

const wildcardActionName = "*"

func modelActionFor(model *AuthorizationModel, action *Action, resource *Resource) (*AuthorizationModelResourceType, *AuthorizationModelAction) {
	resourceType := findModelResourceType(model, resource.Type)
	if resourceType == nil {
		return nil, nil
	}
	actionModel := findModelAction(resourceType, action.Name)
	if actionModel == nil {
		actionModel = findModelAction(resourceType, wildcardActionName)
	}
	return resourceType, actionModel
}

func findModelAction(resourceType *AuthorizationModelResourceType, name string) *AuthorizationModelAction {
	name = strings.TrimSpace(name)
	for _, action := range resourceType.Actions {
		if action != nil && strings.TrimSpace(action.Name) == name {
			return action
		}
	}
	return nil
}

func modelActionAllowedRelations(action *AuthorizationModelAction) map[string]struct{} {
	relations := make(map[string]struct{}, len(action.Relations))
	for _, relation := range action.Relations {
		relation = strings.TrimSpace(relation)
		if relation == "" {
			continue
		}
		relations[relation] = struct{}{}
	}
	return relations
}

func orderedRelations(action *AuthorizationModelAction, included map[string]struct{}) []string {
	relations := make([]string, 0, len(included))
	seen := make(map[string]struct{}, len(included))
	for _, relation := range action.Relations {
		relation = strings.TrimSpace(relation)
		if _, ok := included[relation]; !ok {
			continue
		}
		if _, ok := seen[relation]; ok {
			continue
		}
		seen[relation] = struct{}{}
		relations = append(relations, relation)
	}
	return relations
}

func relationshipTargetMatchesSubject(subject *Subject, target *RelationshipTarget, snapshot *authorizationSnapshot, visited map[string]struct{}) bool {
	if target == nil {
		return false
	}
	if target.Subject != nil {
		return subjectsEqual(target.Subject, subject)
	}
	if target.SubjectSet != nil {
		return subjectMatchesSubjectSet(subject, target.SubjectSet, snapshot, visited)
	}
	return false
}

func subjectMatchesSubjectSet(subject *Subject, subjectSet *SubjectSet, snapshot *authorizationSnapshot, visited map[string]struct{}) bool {
	if subject == nil || subjectSet == nil || subjectSet.Resource == nil {
		return false
	}
	key := subject.Type + "\x00" + subject.Id + "\x00" + subjectSet.Resource.Type + "\x00" + subjectSet.Resource.Id + "\x00" + subjectSet.Relation
	if _, ok := visited[key]; ok {
		return false
	}
	visited[key] = struct{}{}

	for _, relationship := range snapshot.relationshipsFor(subjectSet.Resource, subjectSet.Relation) {
		if relationshipTargetMatchesSubject(subject, relationship.Tuple.Target, snapshot, visited) {
			return true
		}
	}
	return false
}
