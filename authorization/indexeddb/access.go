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
	model         *AuthorizationModel
	relationships []*Relationship
}

func (p *Provider) loadAuthorizationSnapshot(ctx context.Context) (*authorizationSnapshot, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	keys := getStateKeys()

	ref, err := getActiveModelRef(ctx, db.ObjectStore(stores.state), keys.activeModel)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model ref: %v", err)
	}
	if ref == nil {
		return nil, status.Error(codes.NotFound, "active model is not set")
	}

	model, err := getModel(ctx, db.ObjectStore(stores.models), ref.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model %q: %v", ref.Id, err)
	}
	if model == nil {
		return nil, status.Errorf(codes.NotFound, "model %q not found", ref.Id)
	}

	relationships, err := getAllRelationships(ctx, db.ObjectStore(stores.relationships))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list relationships: %v", err)
	}

	return &authorizationSnapshot{
		model:         model,
		relationships: relationships,
	}, nil
}

func getAllRelationships(ctx context.Context, store indexeddb.ObjectStore) ([]*Relationship, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	relationships := make([]*Relationship, 0, len(records))
	for _, record := range records {
		relationship, err := relationshipFromRecord(record)
		if err != nil {
			return nil, err
		}
		relationships = append(relationships, relationship)
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
	resourceType := findModelResourceType(snapshot.model, resource.Type)
	if resourceType == nil {
		return &CheckAccessResponse{Allowed: false, ModelID: modelID}, nil
	}
	if resourceType.DefaultAccessPolicy == DefaultAccessPolicyAllow {
		return &CheckAccessResponse{Allowed: true, ModelID: modelID}, nil
	}
	modelAction := findModelAction(resourceType, action.Name)
	if modelAction == nil {
		return &CheckAccessResponse{Allowed: false, ModelID: modelID}, nil
	}

	allowedRelations := modelActionAllowedRelations(modelAction)
	if len(allowedRelations) == 0 {
		return &CheckAccessResponse{Allowed: false, ModelID: modelID}, nil
	}

	for _, relationship := range snapshot.relationships {
		if relationship == nil || relationship.Tuple == nil {
			continue
		}
		if !resourcesEqual(relationship.Tuple.Resource, resource) {
			continue
		}
		if _, ok := allowedRelations[relationship.Tuple.Relation]; !ok {
			continue
		}
		if relationshipTargetMatchesSubject(subject, relationship.Tuple.Target, snapshot.relationships, make(map[string]struct{})) {
			return &CheckAccessResponse{Allowed: true, ModelID: modelID}, nil
		}
	}

	return &CheckAccessResponse{Allowed: false, ModelID: modelID}, nil
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

func relationshipTargetMatchesSubject(subject *Subject, target *RelationshipTarget, relationships []*Relationship, visited map[string]struct{}) bool {
	if target == nil {
		return false
	}
	if target.Subject != nil {
		return subjectsEqual(target.Subject, subject)
	}
	if target.SubjectSet != nil {
		return subjectMatchesSubjectSet(subject, target.SubjectSet, relationships, visited)
	}
	return false
}

func subjectMatchesSubjectSet(subject *Subject, subjectSet *SubjectSet, relationships []*Relationship, visited map[string]struct{}) bool {
	if subject == nil || subjectSet == nil || subjectSet.Resource == nil {
		return false
	}
	key := subject.Type + "\x00" + subject.Id + "\x00" + subjectSet.Resource.Type + "\x00" + subjectSet.Resource.Id + "\x00" + subjectSet.Relation
	if _, ok := visited[key]; ok {
		return false
	}
	visited[key] = struct{}{}

	for _, relationship := range relationships {
		if relationship == nil || relationship.Tuple == nil {
			continue
		}
		if relationship.Tuple.Relation != subjectSet.Relation {
			continue
		}
		if !resourcesEqual(relationship.Tuple.Resource, subjectSet.Resource) {
			continue
		}
		if relationshipTargetMatchesSubject(subject, relationship.Tuple.Target, relationships, visited) {
			return true
		}
	}
	return false
}
