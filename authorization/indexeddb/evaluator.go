package indexeddb

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultPageSize = 100

func (p *Provider) Evaluate(ctx context.Context, req *gestalt.AccessEvaluationRequest) (*gestalt.AccessDecision, error) {
	if err := validateSubject(req.GetSubject()); err != nil {
		return nil, err
	}
	if err := validateResource(req.GetResource()); err != nil {
		return nil, err
	}
	action := strings.TrimSpace(req.GetAction().GetName())
	if action == "" {
		return nil, status.Error(codes.InvalidArgument, "action name is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}
	requiredRelations := model.compiled.actionRelations(req.GetResource().GetType(), action)
	allowed := false
	for _, relation := range requiredRelations {
		if err := model.compiled.validateRelationship(req.GetSubject().GetType(), relation, req.GetResource().GetType()); err != nil {
			continue
		}
		allowed, err = st.relationshipExists(ctx, req.GetSubject().GetType(), req.GetSubject().GetId(), relation, req.GetResource().GetType(), req.GetResource().GetId())
		if err != nil {
			return nil, err
		}
		if allowed {
			break
		}
	}
	return &gestalt.AccessDecision{
		Allowed: allowed,
		ModelId: model.ref.GetId(),
	}, nil
}

func (p *Provider) EvaluateMany(ctx context.Context, req *gestalt.AccessEvaluationsRequest) (*gestalt.AccessEvaluationsResponse, error) {
	decisions := make([]*gestalt.AccessDecision, 0, len(req.GetRequests()))
	for _, request := range req.GetRequests() {
		decision, err := p.Evaluate(ctx, request)
		if err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	return &gestalt.AccessEvaluationsResponse{Decisions: decisions}, nil
}

func (p *Provider) SearchResources(ctx context.Context, req *gestalt.ResourceSearchRequest) (*gestalt.ResourceSearchResponse, error) {
	if err := validateSubject(req.GetSubject()); err != nil {
		return nil, err
	}
	action := strings.TrimSpace(req.GetAction().GetName())
	if action == "" {
		return nil, status.Error(codes.InvalidArgument, "action name is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}
	candidates, err := st.candidateRelationships(ctx, req.GetSubject(), nil)
	if err != nil {
		return nil, err
	}

	resourcesByKey := make(map[string]*gestalt.AuthorizationResource)
	for _, candidate := range candidates {
		resource := candidate.GetResource()
		if req.GetResourceType() != "" && resource.GetType() != req.GetResourceType() {
			continue
		}
		if !relationshipAllowsAction(model.compiled, candidate, action) {
			continue
		}
		resourcesByKey[resourceKey(resource)] = cloneResource(resource)
	}

	resources := make([]*gestalt.AuthorizationResource, 0, len(resourcesByKey))
	for _, resource := range resourcesByKey {
		resources = append(resources, resource)
	}
	sortResources(resources)
	start, end, nextToken, err := paginate(len(resources), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &gestalt.ResourceSearchResponse{
		Resources:     append([]*gestalt.AuthorizationResource(nil), resources[start:end]...),
		NextPageToken: nextToken,
		ModelId:       model.ref.GetId(),
	}, nil
}

func (p *Provider) SearchSubjects(ctx context.Context, req *gestalt.SubjectSearchRequest) (*gestalt.SubjectSearchResponse, error) {
	if err := validateResource(req.GetResource()); err != nil {
		return nil, err
	}
	action := strings.TrimSpace(req.GetAction().GetName())
	if action == "" {
		return nil, status.Error(codes.InvalidArgument, "action name is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}
	candidates, err := st.candidateRelationships(ctx, nil, req.GetResource())
	if err != nil {
		return nil, err
	}

	subjectsByKey := make(map[string]*gestalt.AuthorizationSubject)
	for _, candidate := range candidates {
		if !relationshipAllowsAction(model.compiled, candidate, action) {
			continue
		}
		subject := candidate.GetSubject()
		if req.GetSubjectType() != "" && subject.GetType() != req.GetSubjectType() {
			continue
		}
		subjectsByKey[subjectKey(subject)] = cloneSubject(subject)
	}

	subjects := make([]*gestalt.AuthorizationSubject, 0, len(subjectsByKey))
	for _, subject := range subjectsByKey {
		subjects = append(subjects, subject)
	}
	sortSubjects(subjects)
	start, end, nextToken, err := paginate(len(subjects), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &gestalt.SubjectSearchResponse{
		Subjects:      append([]*gestalt.AuthorizationSubject(nil), subjects[start:end]...),
		NextPageToken: nextToken,
		ModelId:       model.ref.GetId(),
	}, nil
}

func (p *Provider) SearchActions(ctx context.Context, req *gestalt.ActionSearchRequest) (*gestalt.ActionSearchResponse, error) {
	if err := validateSubject(req.GetSubject()); err != nil {
		return nil, err
	}
	if err := validateResource(req.GetResource()); err != nil {
		return nil, err
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}
	candidates, err := st.candidateRelationships(ctx, req.GetSubject(), req.GetResource())
	if err != nil {
		return nil, err
	}
	resourceModel, ok := model.compiled.resourceType(req.GetResource().GetType())
	if !ok {
		return &gestalt.ActionSearchResponse{ModelId: model.ref.GetId()}, nil
	}

	relations := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if err := model.compiled.validateRelationship(candidate.GetSubject().GetType(), candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
			continue
		}
		relations[candidate.GetRelation()] = struct{}{}
	}

	actions := make([]*gestalt.AuthorizationAction, 0, len(resourceModel.Actions))
	for action, requiredRelations := range resourceModel.Actions {
		for _, relation := range requiredRelations {
			if _, ok := relations[relation]; ok {
				actions = append(actions, &gestalt.AuthorizationAction{Name: action})
				break
			}
		}
	}
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].GetName() < actions[j].GetName()
	})
	start, end, nextToken, err := paginate(len(actions), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &gestalt.ActionSearchResponse{
		Actions:       append([]*gestalt.AuthorizationAction(nil), actions[start:end]...),
		NextPageToken: nextToken,
		ModelId:       model.ref.GetId(),
	}, nil
}

func relationshipAllowsAction(model *compiledModel, relationship *gestalt.Relationship, action string) bool {
	if model == nil || relationship == nil {
		return false
	}
	if err := model.validateRelationship(relationship.GetSubject().GetType(), relationship.GetRelation(), relationship.GetResource().GetType()); err != nil {
		return false
	}
	for _, candidate := range model.actionRelations(relationship.GetResource().GetType(), action) {
		if candidate == relationship.GetRelation() {
			return true
		}
	}
	return false
}

func paginate(total int, pageSize int32, pageToken string) (int, int, string, error) {
	size := int(pageSize)
	if size <= 0 {
		size = defaultPageSize
	}
	start := 0
	if pageToken != "" {
		offset, err := strconv.Atoi(pageToken)
		if err != nil || offset < 0 {
			return 0, 0, "", status.Errorf(codes.InvalidArgument, "invalid page token %q", pageToken)
		}
		start = offset
	}
	if start > total {
		return 0, 0, "", status.Errorf(codes.InvalidArgument, "page token %q is out of range", pageToken)
	}
	end := start + size
	if end > total {
		end = total
	}
	nextToken := ""
	if end < total {
		nextToken = strconv.Itoa(end)
	}
	return start, end, nextToken, nil
}

func resourceKey(resource *gestalt.AuthorizationResource) string {
	return fmt.Sprintf("%s\x00%s", resource.GetType(), resource.GetId())
}

func subjectKey(subject *gestalt.AuthorizationSubject) string {
	return fmt.Sprintf("%s\x00%s", subject.GetType(), subject.GetId())
}

func cloneResource(resource *gestalt.AuthorizationResource) *gestalt.AuthorizationResource {
	if resource == nil {
		return nil
	}
	return &gestalt.AuthorizationResource{
		Type:       resource.GetType(),
		Id:         resource.GetId(),
		Properties: resource.GetProperties(),
	}
}

func cloneSubject(subject *gestalt.AuthorizationSubject) *gestalt.AuthorizationSubject {
	if subject == nil {
		return nil
	}
	return &gestalt.AuthorizationSubject{
		Type:       subject.GetType(),
		Id:         subject.GetId(),
		Properties: subject.GetProperties(),
	}
}

func sortResources(resources []*gestalt.AuthorizationResource) {
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].GetType() != resources[j].GetType() {
			return resources[i].GetType() < resources[j].GetType()
		}
		return resources[i].GetId() < resources[j].GetId()
	})
}

func sortSubjects(subjects []*gestalt.AuthorizationSubject) {
	sort.Slice(subjects, func(i, j int) bool {
		if subjects[i].GetType() != subjects[j].GetType() {
			return subjects[i].GetType() < subjects[j].GetType()
		}
		return subjects[i].GetId() < subjects[j].GetId()
	})
}

func sortRelationships(relationships []*gestalt.Relationship) {
	sort.Slice(relationships, func(i, j int) bool {
		left := relationships[i]
		right := relationships[j]
		if left.GetSubject().GetType() != right.GetSubject().GetType() {
			return left.GetSubject().GetType() < right.GetSubject().GetType()
		}
		if left.GetSubject().GetId() != right.GetSubject().GetId() {
			return left.GetSubject().GetId() < right.GetSubject().GetId()
		}
		if left.GetResource().GetType() != right.GetResource().GetType() {
			return left.GetResource().GetType() < right.GetResource().GetType()
		}
		if left.GetResource().GetId() != right.GetResource().GetId() {
			return left.GetResource().GetId() < right.GetResource().GetId()
		}
		return left.GetRelation() < right.GetRelation()
	})
}
