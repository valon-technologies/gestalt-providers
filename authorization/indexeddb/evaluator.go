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

const (
	defaultPageSize       = 100
	maxEvaluationDepth    = 32
	defaultExpandMaxDepth = 10

	everyoneResourceType = "everyone"
	everyoneResourceID   = "global"
	memberRelation       = "member"
)

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
	evaluator := authorizationEvaluator{
		store:   st,
		model:   model.compiled,
		subject: req.GetSubject(),
	}
	allowed, err := evaluator.evaluateAction(ctx, action, req.GetResource())
	if err != nil {
		return nil, err
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

	resources := resourcesFromMap(resourcesByKey)
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

func (p *Provider) EffectiveSearchResources(ctx context.Context, req *gestalt.ResourceSearchRequest) (*gestalt.ResourceSearchResponse, error) {
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
	candidates, err := st.candidateRelationships(ctx, nil, nil)
	if err != nil {
		return nil, err
	}

	evaluator := authorizationEvaluator{
		store:   st,
		model:   model.compiled,
		subject: req.GetSubject(),
	}
	resourcesByKey := make(map[string]*gestalt.AuthorizationResource)
	for _, candidate := range candidates {
		resource := candidate.GetResource()
		if req.GetResourceType() != "" && resource.GetType() != req.GetResourceType() {
			continue
		}
		key := resourceKey(resource)
		if _, seen := resourcesByKey[key]; seen {
			continue
		}
		allowed, err := evaluator.evaluateAction(ctx, action, resource)
		if err != nil {
			return nil, err
		}
		if allowed {
			resourcesByKey[key] = cloneResource(resource)
		}
	}

	resources := resourcesFromMap(resourcesByKey)
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
		if subject == nil {
			continue
		}
		if req.GetSubjectType() != "" && subject.GetType() != req.GetSubjectType() {
			continue
		}
		subjectsByKey[subjectKey(subject)] = cloneSubject(subject)
	}

	subjects := subjectsFromMap(subjectsByKey)
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

func (p *Provider) EffectiveSearchSubjects(ctx context.Context, req *gestalt.EffectiveSubjectSearchRequest) (*gestalt.EffectiveSubjectSearchResponse, error) {
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

	collector := effectiveTargetCollector{store: st, model: model.compiled}
	targetsByKey := map[string]*gestalt.AuthorizationRelationshipTarget{}
	if err := collector.collectAction(ctx, req.GetResource(), action, targetsByKey, 0, map[string]struct{}{}); err != nil {
		return nil, err
	}

	targets := targetsFromMap(targetsByKey)
	start, end, nextToken, err := paginate(len(targets), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &gestalt.EffectiveSubjectSearchResponse{
		Targets:       append([]*gestalt.AuthorizationRelationshipTarget(nil), targets[start:end]...),
		NextPageToken: nextToken,
		ModelId:       model.ref.GetId(),
		Truncated:     collector.truncated,
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
	resourceModel, ok := model.compiled.resourceType(req.GetResource().GetType())
	if !ok {
		return &gestalt.ActionSearchResponse{ModelId: model.ref.GetId()}, nil
	}

	evaluator := authorizationEvaluator{
		store:   st,
		model:   model.compiled,
		subject: req.GetSubject(),
	}
	actions := make([]*gestalt.AuthorizationAction, 0, len(resourceModel.Actions))
	for action := range resourceModel.Actions {
		allowed, err := evaluator.evaluateAction(ctx, action, req.GetResource())
		if err != nil {
			return nil, err
		}
		if allowed {
			actions = append(actions, &gestalt.AuthorizationAction{Name: action})
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

func (p *Provider) Expand(ctx context.Context, req *gestalt.ExpandRequest) (*gestalt.ExpandResponse, error) {
	if err := validateResource(req.GetResource()); err != nil {
		return nil, err
	}
	relation := strings.TrimSpace(req.GetRelation())
	if relation == "" {
		return nil, status.Error(codes.InvalidArgument, "relation is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, req.GetModelId(), true)
	if err != nil {
		return nil, err
	}

	maxDepth := int(req.GetMaxDepth())
	if maxDepth <= 0 {
		maxDepth = defaultExpandMaxDepth
	}
	expander := relationshipExpander{
		store:    st,
		model:    model.compiled,
		maxDepth: maxDepth,
	}
	root := &gestalt.ExpandNode{
		Target:   gestalt.NewAuthorizationResourceTarget(cloneResource(req.GetResource())),
		Relation: relation,
	}
	if err := expander.expandRelation(ctx, root, req.GetResource(), relation, 0, map[string]struct{}{}); err != nil {
		return nil, err
	}
	return &gestalt.ExpandResponse{
		Root:            root,
		Truncated:       expander.truncated,
		CycleDetected:   expander.cycleDetected,
		MaxDepthReached: expander.maxDepthReached,
		ModelId:         model.ref.GetId(),
	}, nil
}

type authorizationEvaluator struct {
	store   *store
	model   *compiledModel
	subject *gestalt.AuthorizationSubject
}

func (e authorizationEvaluator) evaluateAction(ctx context.Context, action string, resource *gestalt.AuthorizationResource) (bool, error) {
	if rewrite := e.model.actionRewrite(resource.GetType(), action); rewrite != nil {
		return e.evaluateRewrite(ctx, rewrite, resource, "", 0, map[string]struct{}{})
	}
	for _, relation := range e.model.actionRelations(resource.GetType(), action) {
		allowed, err := e.evaluateRelation(ctx, relation, resource, 0, map[string]struct{}{})
		if err != nil || allowed {
			return allowed, err
		}
	}
	return false, nil
}

func (e authorizationEvaluator) evaluateRelation(ctx context.Context, relation string, resource *gestalt.AuthorizationResource, depth int, seen map[string]struct{}) (bool, error) {
	if depth > maxEvaluationDepth {
		return false, nil
	}
	resourceModel, ok := e.model.resourceType(resource.GetType())
	if !ok {
		return false, nil
	}
	if _, ok := resourceModel.Relations[relation]; !ok {
		return false, nil
	}
	if isEveryoneMembership(resource, relation) {
		return true, nil
	}
	key := evaluationKey(resource, relation)
	if _, ok := seen[key]; ok {
		return false, nil
	}
	seen[key] = struct{}{}
	defer delete(seen, key)

	if rewrite := e.model.relationRewrite(resource.GetType(), relation); rewrite != nil {
		return e.evaluateRewrite(ctx, rewrite, resource, relation, depth+1, seen)
	}
	return e.evaluateThis(ctx, relation, resource, depth+1, seen)
}

func (e authorizationEvaluator) evaluateRewrite(ctx context.Context, rewrite *gestalt.AuthorizationModelRewrite, resource *gestalt.AuthorizationResource, relation string, depth int, seen map[string]struct{}) (bool, error) {
	if rewrite == nil || rewrite.GetThis() != nil {
		return e.evaluateThis(ctx, relation, resource, depth+1, seen)
	}
	if computed := rewrite.GetComputedUserset(); computed != nil {
		relation := strings.TrimSpace(computed.GetRelation())
		if relation == "" {
			return false, nil
		}
		return e.evaluateRelation(ctx, relation, resource, depth+1, seen)
	}
	if tupleToUserset := rewrite.GetTupleToUserset(); tupleToUserset != nil {
		tuplesetRelation := strings.TrimSpace(tupleToUserset.GetTuplesetRelation())
		computedRelation := strings.TrimSpace(tupleToUserset.GetComputedRelation())
		if tuplesetRelation == "" || computedRelation == "" {
			return false, nil
		}
		candidates, err := e.store.candidateRelationships(ctx, nil, resource)
		if err != nil {
			return false, err
		}
		for _, candidate := range candidates {
			if candidate.GetRelation() != tuplesetRelation {
				continue
			}
			target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
			if err != nil || target.Kind != "resource" {
				continue
			}
			if err := e.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
				continue
			}
			allowed, err := e.evaluateRelation(ctx, computedRelation, resourceFromTarget(target), depth+1, seen)
			if err != nil || allowed {
				return allowed, err
			}
		}
		return false, nil
	}
	if union := rewrite.GetUnion(); union != nil {
		for _, child := range union.GetChildren() {
			allowed, err := e.evaluateRewrite(ctx, child, resource, relation, depth+1, seen)
			if err != nil || allowed {
				return allowed, err
			}
		}
	}
	return false, nil
}

func (e authorizationEvaluator) evaluateThis(ctx context.Context, relation string, resource *gestalt.AuthorizationResource, depth int, seen map[string]struct{}) (bool, error) {
	if relation == "" {
		return false, nil
	}
	candidates, err := e.store.candidateRelationships(ctx, nil, resource)
	if err != nil {
		return false, err
	}
	for _, candidate := range candidates {
		if candidate.GetRelation() != relation {
			continue
		}
		target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
		if err != nil {
			continue
		}
		if err := e.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
			continue
		}
		switch target.Kind {
		case "subject":
			if targetMatchesSubject(target, e.subject) {
				return true, nil
			}
		case "subject_set":
			allowed, err := e.evaluateRelation(ctx, target.SubjectSetRelation, subjectSetResourceFromTarget(target), depth+1, seen)
			if err != nil || allowed {
				return allowed, err
			}
		}
	}
	return false, nil
}

type effectiveTargetCollector struct {
	store     *store
	model     *compiledModel
	truncated bool
}

func (c *effectiveTargetCollector) collectAction(ctx context.Context, resource *gestalt.AuthorizationResource, action string, targets map[string]*gestalt.AuthorizationRelationshipTarget, depth int, seen map[string]struct{}) error {
	if rewrite := c.model.actionRewrite(resource.GetType(), action); rewrite != nil {
		return c.collectRewrite(ctx, resource, "", rewrite, targets, depth, seen)
	}
	for _, relation := range c.model.actionRelations(resource.GetType(), action) {
		if err := c.collectRelation(ctx, resource, relation, targets, depth, seen); err != nil {
			return err
		}
	}
	return nil
}

func (c *effectiveTargetCollector) collectRelation(ctx context.Context, resource *gestalt.AuthorizationResource, relation string, targets map[string]*gestalt.AuthorizationRelationshipTarget, depth int, seen map[string]struct{}) error {
	if depth > maxEvaluationDepth {
		c.truncated = true
		return nil
	}
	resourceModel, ok := c.model.resourceType(resource.GetType())
	if !ok {
		return nil
	}
	if _, ok := resourceModel.Relations[relation]; !ok {
		return nil
	}
	key := evaluationKey(resource, relation)
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}
	defer delete(seen, key)

	rewrite := c.model.relationRewrite(resource.GetType(), relation)
	if rewrite == nil {
		return c.collectThis(ctx, resource, relation, targets)
	}
	return c.collectRewrite(ctx, resource, relation, rewrite, targets, depth+1, seen)
}

func (c *effectiveTargetCollector) collectRewrite(ctx context.Context, resource *gestalt.AuthorizationResource, relation string, rewrite *gestalt.AuthorizationModelRewrite, targets map[string]*gestalt.AuthorizationRelationshipTarget, depth int, seen map[string]struct{}) error {
	if rewrite == nil || rewrite.GetThis() != nil {
		if relation == "" {
			return nil
		}
		return c.collectThis(ctx, resource, relation, targets)
	}
	if computed := rewrite.GetComputedUserset(); computed != nil {
		return c.collectRelation(ctx, resource, computed.GetRelation(), targets, depth+1, seen)
	}
	if tupleToUserset := rewrite.GetTupleToUserset(); tupleToUserset != nil {
		tuplesetRelation := strings.TrimSpace(tupleToUserset.GetTuplesetRelation())
		computedRelation := strings.TrimSpace(tupleToUserset.GetComputedRelation())
		if tuplesetRelation == "" || computedRelation == "" {
			return nil
		}
		candidates, err := c.store.candidateRelationships(ctx, nil, resource)
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			if candidate.GetRelation() != tuplesetRelation {
				continue
			}
			target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
			if err != nil || target.Kind != "resource" {
				continue
			}
			if err := c.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
				continue
			}
			if err := c.collectRelation(ctx, resourceFromTarget(target), computedRelation, targets, depth+1, seen); err != nil {
				return err
			}
		}
		return nil
	}
	if union := rewrite.GetUnion(); union != nil {
		for _, child := range union.GetChildren() {
			if err := c.collectRewrite(ctx, resource, relation, child, targets, depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *effectiveTargetCollector) collectThis(ctx context.Context, resource *gestalt.AuthorizationResource, relation string, targets map[string]*gestalt.AuthorizationRelationshipTarget) error {
	candidates, err := c.store.candidateRelationships(ctx, nil, resource)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.GetRelation() != relation {
			continue
		}
		target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
		if err != nil {
			continue
		}
		if target.Kind == "resource" {
			continue
		}
		if err := c.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
			continue
		}
		addTarget(targets, candidate.GetTarget())
	}
	return nil
}

type relationshipExpander struct {
	store           *store
	model           *compiledModel
	maxDepth        int
	truncated       bool
	cycleDetected   bool
	maxDepthReached bool
}

func (e *relationshipExpander) expandRelation(ctx context.Context, node *gestalt.ExpandNode, resource *gestalt.AuthorizationResource, relation string, depth int, seen map[string]struct{}) error {
	if depth >= e.maxDepth {
		e.truncated = true
		e.maxDepthReached = true
		return nil
	}
	resourceModel, ok := e.model.resourceType(resource.GetType())
	if !ok {
		return nil
	}
	if _, ok := resourceModel.Relations[relation]; !ok {
		return nil
	}
	key := evaluationKey(resource, relation)
	if _, ok := seen[key]; ok {
		e.cycleDetected = true
		return nil
	}
	seen[key] = struct{}{}
	defer delete(seen, key)

	rewrite := e.model.relationRewrite(resource.GetType(), relation)
	if rewrite == nil {
		return e.expandThis(ctx, node, resource, relation)
	}
	return e.expandRewrite(ctx, node, resource, relation, rewrite, depth+1, seen)
}

func (e *relationshipExpander) expandRewrite(ctx context.Context, node *gestalt.ExpandNode, resource *gestalt.AuthorizationResource, relation string, rewrite *gestalt.AuthorizationModelRewrite, depth int, seen map[string]struct{}) error {
	if rewrite == nil || rewrite.GetThis() != nil {
		return e.expandThis(ctx, node, resource, relation)
	}
	if computed := rewrite.GetComputedUserset(); computed != nil {
		child := &gestalt.ExpandNode{
			Target:   gestalt.NewAuthorizationResourceTarget(cloneResource(resource)),
			Relation: computed.GetRelation(),
		}
		node.Children = append(node.Children, child)
		return e.expandRelation(ctx, child, resource, computed.GetRelation(), depth, seen)
	}
	if tupleToUserset := rewrite.GetTupleToUserset(); tupleToUserset != nil {
		tuplesetRelation := strings.TrimSpace(tupleToUserset.GetTuplesetRelation())
		computedRelation := strings.TrimSpace(tupleToUserset.GetComputedRelation())
		if tuplesetRelation == "" || computedRelation == "" {
			return nil
		}
		candidates, err := e.store.candidateRelationships(ctx, nil, resource)
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			if candidate.GetRelation() != tuplesetRelation {
				continue
			}
			target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
			if err != nil || target.Kind != "resource" {
				continue
			}
			if err := e.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
				continue
			}
			child := &gestalt.ExpandNode{
				Target:   cloneRelationshipTarget(candidate.GetTarget()),
				Relation: tuplesetRelation,
			}
			node.Children = append(node.Children, child)
			grandchild := &gestalt.ExpandNode{
				Target:   gestalt.NewAuthorizationResourceTarget(resourceFromTarget(target)),
				Relation: computedRelation,
			}
			child.Children = append(child.Children, grandchild)
			if err := e.expandRelation(ctx, grandchild, resourceFromTarget(target), computedRelation, depth, seen); err != nil {
				return err
			}
		}
		return nil
	}
	if union := rewrite.GetUnion(); union != nil {
		for _, childRewrite := range union.GetChildren() {
			if err := e.expandRewrite(ctx, node, resource, relation, childRewrite, depth, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *relationshipExpander) expandThis(ctx context.Context, node *gestalt.ExpandNode, resource *gestalt.AuthorizationResource, relation string) error {
	candidates, err := e.store.candidateRelationships(ctx, nil, resource)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.GetRelation() != relation {
			continue
		}
		target, _, err := normalizedRelationshipTarget(candidate.GetTarget(), candidate.GetSubject())
		if err != nil {
			continue
		}
		if err := e.model.validateRelationshipTarget(target, candidate.GetRelation(), candidate.GetResource().GetType()); err != nil {
			continue
		}
		node.Children = append(node.Children, &gestalt.ExpandNode{
			Target:   cloneRelationshipTarget(candidate.GetTarget()),
			Relation: candidate.GetRelation(),
		})
	}
	sortExpandNodes(node.Children)
	return nil
}

func relationshipAllowsAction(model *compiledModel, relationship *gestalt.Relationship, action string) bool {
	if model == nil || relationship == nil {
		return false
	}
	target, _, err := normalizedRelationshipTarget(relationship.GetTarget(), relationship.GetSubject())
	if err != nil || target.Kind != "subject" {
		return false
	}
	if err := model.validateRelationshipTarget(target, relationship.GetRelation(), relationship.GetResource().GetType()); err != nil {
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

func evaluationKey(resource *gestalt.AuthorizationResource, relation string) string {
	return fmt.Sprintf("%s\x00%s\x00%s", resource.GetType(), resource.GetId(), relation)
}

func isEveryoneMembership(resource *gestalt.AuthorizationResource, relation string) bool {
	return resource.GetType() == everyoneResourceType && resource.GetId() == everyoneResourceID && relation == memberRelation
}

func targetMatchesSubject(target compiledRelationshipTarget, subject *gestalt.AuthorizationSubject) bool {
	return target.Kind == "subject" && subject != nil && target.SubjectType == subject.GetType() && target.SubjectID == subject.GetId()
}

func resourceFromTarget(target compiledRelationshipTarget) *gestalt.AuthorizationResource {
	return &gestalt.AuthorizationResource{Type: target.ResourceType, Id: target.ResourceID}
}

func subjectSetResourceFromTarget(target compiledRelationshipTarget) *gestalt.AuthorizationResource {
	return &gestalt.AuthorizationResource{Type: target.SubjectSetResourceType, Id: target.SubjectSetResourceID}
}

func addTarget(targets map[string]*gestalt.AuthorizationRelationshipTarget, target *gestalt.AuthorizationRelationshipTarget) {
	compiled, _, err := normalizedRelationshipTarget(target, nil)
	if err != nil {
		return
	}
	targets[compiledRelationshipTargetKey(compiled)] = cloneRelationshipTarget(target)
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

func cloneRelationshipTarget(target *gestalt.AuthorizationRelationshipTarget) *gestalt.AuthorizationRelationshipTarget {
	if target == nil {
		return nil
	}
	if subject := target.GetSubject(); subject != nil {
		return gestalt.NewAuthorizationSubjectTarget(cloneSubject(subject))
	}
	if resource := target.GetResource(); resource != nil {
		return gestalt.NewAuthorizationResourceTarget(cloneResource(resource))
	}
	if subjectSet := target.GetSubjectSet(); subjectSet != nil {
		return gestalt.NewAuthorizationSubjectSetTarget(cloneResource(subjectSet.GetResource()), subjectSet.GetRelation())
	}
	return nil
}

func resourcesFromMap(resourcesByKey map[string]*gestalt.AuthorizationResource) []*gestalt.AuthorizationResource {
	resources := make([]*gestalt.AuthorizationResource, 0, len(resourcesByKey))
	for _, resource := range resourcesByKey {
		resources = append(resources, resource)
	}
	sortResources(resources)
	return resources
}

func subjectsFromMap(subjectsByKey map[string]*gestalt.AuthorizationSubject) []*gestalt.AuthorizationSubject {
	subjects := make([]*gestalt.AuthorizationSubject, 0, len(subjectsByKey))
	for _, subject := range subjectsByKey {
		subjects = append(subjects, subject)
	}
	sortSubjects(subjects)
	return subjects
}

func targetsFromMap(targetsByKey map[string]*gestalt.AuthorizationRelationshipTarget) []*gestalt.AuthorizationRelationshipTarget {
	targets := make([]*gestalt.AuthorizationRelationshipTarget, 0, len(targetsByKey))
	for _, target := range targetsByKey {
		targets = append(targets, target)
	}
	sortTargets(targets)
	return targets
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

func sortTargets(targets []*gestalt.AuthorizationRelationshipTarget) {
	sort.Slice(targets, func(i, j int) bool {
		return relationshipTargetSortKey(targets[i]) < relationshipTargetSortKey(targets[j])
	})
}

func sortRelationships(relationships []*gestalt.Relationship) {
	sort.Slice(relationships, func(i, j int) bool {
		return relationshipSortKey(relationships[i]) < relationshipSortKey(relationships[j])
	})
}

func sortExpandNodes(nodes []*gestalt.ExpandNode) {
	sort.Slice(nodes, func(i, j int) bool {
		left := relationshipTargetSortKey(nodes[i].GetTarget()) + "\x00" + nodes[i].GetRelation()
		right := relationshipTargetSortKey(nodes[j].GetTarget()) + "\x00" + nodes[j].GetRelation()
		return left < right
	})
}

func relationshipSortKey(relationship *gestalt.Relationship) string {
	target, _, err := normalizedRelationshipTarget(relationship.GetTarget(), relationship.GetSubject())
	if err != nil {
		return ""
	}
	return compiledRelationshipTargetKey(target) + "\x00" + resourceKey(relationship.GetResource()) + "\x00" + relationship.GetRelation()
}

func relationshipTargetSortKey(target *gestalt.AuthorizationRelationshipTarget) string {
	compiled, _, err := normalizedRelationshipTarget(target, nil)
	if err != nil {
		return ""
	}
	return compiledRelationshipTargetKey(compiled)
}
