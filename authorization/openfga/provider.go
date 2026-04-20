package openfga

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	fga "github.com/openfga/go-sdk"
	fgaclient "github.com/openfga/go-sdk/client"
	sdkcredentials "github.com/openfga/go-sdk/credentials"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultPageSize = 100
)

type Provider struct {
	mu     sync.RWMutex
	cfg    config
	client *fgaclient.OpenFgaClient
}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("openfga authorization: %w", err)
	}
	client, err := newClient(cfg)
	if err != nil {
		return fmt.Errorf("openfga authorization: %w", err)
	}
	if _, err := client.GetStore(ctx).Execute(); err != nil {
		return openFGAError("get store", err)
	}

	p.mu.Lock()
	p.cfg = cfg
	p.client = client
	p.mu.Unlock()
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuthorization,
		Name:        "openfga",
		DisplayName: "OpenFGA Authorization",
		Description: "Authorization provider backed by an OpenFGA store.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	client, err := p.configuredClient()
	if err != nil {
		return err
	}
	_, err = client.GetStore(ctx).Execute()
	if err != nil {
		return openFGAError("get store", err)
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = nil
	return nil
}

func (p *Provider) configuredClient() (*fgaclient.OpenFgaClient, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "openfga authorization: provider is not configured")
	}
	return p.client, nil
}

func (p *Provider) GetMetadata(ctx context.Context) (*gestalt.AuthorizationMetadata, error) {
	model, err := p.resolveModel(ctx, "", false)
	if err != nil {
		return nil, err
	}
	activeModelID := ""
	if model != nil {
		activeModelID = model.ref.GetId()
	}
	return &gestalt.AuthorizationMetadata{
		Capabilities:  []string{"decision_plane", "relationship_control_plane", "model_control_plane"},
		ActiveModelId: activeModelID,
	}, nil
}

func (p *Provider) GetActiveModel(ctx context.Context) (*gestalt.GetActiveModelResponse, error) {
	model, err := p.resolveModel(ctx, "", false)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return &gestalt.GetActiveModelResponse{}, nil
	}
	return &gestalt.GetActiveModelResponse{Model: model.ref}, nil
}

func (p *Provider) ListModels(ctx context.Context, req *gestalt.ListModelsRequest) (*gestalt.ListModelsResponse, error) {
	models, err := p.listCompatibleModels(ctx)
	if err != nil {
		return nil, err
	}
	refs := make([]*gestalt.AuthorizationModelRef, 0, len(models))
	for _, model := range models {
		refs = append(refs, model.ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		left := refs[i].GetCreatedAt().AsTime()
		right := refs[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.After(right)
		}
		return refs[i].GetId() < refs[j].GetId()
	})
	start, end, nextToken, err := paginate(len(refs), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &gestalt.ListModelsResponse{
		Models:        append([]*gestalt.AuthorizationModelRef(nil), refs[start:end]...),
		NextPageToken: nextToken,
	}, nil
}

func (p *Provider) WriteModel(ctx context.Context, req *gestalt.WriteModelRequest) (*gestalt.AuthorizationModelRef, error) {
	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}

	model, translated, err := compileAndTranslateAuthorizationModel(req.GetModel())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid authorization model: %v", err)
	}
	if existing, err := p.findExistingModel(ctx, model.digest); err == nil && existing != nil {
		latestDigest, err := p.latestModelDigest(ctx)
		if err != nil {
			return nil, err
		}
		if latestDigest == model.digest {
			return existing.ref, nil
		}
	} else if err != nil {
		return nil, err
	}

	resp, err := client.WriteAuthorizationModel(ctx).Body(*translated).Execute()
	if err != nil {
		return nil, openFGAError("write authorization model", err)
	}

	if existing, err := p.findExistingModel(ctx, model.digest); err == nil && existing != nil {
		return existing.ref, nil
	} else if err != nil {
		return nil, err
	}

	parsedID, err := ulid.Parse(resp.GetAuthorizationModelId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parse written authorization model id: %v", err)
	}
	ref, err := modelRefFromDigest(model.digest, modelVersionString(model.model), timeFromULID(parsedID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build authorization model ref: %v", err)
	}
	return ref, nil
}

func (p *Provider) ReadRelationships(ctx context.Context, req *gestalt.ReadRelationshipsRequest) (*gestalt.ReadRelationshipsResponse, error) {
	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}
	if req.GetSubject() != nil {
		if err := validateSubject(req.GetSubject()); err != nil {
			return nil, err
		}
	}
	if req.GetResource() != nil {
		if err := validateResource(req.GetResource()); err != nil {
			return nil, err
		}
	}

	model, err := p.resolveModel(ctx, req.GetModelId(), false)
	if err != nil {
		return nil, err
	}

	body := fgaclient.ClientReadRequest{}
	if req.GetSubject() != nil {
		user := subjectString(req.GetSubject())
		body.User = &user
	}
	if relation := strings.TrimSpace(req.GetRelation()); relation != "" {
		body.Relation = &relation
	}
	if req.GetResource() != nil {
		object := resourceString(req.GetResource())
		body.Object = &object
	}

	options := fgaclient.ClientReadOptions{}
	if req.GetPageSize() > 0 {
		options.PageSize = fga.PtrInt32(req.GetPageSize())
	}
	if token := strings.TrimSpace(req.GetPageToken()); token != "" {
		options.ContinuationToken = &token
	}
	options.Consistency = higherConsistencyPtr()
	resp, err := client.Read(ctx).Body(body).Options(options).Execute()
	if err != nil {
		return nil, openFGAError("read relationships", err)
	}

	relationships := make([]*gestalt.Relationship, 0, len(resp.GetTuples()))
	for _, tuple := range resp.GetTuples() {
		relationship, err := relationshipFromTuple(tuple)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode relationship tuple: %v", err)
		}
		relationships = append(relationships, relationship)
	}

	modelID := ""
	if model != nil {
		modelID = model.ref.GetId()
	}
	return &gestalt.ReadRelationshipsResponse{
		Relationships: relationships,
		NextPageToken: strings.TrimSpace(resp.GetContinuationToken()),
		ModelId:       modelID,
	}, nil
}

func (p *Provider) WriteRelationships(ctx context.Context, req *gestalt.WriteRelationshipsRequest) error {
	client, err := p.configuredClient()
	if err != nil {
		return err
	}

	model, err := p.resolveModel(ctx, req.GetModelId(), false)
	if err != nil {
		return err
	}
	if model == nil && len(req.GetWrites()) > 0 {
		return status.Error(codes.FailedPrecondition, "openfga authorization: no active model is configured")
	}

	writes := make([]fgaclient.ClientTupleKey, 0, len(req.GetWrites()))
	for _, relationship := range req.GetWrites() {
		if relationship == nil {
			return status.Error(codes.InvalidArgument, "relationship is required")
		}
		if err := validateSubject(relationship.GetSubject()); err != nil {
			return err
		}
		if err := validateResource(relationship.GetResource()); err != nil {
			return err
		}
		relation := strings.TrimSpace(relationship.GetRelation())
		if relation == "" {
			return status.Error(codes.InvalidArgument, "relationship relation is required")
		}
		if err := validateUnsupportedProperties(relationship); err != nil {
			return err
		}
		if model != nil {
			if err := model.compiled.validateRelationship(relationship.GetSubject().GetType(), relation, relationship.GetResource().GetType()); err != nil {
				return status.Errorf(codes.InvalidArgument, "relationship rejected by model %q: %v", model.ref.GetId(), err)
			}
		}
		writes = append(writes, fgaclient.ClientTupleKey{
			User:     subjectString(relationship.GetSubject()),
			Relation: relation,
			Object:   resourceString(relationship.GetResource()),
		})
	}

	deletes := make([]fgaclient.ClientTupleKeyWithoutCondition, 0, len(req.GetDeletes()))
	for _, key := range req.GetDeletes() {
		if key == nil {
			return status.Error(codes.InvalidArgument, "relationship key is required")
		}
		if err := validateSubject(key.GetSubject()); err != nil {
			return err
		}
		if err := validateResource(key.GetResource()); err != nil {
			return err
		}
		relation := strings.TrimSpace(key.GetRelation())
		if relation == "" {
			return status.Error(codes.InvalidArgument, "relationship relation is required")
		}
		deletes = append(deletes, fgaclient.ClientTupleKeyWithoutCondition{
			User:     subjectString(key.GetSubject()),
			Relation: relation,
			Object:   resourceString(key.GetResource()),
		})
	}

	options := fgaclient.ClientWriteOptions{
		Conflict: fgaclient.ClientWriteConflictOptions{
			OnDuplicateWrites: fgaclient.CLIENT_WRITE_REQUEST_ON_DUPLICATE_WRITES_IGNORE,
			OnMissingDeletes:  fgaclient.CLIENT_WRITE_REQUEST_ON_MISSING_DELETES_IGNORE,
		},
	}
	if model != nil {
		options.AuthorizationModelId = &model.authorizationModelID
	}
	_, err = client.Write(ctx).Body(fgaclient.ClientWriteRequest{
		Writes:  writes,
		Deletes: deletes,
	}).Options(options).Execute()
	if err != nil {
		return openFGAError("write relationships", err)
	}
	return nil
}

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

	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}
	if !subjectTypeAllowedForAction(model.compiled, req.GetSubject().GetType(), req.GetResource().GetType(), action) {
		return &gestalt.AccessDecision{Allowed: false, ModelId: model.ref.GetId()}, nil
	}

	resp, err := client.Check(ctx).Body(fgaclient.ClientCheckRequest{
		User:     subjectString(req.GetSubject()),
		Relation: action,
		Object:   resourceString(req.GetResource()),
	}).Options(fgaclient.ClientCheckOptions{
		AuthorizationModelId: &model.authorizationModelID,
		Consistency:          higherConsistencyPtr(),
	}).Execute()
	if err != nil {
		return nil, openFGAError("check access", err)
	}
	return &gestalt.AccessDecision{
		Allowed: resp.GetAllowed(),
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

	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}

	candidateTypes := searchableResourceTypes(model.compiled, req.GetSubject().GetType(), strings.TrimSpace(req.GetResourceType()), action)
	resourcesByKey := make(map[string]*gestalt.AuthorizationResource)
	for _, resourceType := range candidateTypes {
		resp, err := client.ListObjects(ctx).Body(fgaclient.ClientListObjectsRequest{
			User:     subjectString(req.GetSubject()),
			Relation: action,
			Type:     resourceType,
		}).Options(fgaclient.ClientListObjectsOptions{
			AuthorizationModelId: &model.authorizationModelID,
			Consistency:          higherConsistencyPtr(),
		}).Execute()
		if err != nil {
			return nil, openFGAError("list objects", err)
		}
		for _, object := range resp.GetObjects() {
			resource, err := resourceFromObjectString(object)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "decode object %q: %v", object, err)
			}
			resourcesByKey[resourceKey(resource)] = resource
		}
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

	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}
	model, err := p.resolveModel(ctx, "", true)
	if err != nil {
		return nil, err
	}

	subjectTypes := searchableSubjectTypes(model.compiled, req.GetResource().GetType(), strings.TrimSpace(req.GetSubjectType()), action)
	subjectsByKey := make(map[string]*gestalt.AuthorizationSubject)
	object := fga.FgaObject{Type: req.GetResource().GetType(), Id: req.GetResource().GetId()}
	for _, subjectType := range subjectTypes {
		resp, err := client.ListUsers(ctx).Body(fgaclient.ClientListUsersRequest{
			Object:      object,
			Relation:    action,
			UserFilters: []fga.UserTypeFilter{{Type: subjectType}},
		}).Options(fgaclient.ClientListUsersOptions{
			AuthorizationModelId: &model.authorizationModelID,
			Consistency:          higherConsistencyPtr(),
		}).Execute()
		if err != nil {
			return nil, openFGAError("list users", err)
		}
		for _, user := range resp.GetUsers() {
			if !user.HasObject() {
				continue
			}
			objectUser := user.GetObject()
			subject := &gestalt.AuthorizationSubject{
				Type: objectUser.Type,
				Id:   objectUser.Id,
			}
			subjectsByKey[subjectKey(subject)] = subject
		}
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

	client, err := p.configuredClient()
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

	candidateActions := make([]string, 0, len(resourceModel.Actions))
	for actionName := range resourceModel.Actions {
		if subjectTypeAllowedForAction(model.compiled, req.GetSubject().GetType(), req.GetResource().GetType(), actionName) {
			candidateActions = append(candidateActions, actionName)
		}
	}
	slicesSortStrings(candidateActions)
	if len(candidateActions) == 0 {
		return &gestalt.ActionSearchResponse{ModelId: model.ref.GetId()}, nil
	}

	resp, err := client.ListRelations(ctx).Body(fgaclient.ClientListRelationsRequest{
		User:      subjectString(req.GetSubject()),
		Object:    resourceString(req.GetResource()),
		Relations: candidateActions,
	}).Options(fgaclient.ClientListRelationsOptions{
		AuthorizationModelId: &model.authorizationModelID,
		Consistency:          higherConsistencyPtr(),
	}).Execute()
	if err != nil {
		return nil, openFGAError("list relations", err)
	}

	actions := make([]*gestalt.AuthorizationAction, 0, len(resp.Relations))
	for _, actionName := range resp.Relations {
		actions = append(actions, &gestalt.AuthorizationAction{Name: actionName})
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

func (p *Provider) resolveModel(ctx context.Context, requestedModelID string, requireActive bool) (*storedModel, error) {
	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}

	modelID := strings.TrimSpace(requestedModelID)
	if modelID != "" {
		if existing, err := p.findExistingModel(ctx, modelID); err == nil && existing != nil {
			return existing, nil
		} else if err != nil {
			return nil, err
		}
		if _, err := ulid.Parse(modelID); err == nil {
			model, err := p.readAuthorizationModelByID(ctx, modelID)
			if err != nil {
				return nil, err
			}
			if model != nil {
				return model, nil
			}
		}
		return nil, status.Errorf(codes.NotFound, "authorization model %q was not found", modelID)
	}

	resp, err := client.ReadLatestAuthorizationModel(ctx).Execute()
	if err != nil {
		return nil, openFGAError("read latest authorization model", err)
	}
	model, ok := resp.GetAuthorizationModelOk()
	if !ok || model == nil {
		if requireActive {
			return nil, status.Error(codes.FailedPrecondition, "openfga authorization: no active model is configured")
		}
		return nil, nil
	}
	stored, err := storedModelFromOpenFGA(model)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "openfga authorization: latest authorization model is not compatible with Gestalt typed models: %v", err)
	}
	return stored, nil
}

func (p *Provider) findExistingModel(ctx context.Context, digest string) (*storedModel, error) {
	models, err := p.listCompatibleModels(ctx)
	if err != nil {
		return nil, err
	}
	for _, model := range models {
		if model.digest == digest {
			return model, nil
		}
	}
	return nil, nil
}

func (p *Provider) listCompatibleModels(ctx context.Context) ([]*storedModel, error) {
	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}

	byDigest := make(map[string]*storedModel)
	token := ""
	for {
		options := fgaclient.ClientReadAuthorizationModelsOptions{PageSize: fga.PtrInt32(100)}
		if token != "" {
			options.ContinuationToken = &token
		}
		resp, err := client.ReadAuthorizationModels(ctx).Options(options).Execute()
		if err != nil {
			return nil, openFGAError("read authorization models", err)
		}
		for _, candidate := range resp.GetAuthorizationModels() {
			stored, err := storedModelFromOpenFGA(&candidate)
			if err != nil {
				continue
			}
			if existing, ok := byDigest[stored.digest]; ok {
				mergeStoredModels(existing, stored)
				continue
			}
			byDigest[stored.digest] = cloneStoredModel(stored)
		}
		token = strings.TrimSpace(resp.GetContinuationToken())
		if token == "" {
			break
		}
	}

	models := make([]*storedModel, 0, len(byDigest))
	for _, model := range byDigest {
		models = append(models, model)
	}
	return models, nil
}

func (p *Provider) latestModelDigest(ctx context.Context) (string, error) {
	client, err := p.configuredClient()
	if err != nil {
		return "", err
	}
	resp, err := client.ReadLatestAuthorizationModel(ctx).Execute()
	if err != nil {
		return "", openFGAError("read latest authorization model", err)
	}
	model, ok := resp.GetAuthorizationModelOk()
	if !ok || model == nil {
		return "", nil
	}
	stored, err := storedModelFromOpenFGA(model)
	if err != nil {
		return "", nil
	}
	return stored.digest, nil
}

func (p *Provider) readAuthorizationModelByID(ctx context.Context, modelID string) (*storedModel, error) {
	client, err := p.configuredClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.ReadAuthorizationModel(ctx).Options(fgaclient.ClientReadAuthorizationModelOptions{
		AuthorizationModelId: &modelID,
	}).Body(fgaclient.ClientReadAuthorizationModelRequest{}).Execute()
	if err != nil {
		if isOpenFGANotFound(err) {
			return nil, nil
		}
		return nil, openFGAError("read authorization model", err)
	}
	model, ok := resp.GetAuthorizationModelOk()
	if !ok || model == nil {
		return nil, nil
	}
	stored, err := storedModelFromOpenFGA(model)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "openfga authorization: authorization model %q is not compatible with Gestalt typed models: %v", modelID, err)
	}
	return stored, nil
}

func newClient(cfg config) (*fgaclient.OpenFgaClient, error) {
	clientCfg := &fgaclient.ClientConfiguration{
		ApiUrl:  cfg.APIURL,
		StoreId: cfg.StoreID,
	}
	if cfg.APIToken != "" {
		creds, err := sdkcredentials.NewCredentials(sdkcredentials.Credentials{
			Method: sdkcredentials.CredentialsMethodApiToken,
			Config: &sdkcredentials.Config{
				ApiToken: cfg.APIToken,
			},
		})
		if err != nil {
			return nil, err
		}
		clientCfg.Credentials = creds
	} else if cfg.ClientID != "" {
		creds, err := sdkcredentials.NewCredentials(sdkcredentials.Credentials{
			Method: sdkcredentials.CredentialsMethodClientCredentials,
			Config: &sdkcredentials.Config{
				ClientCredentialsClientId:       cfg.ClientID,
				ClientCredentialsClientSecret:   cfg.ClientSecret,
				ClientCredentialsApiTokenIssuer: cfg.APITokenIssuer,
				ClientCredentialsApiAudience:    cfg.APIAudience,
				ClientCredentialsScopes:         cfg.Scopes,
			},
		})
		if err != nil {
			return nil, err
		}
		clientCfg.Credentials = creds
	}
	return fgaclient.NewSdkClient(clientCfg)
}

func searchableResourceTypes(model *compiledModel, subjectType, requestedType, action string) []string {
	if strings.TrimSpace(requestedType) != "" {
		if !subjectTypeAllowedForAction(model, subjectType, requestedType, action) {
			return nil
		}
		return []string{requestedType}
	}

	resourceTypes := make([]string, 0, len(model.ResourceTypes))
	for resourceType := range model.ResourceTypes {
		if subjectTypeAllowedForAction(model, subjectType, resourceType, action) {
			resourceTypes = append(resourceTypes, resourceType)
		}
	}
	slicesSortStrings(resourceTypes)
	return resourceTypes
}

func searchableSubjectTypes(model *compiledModel, resourceType, requestedType, action string) []string {
	resource, ok := model.resourceType(resourceType)
	if !ok {
		return nil
	}
	requiredRelations := resource.Actions[action]
	if len(requiredRelations) == 0 {
		return nil
	}

	if requestedType != "" {
		if subjectTypeAllowedForAction(model, requestedType, resourceType, action) {
			return []string{requestedType}
		}
		return nil
	}

	subjectTypes := make(map[string]struct{})
	for _, relationName := range requiredRelations {
		relation := resource.Relations[relationName]
		for subjectType := range relation.SubjectTypes {
			subjectTypes[subjectType] = struct{}{}
		}
	}
	out := make([]string, 0, len(subjectTypes))
	for subjectType := range subjectTypes {
		out = append(out, subjectType)
	}
	slicesSortStrings(out)
	return out
}

func subjectTypeAllowedForAction(model *compiledModel, subjectType, resourceType, action string) bool {
	for _, relationName := range model.actionRelations(resourceType, action) {
		if err := model.validateRelationship(subjectType, relationName, resourceType); err == nil {
			return true
		}
	}
	return false
}

func validateSubject(subject *gestalt.AuthorizationSubject) error {
	if subject == nil {
		return status.Error(codes.InvalidArgument, "subject is required")
	}
	if strings.TrimSpace(subject.GetType()) == "" {
		return status.Error(codes.InvalidArgument, "subject type is required")
	}
	if strings.TrimSpace(subject.GetId()) == "" {
		return status.Error(codes.InvalidArgument, "subject id is required")
	}
	return nil
}

func validateResource(resource *gestalt.AuthorizationResource) error {
	if resource == nil {
		return status.Error(codes.InvalidArgument, "resource is required")
	}
	if strings.TrimSpace(resource.GetType()) == "" {
		return status.Error(codes.InvalidArgument, "resource type is required")
	}
	if strings.TrimSpace(resource.GetId()) == "" {
		return status.Error(codes.InvalidArgument, "resource id is required")
	}
	return nil
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

func subjectString(subject *gestalt.AuthorizationSubject) string {
	return subject.GetType() + ":" + subject.GetId()
}

func resourceString(resource *gestalt.AuthorizationResource) string {
	return resource.GetType() + ":" + resource.GetId()
}

func relationshipFromTuple(tuple fga.Tuple) (*gestalt.Relationship, error) {
	key := tuple.GetKey()
	subject, err := subjectFromUserString(key.GetUser())
	if err != nil {
		return nil, err
	}
	resource, err := resourceFromObjectString(key.GetObject())
	if err != nil {
		return nil, err
	}
	return &gestalt.Relationship{
		Subject:  subject,
		Relation: key.GetRelation(),
		Resource: resource,
	}, nil
}

func subjectFromUserString(user string) (*gestalt.AuthorizationSubject, error) {
	typeName, id, ok := strings.Cut(user, ":")
	if !ok || strings.TrimSpace(typeName) == "" || strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("invalid OpenFGA user %q", user)
	}
	return &gestalt.AuthorizationSubject{Type: typeName, Id: id}, nil
}

func resourceFromObjectString(object string) (*gestalt.AuthorizationResource, error) {
	typeName, id, ok := strings.Cut(object, ":")
	if !ok || strings.TrimSpace(typeName) == "" || strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("invalid OpenFGA object %q", object)
	}
	return &gestalt.AuthorizationResource{Type: typeName, Id: id}, nil
}

func timeFromULID(id ulid.ULID) time.Time {
	return time.UnixMilli(int64(id.Time())).UTC()
}

func isOpenFGANotFound(err error) bool {
	var notFound fga.FgaApiNotFoundError
	return errors.As(err, &notFound)
}

func openFGAError(op string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}

	var required fgaclient.FgaRequiredParamError
	if errors.As(err, &required) {
		return status.Errorf(codes.InvalidArgument, "openfga %s: %v", op, err)
	}
	var invalid fgaclient.FgaInvalidError
	if errors.As(err, &invalid) {
		return status.Errorf(codes.InvalidArgument, "openfga %s: %v", op, err)
	}

	var validation fga.FgaApiValidationError
	if errors.As(err, &validation) {
		return status.Errorf(codes.InvalidArgument, "openfga %s: %v", op, err)
	}
	var authErr fga.FgaApiAuthenticationError
	if errors.As(err, &authErr) {
		return status.Errorf(codes.Unauthenticated, "openfga %s: %v", op, err)
	}
	var rateLimited fga.FgaApiRateLimitExceededError
	if errors.As(err, &rateLimited) {
		return status.Errorf(codes.ResourceExhausted, "openfga %s: %v", op, err)
	}
	var notFound fga.FgaApiNotFoundError
	if errors.As(err, &notFound) {
		return status.Errorf(codes.NotFound, "openfga %s: %v", op, err)
	}
	var internal fga.FgaApiInternalError
	if errors.As(err, &internal) {
		return status.Errorf(codes.Internal, "openfga %s: %v", op, err)
	}
	var apiErr fga.FgaApiError
	if errors.As(err, &apiErr) {
		code := codes.Unknown
		switch apiErr.ResponseStatusCode() {
		case http.StatusBadRequest:
			code = codes.InvalidArgument
		case http.StatusUnauthorized:
			code = codes.Unauthenticated
		case http.StatusForbidden:
			code = codes.PermissionDenied
		case http.StatusNotFound:
			code = codes.NotFound
		case http.StatusTooManyRequests:
			code = codes.ResourceExhausted
		default:
			if apiErr.ResponseStatusCode() >= 500 {
				code = codes.Internal
			}
		}
		return status.Errorf(code, "openfga %s: %v", op, err)
	}
	var generic fga.GenericOpenAPIError
	if errors.As(err, &generic) {
		return status.Errorf(codes.Unknown, "openfga %s: %v", op, err)
	}
	return status.Errorf(codes.Unknown, "openfga %s: %v", op, err)
}

func validateUnsupportedProperties(relationship *gestalt.Relationship) error {
	if relationship == nil {
		return nil
	}
	if hasProperties(relationship.GetSubject().GetProperties()) {
		return status.Error(codes.InvalidArgument, "openfga authorization: subject properties are not supported on relationship writes")
	}
	if hasProperties(relationship.GetResource().GetProperties()) {
		return status.Error(codes.InvalidArgument, "openfga authorization: resource properties are not supported on relationship writes")
	}
	if hasProperties(relationship.GetProperties()) {
		return status.Error(codes.InvalidArgument, "openfga authorization: relationship properties are not supported on relationship writes")
	}
	return nil
}

func hasProperties(value *structpb.Struct) bool {
	return value != nil && len(value.GetFields()) > 0
}

func higherConsistencyPtr() *fga.ConsistencyPreference {
	return fga.CONSISTENCYPREFERENCE_HIGHER_CONSISTENCY.Ptr()
}

func cloneStoredModel(model *storedModel) *storedModel {
	if model == nil {
		return nil
	}
	cloned := *model
	if model.ref != nil {
		refCopy := *model.ref
		cloned.ref = &refCopy
	}
	return &cloned
}

func mergeStoredModels(dst, src *storedModel) {
	if dst == nil || src == nil {
		return
	}
	if dst.ref == nil || (src.ref != nil && src.ref.GetCreatedAt().AsTime().Before(dst.ref.GetCreatedAt().AsTime())) {
		if src.ref != nil {
			refCopy := *src.ref
			dst.ref = &refCopy
		}
	}
	if src.writtenAt.After(dst.writtenAt) {
		dst.authorizationModelID = src.authorizationModelID
		dst.writtenAt = src.writtenAt
	}
}

func slicesSortStrings(values []string) {
	sort.Strings(values)
}
