package indexeddb

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	idbfake "github.com/valon-technologies/gestalt-providers/indexeddb/memoryfake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthorizationProviderRoundTrip(t *testing.T) {
	sess := newProviderSession(t)

	meta := sess.provider.Metadata()
	if meta.Kind != gestalt.ProviderKindAuthorization {
		t.Fatalf("kind = %v, want AUTHORIZATION", meta.Kind)
	}
	if meta.Name != "indexeddb" {
		t.Fatalf("name = %q, want indexeddb", meta.Name)
	}

	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	modelRef, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	if modelRef.GetId() == "" {
		t.Fatal("WriteModel returned empty id")
	}
	if modelRef.GetVersion() != "1" {
		t.Fatalf("model version = %q, want 1", modelRef.GetVersion())
	}

	authzMeta, err := sess.provider.GetMetadata(sess.ctx)
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if authzMeta.GetActiveModelId() != modelRef.GetId() {
		t.Fatalf("active_model_id = %q, want %q", authzMeta.GetActiveModelId(), modelRef.GetId())
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{
					Type:       "document",
					Id:         "doc-1",
					Properties: map[string]any{"title": "Roadmap"},
				},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-2"},
			},
			{
				Subject: &gestalt.AuthorizationSubject{
					Type:       "user",
					Id:         "bob",
					Properties: map[string]any{"email": "bob@example.test"},
				},
				Relation: "editor",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	allowed, err := sess.provider.Evaluate(sess.ctx, &gestalt.AccessEvaluationRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Action:   &gestalt.AuthorizationAction{Name: "read"},
		Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
	})
	if err != nil {
		t.Fatalf("Evaluate(read): %v", err)
	}
	if !allowed.GetAllowed() {
		t.Fatal("Evaluate(read) = false, want true")
	}
	if allowed.GetModelId() != modelRef.GetId() {
		t.Fatalf("decision model_id = %q, want %q", allowed.GetModelId(), modelRef.GetId())
	}

	denied, err := sess.provider.Evaluate(sess.ctx, &gestalt.AccessEvaluationRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Action:   &gestalt.AuthorizationAction{Name: "write"},
		Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
	})
	if err != nil {
		t.Fatalf("Evaluate(write): %v", err)
	}
	if denied.GetAllowed() {
		t.Fatal("Evaluate(write) = true, want false")
	}

	many, err := sess.provider.EvaluateMany(sess.ctx, &gestalt.AccessEvaluationsRequest{
		Requests: []*gestalt.AccessEvaluationRequest{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Action:   &gestalt.AuthorizationAction{Name: "read"},
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Action:   &gestalt.AuthorizationAction{Name: "write"},
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("EvaluateMany: %v", err)
	}
	if len(many.GetDecisions()) != 2 || !many.GetDecisions()[0].GetAllowed() || many.GetDecisions()[1].GetAllowed() {
		t.Fatalf("EvaluateMany decisions = %#v", many.GetDecisions())
	}

	resourceSearch, err := sess.provider.SearchResources(sess.ctx, &gestalt.ResourceSearchRequest{
		Subject:      &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Action:       &gestalt.AuthorizationAction{Name: "read"},
		ResourceType: "document",
	})
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if got := resourceIDs(resourceSearch.GetResources()); !reflect.DeepEqual(got, []string{"doc-1", "doc-2"}) {
		t.Fatalf("SearchResources ids = %#v, want %#v", got, []string{"doc-1", "doc-2"})
	}

	subjectSearch, err := sess.provider.SearchSubjects(sess.ctx, &gestalt.SubjectSearchRequest{
		Resource:    &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
		Action:      &gestalt.AuthorizationAction{Name: "write"},
		SubjectType: "user",
	})
	if err != nil {
		t.Fatalf("SearchSubjects: %v", err)
	}
	if got := subjectIDs(subjectSearch.GetSubjects()); !reflect.DeepEqual(got, []string{"bob"}) {
		t.Fatalf("SearchSubjects ids = %#v, want %#v", got, []string{"bob"})
	}

	noSubjects, err := sess.provider.SearchSubjects(sess.ctx, &gestalt.SubjectSearchRequest{
		Resource:    &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
		Action:      &gestalt.AuthorizationAction{Name: "write"},
		SubjectType: "service",
	})
	if err != nil {
		t.Fatalf("SearchSubjects(filtered): %v", err)
	}
	if got := subjectIDs(noSubjects.GetSubjects()); len(got) != 0 {
		t.Fatalf("SearchSubjects filtered ids = %#v, want empty", got)
	}

	actionSearch, err := sess.provider.SearchActions(sess.ctx, &gestalt.ActionSearchRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions: %v", err)
	}
	if got := actionNames(actionSearch.GetActions()); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("SearchActions actions = %#v, want %#v", got, []string{"read", "write"})
	}

	readResp, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Subject: &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships: %v", err)
	}
	if got := relationshipPairs(readResp.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:document/doc-1", "editor:document/doc-2"}) {
		t.Fatalf("ReadRelationships pairs = %#v", got)
	}

	active, err := sess.provider.GetActiveModel(sess.ctx)
	if err != nil {
		t.Fatalf("GetActiveModel: %v", err)
	}
	if active.GetModel().GetId() != modelRef.GetId() {
		t.Fatalf("active model id = %q, want %q", active.GetModel().GetId(), modelRef.GetId())
	}

	models, err := sess.provider.ListModels(sess.ctx, &gestalt.ListModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models.GetModels()) != 1 || models.GetModels()[0].GetId() != modelRef.GetId() {
		t.Fatalf("ListModels = %#v", models.GetModels())
	}

	rotatedModel, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: rotatedModel()})
	if err != nil {
		t.Fatalf("WriteModel(rotated): %v", err)
	}
	if rotatedModel.GetId() == "" {
		t.Fatal("WriteModel(rotated) returned empty id")
	}

	staleActionSearch, err := sess.provider.SearchActions(sess.ctx, &gestalt.ActionSearchRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions(stale): %v", err)
	}
	if got := actionNames(staleActionSearch.GetActions()); len(got) != 0 {
		t.Fatalf("SearchActions stale actions = %#v, want empty", got)
	}

	rotatedDecision, err := sess.provider.Evaluate(sess.ctx, &gestalt.AccessEvaluationRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		Action:   &gestalt.AuthorizationAction{Name: "write"},
		Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("Evaluate(rotated): %v", err)
	}
	if rotatedDecision.GetAllowed() {
		t.Fatal("Evaluate(rotated) = true, want false")
	}
}

func TestAuthorizationProviderInitializesObjectStores(t *testing.T) {
	sess := newProviderSessionWithSeed(t, false)

	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	if got, want := sess.idb.CreatedStoreNames(), []string{stateStoreName, modelsStoreName, relationsStoreName}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created stores = %v, want %v", got, want)
	}
	schema, ok := sess.idb.StoreSchema(relationsStoreName)
	if !ok {
		t.Fatalf("relationships store was not created")
	}
	if want := authorizationRelationshipsSchema(); !reflect.DeepEqual(schema.Indexes, want.Indexes) {
		t.Fatalf("relationships indexes = %+v, want %+v", schema.Indexes, want.Indexes)
	}
}

func TestAuthorizationProviderValidationAndPagination(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	firstModel, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: expandedModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() == secondModel.GetId() {
		t.Fatal("expected distinct model ids")
	}

	firstPage, err := sess.provider.ListModels(sess.ctx, &gestalt.ListModelsRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("ListModels(first page): %v", err)
	}
	if len(firstPage.GetModels()) != 1 || firstPage.GetNextPageToken() == "" {
		t.Fatalf("first ListModels page = %#v", firstPage)
	}
	secondPage, err := sess.provider.ListModels(sess.ctx, &gestalt.ListModelsRequest{
		PageSize:  1,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListModels(second page): %v", err)
	}
	if len(secondPage.GetModels()) != 1 {
		t.Fatalf("second ListModels page = %#v", secondPage)
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships invalid subject type code = %v, want INVALID_ARGUMENT", err)
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships(valid): %v", err)
	}

	readPage, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("ReadRelationships(first page): %v", err)
	}
	if len(readPage.GetRelationships()) != 1 || readPage.GetNextPageToken() == "" {
		t.Fatalf("first ReadRelationships page = %#v", readPage)
	}
	nextPage, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Subject:   &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
		PageSize:  1,
		PageToken: readPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ReadRelationships(second page): %v", err)
	}
	if len(nextPage.GetRelationships()) != 1 {
		t.Fatalf("second ReadRelationships page = %#v", nextPage)
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Deletes: []*gestalt.RelationshipKey{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-1"},
			},
		},
		Writes: []*gestalt.Relationship{
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "document", Id: "doc-9"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships mixed batch code = %v, want INVALID_ARGUMENT", err)
	}

	atomicRead, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Subject: &gestalt.AuthorizationSubject{Type: "user", Id: "alice"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships(after failed batch): %v", err)
	}
	if got := relationshipPairs(atomicRead.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:document/doc-1", "editor:document/doc-2"}) {
		t.Fatalf("ReadRelationships after failed batch = %#v", got)
	}
}

func TestAuthorizationProviderWriteModelIsIdempotent(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	firstModel, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() != secondModel.GetId() {
		t.Fatalf("WriteModel id mismatch: first=%q second=%q", firstModel.GetId(), secondModel.GetId())
	}
	if !firstModel.GetCreatedAt().Equal(secondModel.GetCreatedAt()) {
		t.Fatalf("WriteModel created_at mismatch: first=%v second=%v", firstModel.GetCreatedAt(), secondModel.GetCreatedAt())
	}

	models, err := sess.provider.ListModels(sess.ctx, &gestalt.ListModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models.GetModels()) != 1 {
		t.Fatalf("ListModels count = %d, want 1", len(models.GetModels()))
	}
}

func TestAuthorizationProviderZanzibarTargetsAndEffectiveAccess(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	modelRef, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: zanzibarSessionModel()})
	if err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "everyone", Id: "global"}, "member"),
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-public"},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
				Relation: "member",
				Resource: &gestalt.AuthorizationResource{Type: "team", Id: "team-ml"},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:bob"},
				Relation: "member",
				Resource: &gestalt.AuthorizationResource{Type: "slack_channel", Id: "C123"},
			},
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "team", Id: "team-ml", Properties: map[string]any{"name": "ML"}}, "member"),
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-team"},
			},
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "slack_channel", Id: "C123"}, "member"),
				Relation: "editor",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-slack"},
			},
			{
				Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:charlie"},
				Relation: "viewer",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-parent"},
			},
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&gestalt.AuthorizationResource{Type: "agent_session", Id: "session-parent", Properties: map[string]any{"title": "Parent"}}),
				Relation: "parent",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-child"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	assertDecision(t, sess, "user:dana", "view", "session-public", true)
	assertDecision(t, sess, "user:alice", "view", "session-team", true)
	assertDecision(t, sess, "user:alice", "edit", "session-team", false)
	assertDecision(t, sess, "user:bob", "edit", "session-slack", true)
	assertDecision(t, sess, "user:bob", "view", "session-slack", true)
	assertDecision(t, sess, "user:charlie", "view", "session-child", true)
	assertDecision(t, sess, "user:charlie", "edit", "session-child", false)

	directResources, err := sess.provider.SearchResources(sess.ctx, &gestalt.ResourceSearchRequest{
		Subject:      &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
		Action:       &gestalt.AuthorizationAction{Name: "view"},
		ResourceType: "agent_session",
	})
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if got := resourceIDs(directResources.GetResources()); len(got) != 0 {
		t.Fatalf("direct SearchResources ids = %#v, want empty", got)
	}

	actionSearch, err := sess.provider.SearchActions(sess.ctx, &gestalt.ActionSearchRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-team"},
	})
	if err != nil {
		t.Fatalf("SearchActions(effective): %v", err)
	}
	if got := actionNames(actionSearch.GetActions()); !reflect.DeepEqual(got, []string{"view"}) {
		t.Fatalf("SearchActions(effective) actions = %#v, want view", got)
	}

	effectiveResources, err := sess.provider.EffectiveSearchResources(sess.ctx, &gestalt.ResourceSearchRequest{
		Subject:      &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
		Action:       &gestalt.AuthorizationAction{Name: "view"},
		ResourceType: "agent_session",
	})
	if err != nil {
		t.Fatalf("EffectiveSearchResources: %v", err)
	}
	if got := resourceIDs(effectiveResources.GetResources()); !reflect.DeepEqual(got, []string{"session-public", "session-team"}) {
		t.Fatalf("EffectiveSearchResources ids = %#v, want public and team sessions", got)
	}
	if effectiveResources.GetModelId() != modelRef.GetId() {
		t.Fatalf("EffectiveSearchResources model_id = %q, want %q", effectiveResources.GetModelId(), modelRef.GetId())
	}

	targetResp, err := sess.provider.EffectiveSearchSubjects(sess.ctx, &gestalt.EffectiveSubjectSearchRequest{
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-slack"},
		Action:   &gestalt.AuthorizationAction{Name: "edit"},
	})
	if err != nil {
		t.Fatalf("EffectiveSearchSubjects: %v", err)
	}
	if got := targetSummaries(targetResp.GetTargets()); !reflect.DeepEqual(got, []string{"set:slack_channel/C123#member"}) {
		t.Fatalf("EffectiveSearchSubjects targets = %#v", got)
	}

	readResp, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "team", Id: "team-ml"}, "member"),
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-team"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships(target): %v", err)
	}
	if got := relationshipPairs(readResp.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:agent_session/session-team"}) {
		t.Fatalf("ReadRelationships(target) pairs = %#v", got)
	}
	if got := targetSummary(readResp.GetRelationships()[0].GetTarget()); got != "set:team/team-ml#member" {
		t.Fatalf("ReadRelationships(target) target = %q, want set:team/team-ml#member", got)
	}
	if got := readResp.GetRelationships()[0].GetTarget().GetSubjectSet().GetResource().GetProperties()["name"]; got != "ML" {
		t.Fatalf("ReadRelationships(target) target resource property name = %#v, want ML", got)
	}

	parentReadResp, err := sess.provider.ReadRelationships(sess.ctx, &gestalt.ReadRelationshipsRequest{
		Target:   gestalt.NewAuthorizationResourceTarget(&gestalt.AuthorizationResource{Type: "agent_session", Id: "session-parent"}),
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-child"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships(parent target): %v", err)
	}
	if got := relationshipPairs(parentReadResp.GetRelationships()); !reflect.DeepEqual(got, []string{"parent:agent_session/session-child"}) {
		t.Fatalf("ReadRelationships(parent target) pairs = %#v", got)
	}
	if got := parentReadResp.GetRelationships()[0].GetTarget().GetResource().GetProperties()["title"]; got != "Parent" {
		t.Fatalf("ReadRelationships(parent target) target resource property title = %#v, want Parent", got)
	}

	expandResp, err := sess.provider.Expand(sess.ctx, &gestalt.ExpandRequest{
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-child"},
		Relation: "viewer",
		MaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if !expandContainsTarget(expandResp.GetRoot(), "subject:subject/user:charlie") {
		t.Fatalf("Expand tree did not include inherited charlie viewer: %#v", expandResp.GetRoot())
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Deletes: []*gestalt.RelationshipKey{{
			Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "team", Id: "team-ml"}, "member"),
			Relation: "viewer",
			Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-team"},
		}},
	})
	if err != nil {
		t.Fatalf("WriteRelationships(delete target): %v", err)
	}
	assertDecision(t, sess, "user:alice", "view", "session-team", false)
}

func TestAuthorizationProviderRejectsMismatchedRelationshipTargets(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})
	if _, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: zanzibarSessionModel()}); err != nil {
		t.Fatalf("WriteModel: %v", err)
	}

	err := sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{{
			Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
			Target:   gestalt.NewAuthorizationSubjectTarget(&gestalt.AuthorizationSubject{Type: "subject", Id: "user:bob"}),
			Relation: "viewer",
			Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-1"},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("mismatched subject target code = %v, want INVALID_ARGUMENT", err)
	}

	err = sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{{
			Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: "user:alice"},
			Target:   gestalt.NewAuthorizationSubjectSetTarget(&gestalt.AuthorizationResource{Type: "team", Id: "team-ml"}, "member"),
			Relation: "viewer",
			Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-1"},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("subject plus non-subject target code = %v, want INVALID_ARGUMENT", err)
	}
}

func TestAuthorizationProviderExpandDetectsCyclesAndMaxDepth(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})
	if _, err := sess.provider.WriteModel(sess.ctx, &gestalt.WriteModelRequest{Model: zanzibarSessionModel()}); err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	err := sess.provider.WriteRelationships(sess.ctx, &gestalt.WriteRelationshipsRequest{
		Writes: []*gestalt.Relationship{
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&gestalt.AuthorizationResource{Type: "agent_session", Id: "session-b"}),
				Relation: "parent",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-a"},
			},
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&gestalt.AuthorizationResource{Type: "agent_session", Id: "session-a"}),
				Relation: "parent",
				Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-b"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	cycleResp, err := sess.provider.Expand(sess.ctx, &gestalt.ExpandRequest{
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-a"},
		Relation: "viewer",
		MaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("Expand(cycle): %v", err)
	}
	if !cycleResp.GetCycleDetected() {
		t.Fatal("Expand(cycle) did not report cycle_detected")
	}

	depthResp, err := sess.provider.Expand(sess.ctx, &gestalt.ExpandRequest{
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: "session-a"},
		Relation: "viewer",
		MaxDepth: 1,
	})
	if err != nil {
		t.Fatalf("Expand(max depth): %v", err)
	}
	if !depthResp.GetMaxDepthReached() || !depthResp.GetTruncated() {
		t.Fatalf("Expand(max depth) flags = max_depth_reached:%v truncated:%v, want both true", depthResp.GetMaxDepthReached(), depthResp.GetTruncated())
	}
}

type providerSession struct {
	ctx      context.Context
	cancel   context.CancelFunc
	provider *Provider
	idb      *idbfake.IndexedDB
}

func newProviderSession(t *testing.T) *providerSession {
	t.Helper()
	return newProviderSessionWithSeed(t, true)
}

func newProviderSessionWithSeed(t *testing.T, seedStores bool) *providerSession {
	t.Helper()

	fakeDB := idbfake.New()
	if seedStores {
		if err := seedAuthorizationStoresOnClient(context.Background(), fakeDB); err != nil {
			t.Fatalf("seedAuthorizationStoresOnClient: %v", err)
		}
	}

	origConnect := connectIndexedDB
	connectIndexedDB = func(binding string) (indexeddb.Database, error) {
		if binding != "" && binding != "test" {
			return nil, fmt.Errorf("unexpected indexeddb binding %q", binding)
		}
		return fakeDB, nil
	}
	t.Cleanup(func() { connectIndexedDB = origConnect })

	authzProvider := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	session := &providerSession{
		ctx:      ctx,
		cancel:   cancel,
		provider: authzProvider,
		idb:      fakeDB,
	}
	t.Cleanup(func() {
		cancel()
		_ = authzProvider.Close()
	})
	return session
}

func seedAuthorizationStoresOnClient(ctx context.Context, client indexeddb.Database) error {
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: stateStoreName, schema: gestalt.ObjectStoreSchema{}},
		{name: modelsStoreName, schema: gestalt.ObjectStoreSchema{}},
		{name: relationsStoreName, schema: authorizationRelationshipsSchema()},
	} {
		if _, err := client.CreateObjectStore(ctx, def.name, def.schema); err != nil {
			return err
		}
	}
	return nil
}

func (s *providerSession) configure(t *testing.T, config map[string]any) {
	t.Helper()
	if err := s.provider.Configure(s.ctx, "authz-indexeddb", config); err != nil {
		t.Fatalf("ConfigureProvider: %v", err)
	}
}

func resourceIDs(resources []*gestalt.AuthorizationResource) []string {
	out := make([]string, len(resources))
	for i, resource := range resources {
		out[i] = resource.GetId()
	}
	return out
}

func subjectIDs(subjects []*gestalt.AuthorizationSubject) []string {
	out := make([]string, len(subjects))
	for i, subject := range subjects {
		out[i] = subject.GetId()
	}
	return out
}

func actionNames(actions []*gestalt.AuthorizationAction) []string {
	out := make([]string, len(actions))
	for i, action := range actions {
		out[i] = action.GetName()
	}
	return out
}

func relationshipPairs(relationships []*gestalt.Relationship) []string {
	out := make([]string, len(relationships))
	for i, relationship := range relationships {
		out[i] = relationship.GetRelation() + ":" + relationship.GetResource().GetType() + "/" + relationship.GetResource().GetId()
	}
	return out
}

func targetSummaries(targets []*gestalt.AuthorizationRelationshipTarget) []string {
	out := make([]string, len(targets))
	for i, target := range targets {
		out[i] = targetSummary(target)
	}
	return out
}

func targetSummary(target *gestalt.AuthorizationRelationshipTarget) string {
	if subject := target.GetSubject(); subject != nil {
		return "subject:" + subject.GetType() + "/" + subject.GetId()
	}
	if resource := target.GetResource(); resource != nil {
		return "resource:" + resource.GetType() + "/" + resource.GetId()
	}
	if set := target.GetSubjectSet(); set != nil {
		return "set:" + set.GetResource().GetType() + "/" + set.GetResource().GetId() + "#" + set.GetRelation()
	}
	return "<nil>"
}

func expandContainsTarget(node *gestalt.ExpandNode, summary string) bool {
	if node == nil {
		return false
	}
	if targetSummary(node.GetTarget()) == summary {
		return true
	}
	for _, child := range node.GetChildren() {
		if expandContainsTarget(child, summary) {
			return true
		}
	}
	return false
}

func assertDecision(t *testing.T, sess *providerSession, subjectID, action, sessionID string, want bool) {
	t.Helper()
	resp, err := sess.provider.Evaluate(sess.ctx, &gestalt.AccessEvaluationRequest{
		Subject:  &gestalt.AuthorizationSubject{Type: "subject", Id: subjectID},
		Action:   &gestalt.AuthorizationAction{Name: action},
		Resource: &gestalt.AuthorizationResource{Type: "agent_session", Id: sessionID},
	})
	if err != nil {
		t.Fatalf("Evaluate(%s, %s, %s): %v", subjectID, action, sessionID, err)
	}
	if resp.GetAllowed() != want {
		t.Fatalf("Evaluate(%s, %s, %s) = %v, want %v", subjectID, action, sessionID, resp.GetAllowed(), want)
	}
}

func roundTripModel() *gestalt.AuthorizationModel {
	return &gestalt.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*gestalt.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*gestalt.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
			},
			Actions: []*gestalt.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
	}
}

func expandedModel() *gestalt.AuthorizationModel {
	return &gestalt.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*gestalt.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*gestalt.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
				{Name: "owner", SubjectTypes: []string{"user"}},
			},
			Actions: []*gestalt.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor", "owner"}},
				{Name: "write", Relations: []string{"editor", "owner"}},
				{Name: "admin", Relations: []string{"owner"}},
			},
		}},
	}
}

func rotatedModel() *gestalt.AuthorizationModel {
	return &gestalt.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*gestalt.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*gestalt.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"group"}},
			},
			Actions: []*gestalt.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
	}
}

func zanzibarSessionModel() *gestalt.AuthorizationModel {
	return &gestalt.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*gestalt.AuthorizationModelResourceType{
			{
				Name: "agent_session",
				Relations: []*gestalt.AuthorizationModelRelation{
					zanzibarSessionAccessRelation("viewer"),
					zanzibarSessionAccessRelation("editor"),
					{
						Name:           "parent",
						SubjectTypes:   []string{"subject"},
						AllowedTargets: []*gestalt.AuthorizationModelAllowedTarget{gestalt.NewAuthorizationModelResourceTypeTarget("agent_session")},
					},
				},
				Actions: []*gestalt.AuthorizationModelAction{
					{
						Name:      "view",
						Relations: []string{"viewer", "editor"},
						Rewrite: gestalt.NewAuthorizationModelUnionRewrite(
							gestalt.NewAuthorizationModelComputedUsersetRewrite("viewer"),
							gestalt.NewAuthorizationModelComputedUsersetRewrite("editor"),
						),
					},
					{
						Name:      "edit",
						Relations: []string{"editor"},
						Rewrite:   gestalt.NewAuthorizationModelComputedUsersetRewrite("editor"),
					},
				},
			},
			membershipResourceType("everyone"),
			membershipResourceType("team"),
			membershipResourceType("slack_channel"),
		},
	}
}

func zanzibarSessionAccessRelation(name string) *gestalt.AuthorizationModelRelation {
	children := []*gestalt.AuthorizationModelRewrite{gestalt.NewAuthorizationModelThisRewrite()}
	if name == "viewer" {
		children = append(children, gestalt.NewAuthorizationModelComputedUsersetRewrite("editor"))
	}
	children = append(children, gestalt.NewAuthorizationModelTupleToUsersetRewrite("parent", name))
	return &gestalt.AuthorizationModelRelation{
		Name:         name,
		SubjectTypes: []string{"subject"},
		AllowedTargets: []*gestalt.AuthorizationModelAllowedTarget{
			gestalt.NewAuthorizationModelSubjectTypeTarget("subject"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("everyone", "member"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("team", "member"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("slack_channel", "member"),
		},
		Rewrite: gestalt.NewAuthorizationModelUnionRewrite(children...),
	}
}

func membershipResourceType(name string) *gestalt.AuthorizationModelResourceType {
	return &gestalt.AuthorizationModelResourceType{
		Name: name,
		Relations: []*gestalt.AuthorizationModelRelation{{
			Name:         "member",
			SubjectTypes: []string{"subject"},
			AllowedTargets: []*gestalt.AuthorizationModelAllowedTarget{
				gestalt.NewAuthorizationModelSubjectTypeTarget("subject"),
			},
			Rewrite: gestalt.NewAuthorizationModelThisRewrite(),
		}},
		Actions: []*gestalt.AuthorizationModelAction{{
			Name:      "member",
			Relations: []string{"member"},
			Rewrite:   gestalt.NewAuthorizationModelComputedUsersetRewrite("member"),
		}},
	}
}
