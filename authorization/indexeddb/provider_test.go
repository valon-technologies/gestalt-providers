package indexeddb

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAuthorizationProviderRoundTrip(t *testing.T) {
	sess := newProviderSession(t)

	meta, err := sess.runtime.GetProviderIdentity(sess.ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("GetProviderIdentity: %v", err)
	}
	if meta.GetKind() != proto.ProviderKind_PROVIDER_KIND_AUTHORIZATION {
		t.Fatalf("kind = %v, want AUTHORIZATION", meta.GetKind())
	}
	if meta.GetName() != "indexeddb" {
		t.Fatalf("name = %q, want indexeddb", meta.GetName())
	}

	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	modelRef, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	if modelRef.GetId() == "" {
		t.Fatal("WriteModel returned empty id")
	}
	if modelRef.GetVersion() != "1" {
		t.Fatalf("model version = %q, want 1", modelRef.GetVersion())
	}

	authzMeta, err := sess.client.GetMetadata(sess.ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	if authzMeta.GetActiveModelId() != modelRef.GetId() {
		t.Fatalf("active_model_id = %q, want %q", authzMeta.GetActiveModelId(), modelRef.GetId())
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{
					Type:       "document",
					Id:         "doc-1",
					Properties: mustStruct(t, map[string]any{"title": "Roadmap"}),
				},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-2"},
			},
			{
				Subject: &proto.Subject{
					Type:       "user",
					Id:         "bob",
					Properties: mustStruct(t, map[string]any{"email": "bob@example.test"}),
				},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	allowed, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "read"},
		Resource: &proto.Resource{Type: "document", Id: "doc-1"},
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

	denied, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "write"},
		Resource: &proto.Resource{Type: "document", Id: "doc-1"},
	})
	if err != nil {
		t.Fatalf("Evaluate(write): %v", err)
	}
	if denied.GetAllowed() {
		t.Fatal("Evaluate(write) = true, want false")
	}

	many, err := sess.client.EvaluateMany(sess.ctx, &proto.AccessEvaluationsRequest{
		Requests: []*proto.AccessEvaluationRequest{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Action:   &proto.Action{Name: "read"},
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Action:   &proto.Action{Name: "write"},
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("EvaluateMany: %v", err)
	}
	if len(many.GetDecisions()) != 2 || !many.GetDecisions()[0].GetAllowed() || many.GetDecisions()[1].GetAllowed() {
		t.Fatalf("EvaluateMany decisions = %#v", many.GetDecisions())
	}

	resourceSearch, err := sess.client.SearchResources(sess.ctx, &proto.ResourceSearchRequest{
		Subject:      &proto.Subject{Type: "user", Id: "alice"},
		Action:       &proto.Action{Name: "read"},
		ResourceType: "document",
	})
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if got := resourceIDs(resourceSearch.GetResources()); !reflect.DeepEqual(got, []string{"doc-1", "doc-2"}) {
		t.Fatalf("SearchResources ids = %#v, want %#v", got, []string{"doc-1", "doc-2"})
	}

	subjectSearch, err := sess.client.SearchSubjects(sess.ctx, &proto.SubjectSearchRequest{
		Resource:    &proto.Resource{Type: "document", Id: "doc-1"},
		Action:      &proto.Action{Name: "write"},
		SubjectType: "user",
	})
	if err != nil {
		t.Fatalf("SearchSubjects: %v", err)
	}
	if got := subjectIDs(subjectSearch.GetSubjects()); !reflect.DeepEqual(got, []string{"bob"}) {
		t.Fatalf("SearchSubjects ids = %#v, want %#v", got, []string{"bob"})
	}

	noSubjects, err := sess.client.SearchSubjects(sess.ctx, &proto.SubjectSearchRequest{
		Resource:    &proto.Resource{Type: "document", Id: "doc-1"},
		Action:      &proto.Action{Name: "write"},
		SubjectType: "service",
	})
	if err != nil {
		t.Fatalf("SearchSubjects(filtered): %v", err)
	}
	if got := subjectIDs(noSubjects.GetSubjects()); len(got) != 0 {
		t.Fatalf("SearchSubjects filtered ids = %#v, want empty", got)
	}

	actionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions: %v", err)
	}
	if got := actionNames(actionSearch.GetActions()); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("SearchActions actions = %#v, want %#v", got, []string{"read", "write"})
	}

	readResp, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject: &proto.Subject{Type: "user", Id: "alice"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships: %v", err)
	}
	if got := relationshipPairs(readResp.GetRelationships()); !reflect.DeepEqual(got, []string{"viewer:document/doc-1", "editor:document/doc-2"}) {
		t.Fatalf("ReadRelationships pairs = %#v", got)
	}

	active, err := sess.client.GetActiveModel(sess.ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetActiveModel: %v", err)
	}
	if active.GetModel().GetId() != modelRef.GetId() {
		t.Fatalf("active model id = %q, want %q", active.GetModel().GetId(), modelRef.GetId())
	}

	models, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models.GetModels()) != 1 || models.GetModels()[0].GetId() != modelRef.GetId() {
		t.Fatalf("ListModels = %#v", models.GetModels())
	}

	rotatedModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: rotatedModel()})
	if err != nil {
		t.Fatalf("WriteModel(rotated): %v", err)
	}
	if rotatedModel.GetId() == "" {
		t.Fatal("WriteModel(rotated) returned empty id")
	}

	staleActionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("SearchActions(stale): %v", err)
	}
	if got := actionNames(staleActionSearch.GetActions()); len(got) != 0 {
		t.Fatalf("SearchActions stale actions = %#v, want empty", got)
	}

	rotatedDecision, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		Action:   &proto.Action{Name: "write"},
		Resource: &proto.Resource{Type: "document", Id: "doc-2"},
	})
	if err != nil {
		t.Fatalf("Evaluate(rotated): %v", err)
	}
	if rotatedDecision.GetAllowed() {
		t.Fatal("Evaluate(rotated) = true, want false")
	}
}

func TestAuthorizationProviderValidationAndPagination(t *testing.T) {
	sess := newProviderSession(t)
	sess.configure(t, map[string]any{
		"indexeddb": "test",
	})

	firstModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: expandedModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() == secondModel.GetId() {
		t.Fatal("expected distinct model ids")
	}

	firstPage, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("ListModels(first page): %v", err)
	}
	if len(firstPage.GetModels()) != 1 || firstPage.GetNextPageToken() == "" {
		t.Fatalf("first ListModels page = %#v", firstPage)
	}
	secondPage, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{
		PageSize:  1,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListModels(second page): %v", err)
	}
	if len(secondPage.GetModels()) != 1 {
		t.Fatalf("second ListModels page = %#v", secondPage)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships invalid subject type code = %v, want INVALID_ARGUMENT", err)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "editor",
				Resource: &proto.Resource{Type: "document", Id: "doc-2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships(valid): %v", err)
	}

	readPage, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject:  &proto.Subject{Type: "user", Id: "alice"},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("ReadRelationships(first page): %v", err)
	}
	if len(readPage.GetRelationships()) != 1 || readPage.GetNextPageToken() == "" {
		t.Fatalf("first ReadRelationships page = %#v", readPage)
	}
	nextPage, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject:   &proto.Subject{Type: "user", Id: "alice"},
		PageSize:  1,
		PageToken: readPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ReadRelationships(second page): %v", err)
	}
	if len(nextPage.GetRelationships()) != 1 {
		t.Fatalf("second ReadRelationships page = %#v", nextPage)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Deletes: []*proto.RelationshipKey{
			{
				Subject:  &proto.Subject{Type: "user", Id: "alice"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-1"},
			},
		},
		Writes: []*proto.Relationship{
			{
				Subject:  &proto.Subject{Type: "service", Id: "worker-1"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "document", Id: "doc-9"},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRelationships mixed batch code = %v, want INVALID_ARGUMENT", err)
	}

	atomicRead, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Subject: &proto.Subject{Type: "user", Id: "alice"},
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

	firstModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(first): %v", err)
	}
	secondModel, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: roundTripModel()})
	if err != nil {
		t.Fatalf("WriteModel(second): %v", err)
	}
	if firstModel.GetId() != secondModel.GetId() {
		t.Fatalf("WriteModel id mismatch: first=%q second=%q", firstModel.GetId(), secondModel.GetId())
	}
	if !firstModel.GetCreatedAt().AsTime().Equal(secondModel.GetCreatedAt().AsTime()) {
		t.Fatalf("WriteModel created_at mismatch: first=%v second=%v", firstModel.GetCreatedAt(), secondModel.GetCreatedAt())
	}

	models, err := sess.client.ListModels(sess.ctx, &proto.ListModelsRequest{})
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

	modelRef, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: zanzibarSessionModel()})
	if err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "everyone", Id: "global"}, "member"),
				Relation: "viewer",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-public"},
			},
			{
				Subject:  &proto.Subject{Type: "subject", Id: "user:alice"},
				Relation: "member",
				Resource: &proto.Resource{Type: "team", Id: "team-ml"},
			},
			{
				Subject:  &proto.Subject{Type: "subject", Id: "user:bob"},
				Relation: "member",
				Resource: &proto.Resource{Type: "slack_channel", Id: "C123"},
			},
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "team", Id: "team-ml", Properties: mustStruct(t, map[string]any{"name": "ML"})}, "member"),
				Relation: "viewer",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-team"},
			},
			{
				Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "slack_channel", Id: "C123"}, "member"),
				Relation: "editor",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-slack"},
			},
			{
				Subject:  &proto.Subject{Type: "subject", Id: "user:charlie"},
				Relation: "viewer",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-parent"},
			},
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&proto.Resource{Type: "agent_session", Id: "session-parent", Properties: mustStruct(t, map[string]any{"title": "Parent"})}),
				Relation: "parent",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-child"},
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

	directResources, err := sess.client.SearchResources(sess.ctx, &proto.ResourceSearchRequest{
		Subject:      &proto.Subject{Type: "subject", Id: "user:alice"},
		Action:       &proto.Action{Name: "view"},
		ResourceType: "agent_session",
	})
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if got := resourceIDs(directResources.GetResources()); len(got) != 0 {
		t.Fatalf("direct SearchResources ids = %#v, want empty", got)
	}

	actionSearch, err := sess.client.SearchActions(sess.ctx, &proto.ActionSearchRequest{
		Subject:  &proto.Subject{Type: "subject", Id: "user:alice"},
		Resource: &proto.Resource{Type: "agent_session", Id: "session-team"},
	})
	if err != nil {
		t.Fatalf("SearchActions(effective): %v", err)
	}
	if got := actionNames(actionSearch.GetActions()); !reflect.DeepEqual(got, []string{"view"}) {
		t.Fatalf("SearchActions(effective) actions = %#v, want view", got)
	}

	effectiveResources, err := sess.client.EffectiveSearchResources(sess.ctx, &proto.ResourceSearchRequest{
		Subject:      &proto.Subject{Type: "subject", Id: "user:alice"},
		Action:       &proto.Action{Name: "view"},
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

	targetResp, err := sess.client.EffectiveSearchSubjects(sess.ctx, &proto.EffectiveSubjectSearchRequest{
		Resource: &proto.Resource{Type: "agent_session", Id: "session-slack"},
		Action:   &proto.Action{Name: "edit"},
	})
	if err != nil {
		t.Fatalf("EffectiveSearchSubjects: %v", err)
	}
	if got := targetSummaries(targetResp.GetTargets()); !reflect.DeepEqual(got, []string{"set:slack_channel/C123#member"}) {
		t.Fatalf("EffectiveSearchSubjects targets = %#v", got)
	}

	readResp, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "team", Id: "team-ml"}, "member"),
		Resource: &proto.Resource{Type: "agent_session", Id: "session-team"},
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
	if got := readResp.GetRelationships()[0].GetTarget().GetSubjectSet().GetResource().GetProperties().AsMap()["name"]; got != "ML" {
		t.Fatalf("ReadRelationships(target) target resource property name = %#v, want ML", got)
	}

	parentReadResp, err := sess.client.ReadRelationships(sess.ctx, &proto.ReadRelationshipsRequest{
		Target:   gestalt.NewAuthorizationResourceTarget(&proto.Resource{Type: "agent_session", Id: "session-parent"}),
		Resource: &proto.Resource{Type: "agent_session", Id: "session-child"},
	})
	if err != nil {
		t.Fatalf("ReadRelationships(parent target): %v", err)
	}
	if got := relationshipPairs(parentReadResp.GetRelationships()); !reflect.DeepEqual(got, []string{"parent:agent_session/session-child"}) {
		t.Fatalf("ReadRelationships(parent target) pairs = %#v", got)
	}
	if got := parentReadResp.GetRelationships()[0].GetTarget().GetResource().GetProperties().AsMap()["title"]; got != "Parent" {
		t.Fatalf("ReadRelationships(parent target) target resource property title = %#v, want Parent", got)
	}

	expandResp, err := sess.client.Expand(sess.ctx, &proto.ExpandRequest{
		Resource: &proto.Resource{Type: "agent_session", Id: "session-child"},
		Relation: "viewer",
		MaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}
	if !expandContainsTarget(expandResp.GetRoot(), "subject:subject/user:charlie") {
		t.Fatalf("Expand tree did not include inherited charlie viewer: %#v", expandResp.GetRoot())
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Deletes: []*proto.RelationshipKey{{
			Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "team", Id: "team-ml"}, "member"),
			Relation: "viewer",
			Resource: &proto.Resource{Type: "agent_session", Id: "session-team"},
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
	if _, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: zanzibarSessionModel()}); err != nil {
		t.Fatalf("WriteModel: %v", err)
	}

	_, err := sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{{
			Subject:  &proto.Subject{Type: "subject", Id: "user:alice"},
			Target:   gestalt.NewAuthorizationSubjectTarget(&proto.Subject{Type: "subject", Id: "user:bob"}),
			Relation: "viewer",
			Resource: &proto.Resource{Type: "agent_session", Id: "session-1"},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("mismatched subject target code = %v, want INVALID_ARGUMENT", err)
	}

	_, err = sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{{
			Subject:  &proto.Subject{Type: "subject", Id: "user:alice"},
			Target:   gestalt.NewAuthorizationSubjectSetTarget(&proto.Resource{Type: "team", Id: "team-ml"}, "member"),
			Relation: "viewer",
			Resource: &proto.Resource{Type: "agent_session", Id: "session-1"},
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
	if _, err := sess.client.WriteModel(sess.ctx, &proto.WriteModelRequest{Model: zanzibarSessionModel()}); err != nil {
		t.Fatalf("WriteModel: %v", err)
	}
	_, err := sess.client.WriteRelationships(sess.ctx, &proto.WriteRelationshipsRequest{
		Writes: []*proto.Relationship{
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&proto.Resource{Type: "agent_session", Id: "session-b"}),
				Relation: "parent",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-a"},
			},
			{
				Target:   gestalt.NewAuthorizationResourceTarget(&proto.Resource{Type: "agent_session", Id: "session-a"}),
				Relation: "parent",
				Resource: &proto.Resource{Type: "agent_session", Id: "session-b"},
			},
		},
	})
	if err != nil {
		t.Fatalf("WriteRelationships: %v", err)
	}

	cycleResp, err := sess.client.Expand(sess.ctx, &proto.ExpandRequest{
		Resource: &proto.Resource{Type: "agent_session", Id: "session-a"},
		Relation: "viewer",
		MaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("Expand(cycle): %v", err)
	}
	if !cycleResp.GetCycleDetected() {
		t.Fatal("Expand(cycle) did not report cycle_detected")
	}

	depthResp, err := sess.client.Expand(sess.ctx, &proto.ExpandRequest{
		Resource: &proto.Resource{Type: "agent_session", Id: "session-a"},
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
	ctx        context.Context
	cancel     context.CancelFunc
	runtime    proto.ProviderLifecycleClient
	client     proto.AuthorizationProviderClient
	idbErrCh   chan error
	authzErrCh chan error
}

func newProviderSession(t *testing.T) *providerSession {
	t.Helper()

	idbSocket := newSocketPath(t, "indexeddb.sock")
	authzSocket := newSocketPath(t, "authorization.sock")

	t.Setenv(proto.EnvProviderSocket, idbSocket)
	idbProvider := newTestIndexedDBProvider()
	seedAuthorizationStores(t, idbProvider)
	idbCtx, idbCancel := context.WithCancel(context.Background())
	idbErrCh := make(chan error, 1)
	go func() {
		idbErrCh <- gestalt.ServeIndexedDBProvider(idbCtx, idbProvider)
	}()
	idbConn := newUnixConn(t, idbSocket)
	_ = idbConn.Close()

	t.Setenv(gestalt.IndexedDBSocketEnv("test"), idbSocket)
	t.Setenv(proto.EnvProviderSocket, authzSocket)
	authzProvider := New()
	authzCtx, authzCancel := context.WithCancel(context.Background())
	authzErrCh := make(chan error, 1)
	go func() {
		authzErrCh <- gestalt.ServeAuthorizationProvider(authzCtx, authzProvider)
	}()

	conn := newUnixConn(t, authzSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	session := &providerSession{
		ctx:        ctx,
		cancel:     cancel,
		runtime:    proto.NewProviderLifecycleClient(conn),
		client:     proto.NewAuthorizationProviderClient(conn),
		idbErrCh:   idbErrCh,
		authzErrCh: authzErrCh,
	}
	t.Cleanup(func() {
		cancel()
		authzCancel()
		waitServeResult(t, authzErrCh)
		idbCancel()
		waitServeResult(t, idbErrCh)
		_ = conn.Close()
	})
	return session
}

func (s *providerSession) configure(t *testing.T, config map[string]any) {
	t.Helper()
	cfg, err := structpb.NewStruct(config)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	_, err = s.runtime.ConfigureProvider(s.ctx, &proto.ConfigureProviderRequest{
		Name:            "authz-indexeddb",
		Config:          cfg,
		ProtocolVersion: proto.CurrentProtocolVersion,
	})
	if err != nil {
		t.Fatalf("ConfigureProvider: %v", err)
	}
}

func seedAuthorizationStores(t *testing.T, provider *testIndexedDBProvider) {
	t.Helper()
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: stateStoreName, schema: gestalt.ObjectStoreSchema{}},
		{name: modelsStoreName, schema: gestalt.ObjectStoreSchema{}},
		{name: relationsStoreName, schema: authorizationRelationshipsSchema()},
	} {
		if err := provider.CreateObjectStore(context.Background(), def.name, def.schema); err != nil {
			t.Fatalf("CreateObjectStore(%s): %v", def.name, err)
		}
	}
}

func authorizationRelationshipsSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: relationshipsBySubj, KeyPath: []string{"subject_type", "subject_id"}},
			{Name: relationshipsByRes, KeyPath: []string{"resource_type", "resource_id"}},
			{Name: relationshipsByPair, KeyPath: []string{"subject_type", "subject_id", "resource_type", "resource_id"}},
		},
	}
}

func newSocketPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gst-authz-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

func newUnixConn(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			conn, dialErr := grpc.NewClient(
				"passthrough:///"+socket,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", addr)
				}),
			)
			if dialErr != nil {
				t.Fatalf("grpc.NewClient: %v", dialErr)
			}
			conn.Connect()
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %q was not created", socket)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitServeResult(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not stop after context cancellation")
	}
}

func mustStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	value, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	return value
}

func resourceIDs(resources []*proto.Resource) []string {
	out := make([]string, len(resources))
	for i, resource := range resources {
		out[i] = resource.GetId()
	}
	return out
}

func subjectIDs(subjects []*proto.Subject) []string {
	out := make([]string, len(subjects))
	for i, subject := range subjects {
		out[i] = subject.GetId()
	}
	return out
}

func actionNames(actions []*proto.Action) []string {
	out := make([]string, len(actions))
	for i, action := range actions {
		out[i] = action.GetName()
	}
	return out
}

func relationshipPairs(relationships []*proto.Relationship) []string {
	out := make([]string, len(relationships))
	for i, relationship := range relationships {
		out[i] = relationship.GetRelation() + ":" + relationship.GetResource().GetType() + "/" + relationship.GetResource().GetId()
	}
	return out
}

func targetSummaries(targets []*proto.RelationshipTarget) []string {
	out := make([]string, len(targets))
	for i, target := range targets {
		out[i] = targetSummary(target)
	}
	return out
}

func targetSummary(target *proto.RelationshipTarget) string {
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

func expandContainsTarget(node *proto.ExpandNode, summary string) bool {
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
	resp, err := sess.client.Evaluate(sess.ctx, &proto.AccessEvaluationRequest{
		Subject:  &proto.Subject{Type: "subject", Id: subjectID},
		Action:   &proto.Action{Name: action},
		Resource: &proto.Resource{Type: "agent_session", Id: sessionID},
	})
	if err != nil {
		t.Fatalf("Evaluate(%s, %s, %s): %v", subjectID, action, sessionID, err)
	}
	if resp.GetAllowed() != want {
		t.Fatalf("Evaluate(%s, %s, %s) = %v, want %v", subjectID, action, sessionID, resp.GetAllowed(), want)
	}
}

func roundTripModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
	}
}

func expandedModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"user"}},
				{Name: "owner", SubjectTypes: []string{"user"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor", "owner"}},
				{Name: "write", Relations: []string{"editor", "owner"}},
				{Name: "admin", Relations: []string{"owner"}},
			},
		}},
	}
}

func rotatedModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{{
			Name: "document",
			Relations: []*proto.AuthorizationModelRelation{
				{Name: "viewer", SubjectTypes: []string{"user"}},
				{Name: "editor", SubjectTypes: []string{"group"}},
			},
			Actions: []*proto.AuthorizationModelAction{
				{Name: "read", Relations: []string{"viewer", "editor"}},
				{Name: "write", Relations: []string{"editor"}},
			},
		}},
	}
}

func zanzibarSessionModel() *proto.AuthorizationModel {
	return &proto.AuthorizationModel{
		Version: 1,
		ResourceTypes: []*proto.AuthorizationModelResourceType{
			{
				Name: "agent_session",
				Relations: []*proto.AuthorizationModelRelation{
					zanzibarSessionAccessRelation("viewer"),
					zanzibarSessionAccessRelation("editor"),
					{
						Name:           "parent",
						SubjectTypes:   []string{"subject"},
						AllowedTargets: []*proto.AuthorizationModelAllowedTarget{gestalt.NewAuthorizationModelResourceTypeTarget("agent_session")},
					},
				},
				Actions: []*proto.AuthorizationModelAction{
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

func zanzibarSessionAccessRelation(name string) *proto.AuthorizationModelRelation {
	children := []*proto.AuthorizationModelRewrite{gestalt.NewAuthorizationModelThisRewrite()}
	if name == "viewer" {
		children = append(children, gestalt.NewAuthorizationModelComputedUsersetRewrite("editor"))
	}
	children = append(children, gestalt.NewAuthorizationModelTupleToUsersetRewrite("parent", name))
	return &proto.AuthorizationModelRelation{
		Name:         name,
		SubjectTypes: []string{"subject"},
		AllowedTargets: []*proto.AuthorizationModelAllowedTarget{
			gestalt.NewAuthorizationModelSubjectTypeTarget("subject"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("everyone", "member"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("team", "member"),
			gestalt.NewAuthorizationModelSubjectSetAllowedTarget("slack_channel", "member"),
		},
		Rewrite: gestalt.NewAuthorizationModelUnionRewrite(children...),
	}
}

func membershipResourceType(name string) *proto.AuthorizationModelResourceType {
	return &proto.AuthorizationModelResourceType{
		Name: name,
		Relations: []*proto.AuthorizationModelRelation{{
			Name:         "member",
			SubjectTypes: []string{"subject"},
			AllowedTargets: []*proto.AuthorizationModelAllowedTarget{
				gestalt.NewAuthorizationModelSubjectTypeTarget("subject"),
			},
			Rewrite: gestalt.NewAuthorizationModelThisRewrite(),
		}},
		Actions: []*proto.AuthorizationModelAction{{
			Name:      "member",
			Relations: []string{"member"},
			Rewrite:   gestalt.NewAuthorizationModelComputedUsersetRewrite("member"),
		}},
	}
}
