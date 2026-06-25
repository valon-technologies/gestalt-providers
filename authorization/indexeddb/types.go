package indexeddb

import gestalt "github.com/valon-technologies/gestalt/sdk/go"

type CheckAccessRequest = gestalt.CheckAccessRequest
type CheckAccessResponse = gestalt.CheckAccessResponse
type CheckAccessManyRequest = gestalt.CheckAccessManyRequest
type CheckAccessManyResponse = gestalt.CheckAccessManyResponse
type RelationshipFilter = gestalt.RelationshipFilter
type ListRelationshipsRequest = gestalt.ListRelationshipsRequest
type ListRelationshipsResponse = gestalt.ListRelationshipsResponse
type AddRelationshipRequest = gestalt.AddRelationshipRequest
type AddRelationshipResponse = gestalt.AddRelationshipResponse
type DeleteRelationshipRequest = gestalt.DeleteRelationshipRequest
type DeleteRelationshipResponse = gestalt.DeleteRelationshipResponse
type SetAuthorizationStateRequest = gestalt.SetAuthorizationStateRequest
type SetAuthorizationStateResponse = gestalt.SetAuthorizationStateResponse
type Subject = gestalt.AuthorizationSubject
type Action = gestalt.AuthorizationAction
type Resource = gestalt.AuthorizationResource

// Relationship is stored with a shape like:
//
//	{
//	  "tuple": {
//	    "target": {"subject": {"type": "subject", "id": "user:alice"}},
//	    "relation": "member",
//	    "resource": {"type": "group", "id": "engineering"}
//	  },
//	  "source_layer": "runtime"
//	}
//
// Subject-set targets represent inherited membership/grants:
//
//	{
//	  "tuple": {
//	    "target": {
//	      "subject_set": {
//	        "resource": {"type": "group", "id": "engineering"},
//	        "relation": "member"
//	      }
//	    },
//	    "relation": "reader",
//	    "resource": {"type": "repository", "id": "valon-tools"}
//	  },
//	  "source_layer": "static_config"
//	}
type Relationship = gestalt.Relationship
type RelationshipTuple = gestalt.RelationshipTuple
type RelationshipTarget = gestalt.RelationshipTarget
type SubjectSet = gestalt.SubjectSet
type RelationshipTargetType = gestalt.RelationshipTargetType
type SourceLayer = gestalt.SourceLayer

const (
	RelationshipTargetTypeUnspecified = gestalt.RelationshipTargetTypeUnspecified
	RelationshipTargetTypeSubject     = gestalt.RelationshipTargetTypeSubject
	RelationshipTargetTypeResource    = gestalt.RelationshipTargetTypeResource
	RelationshipTargetTypeSubjectSet  = gestalt.RelationshipTargetTypeSubjectSet
	SourceLayerUnspecified            = gestalt.SourceLayerUnspecified
	SourceLayerStaticConfig           = gestalt.SourceLayerStaticConfig
	SourceLayerRuntime                = gestalt.SourceLayerRuntime
)

// AuthorizationModel is currently stored with a shape like:
//
//	{
//	  "id": "model-1",
//	  "version": "v1",
//	  "resource_types": [
//	    {
//	      "name": "repository",
//	      "source_layer": "static_config",
//	      "relations": [
//	        {"name": "reader"},
//	        {"name": "maintainer"}
//	      ],
//	      "actions": [
//	        {"name": "read", "relations": ["reader", "maintainer"]},
//	        {"name": "administer", "relations": ["maintainer"]}
//	      ]
//	    },
//	    {
//	      "name": "group",
//	      "source_layer": "runtime",
//	      "relations": [
//	        {"name": "member"}
//	      ]
//	    }
//	  ]
//	}
type AuthorizationModel = gestalt.AuthorizationModel
type AuthorizationModelRef = gestalt.AuthorizationModelRef
type AuthorizationModelResourceType = gestalt.AuthorizationModelResourceType
type AuthorizationModelRelation = gestalt.ModelRelation
type AuthorizationModelAction = gestalt.ModelAction
type AuthorizationModelAllowedTarget = gestalt.ModelAllowedTarget
type SubjectSetType = gestalt.SubjectSetType
type GetActiveModelRefResponse = gestalt.GetActiveModelRefResponse
type SetActiveModelRequest = gestalt.SetActiveModelRequest
type SetActiveModelResponse = gestalt.SetActiveModelResponse
type ListActiveModelResourceTypesRequest = gestalt.ListActiveModelResourceTypesRequest
type AuthorizationModelResourceTypeFilter = gestalt.AuthorizationModelResourceTypeFilter
type ListActiveModelResourceTypesResponse = gestalt.ListActiveModelResourceTypesResponse
