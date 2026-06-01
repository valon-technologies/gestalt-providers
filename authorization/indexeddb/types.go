package indexeddb

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
)

// AuthorizationProvider is the proposed authorization provider shape.
type AuthorizationProvider interface {
	ListRelationships(context.Context, *ListRelationshipsRequest) (*ListRelationshipsResponse, error)
	AddRelationship(context.Context, *AddRelationshipRequest) (*AddRelationshipResponse, error)
	DeleteRelationship(context.Context, *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error)
	SetRelationships(context.Context, *SetRelationshipsRequest) (*SetRelationshipsResponse, error)
	GetActiveModelRef(context.Context, *emptypb.Empty) (*GetActiveModelRefResponse, error)
	SetActiveModel(context.Context, *SetActiveModelRequest) (*SetActiveModelResponse, error)
	ListActiveModelResourceTypes(context.Context, *ListActiveModelResourceTypesRequest) (*ListActiveModelResourceTypesResponse, error)

	CheckAccess(context.Context, *CheckAccessRequest) (*CheckAccessResponse, error)
	CheckAccessMany(context.Context, *CheckAccessManyRequest) (*CheckAccessManyResponse, error)
}

type CheckAccessRequest struct {
	Subject  *Subject
	Action   *Action
	Resource *Resource
}

type CheckAccessResponse struct {
	Allowed bool
	ModelID string
}

type CheckAccessManyRequest struct {
	Requests []*CheckAccessRequest
}

type CheckAccessManyResponse struct {
	Decisions []*CheckAccessResponse
}

type ListRelationshipsRequest struct {
	Filter    *RelationshipFilter
	PageSize  int32
	PageToken string
}

type RelationshipFilter struct {
	Target           *RelationshipTarget
	Relation         string
	Resource         *Resource
	TargetType       RelationshipTargetType
	TargetEntityType string
	ResourceType     string
	SourceLayer      SourceLayer
}

type ListRelationshipsResponse struct {
	Relationships []*Relationship
	NextPageToken string
}

type AddRelationshipRequest struct {
	Relationship *Relationship
}

type AddRelationshipResponse struct {
	Relationship *Relationship
}

type DeleteRelationshipRequest struct {
	RelationshipTuple *RelationshipTuple
}

type DeleteRelationshipResponse struct{}

type SetRelationshipsRequest struct {
	Relationships []*Relationship
}

type SetRelationshipsResponse struct {
	Relationships []*Relationship
}

type Subject struct {
	Type       string         `json:"type"`
	Id         string         `json:"id"`
	Properties map[string]any `json:"properties,omitempty"`
}

type Action struct {
	Name       string         `json:"name"`
	Properties map[string]any `json:"properties,omitempty"`
}

type Resource struct {
	Type       string         `json:"type"`
	Id         string         `json:"id"`
	Properties map[string]any `json:"properties,omitempty"`
}

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
type Relationship struct {
	Tuple       *RelationshipTuple `json:"tuple"`
	Properties  map[string]any     `json:"properties,omitempty"`
	SourceLayer SourceLayer        `json:"source_layer"`
}

type RelationshipTuple struct {
	Target   *RelationshipTarget `json:"target"`
	Relation string              `json:"relation"`
	Resource *Resource           `json:"resource"`
}

type RelationshipTarget struct {
	Subject    *Subject    `json:"subject,omitempty"`
	Resource   *Resource   `json:"resource,omitempty"`
	SubjectSet *SubjectSet `json:"subject_set,omitempty"`
}

type SubjectSet struct {
	Resource *Resource `json:"resource"`
	Relation string    `json:"relation"`
}

type RelationshipTargetType int32

const (
	RelationshipTargetTypeUnspecified RelationshipTargetType = 0
	RelationshipTargetTypeSubject     RelationshipTargetType = 1
	RelationshipTargetTypeResource    RelationshipTargetType = 2
	RelationshipTargetTypeSubjectSet  RelationshipTargetType = 3
)

type SourceLayer int32

const (
	SourceLayerUnspecified  SourceLayer = 0
	SourceLayerStaticConfig SourceLayer = 1
	SourceLayerRuntime      SourceLayer = 2
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
type AuthorizationModel struct {
	Id            string                            `json:"id"`
	Version       string                            `json:"version"`
	ResourceTypes []*AuthorizationModelResourceType `json:"resource_types"`
}

type AuthorizationModelRef struct {
	Id        string    `json:"id"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

type AuthorizationModelResourceType struct {
	Name        string                        `json:"name"`
	Relations   []*AuthorizationModelRelation `json:"relations,omitempty"`
	Actions     []*AuthorizationModelAction   `json:"actions,omitempty"`
	SourceLayer SourceLayer                   `json:"source_layer"`
}

type AuthorizationModelRelation struct {
	Name           string                             `json:"name"`
	AllowedTargets []*AuthorizationModelAllowedTarget `json:"allowed_targets,omitempty"`
}

type AuthorizationModelAction struct {
	Name      string   `json:"name"`
	Relations []string `json:"relations,omitempty"`
}

type AuthorizationModelAllowedTarget struct {
	SubjectType    string          `json:"subject_type,omitempty"`
	ResourceType   string          `json:"resource_type,omitempty"`
	SubjectSetType *SubjectSetType `json:"subject_set_type,omitempty"`
}

type SubjectSetType struct {
	ResourceType string `json:"resource_type"`
	Relation     string `json:"relation"`
}

type GetActiveModelRefResponse struct {
	Model *AuthorizationModelRef
}

type SetActiveModelRequest struct {
	Model *AuthorizationModel
}

type SetActiveModelResponse struct {
	Model *AuthorizationModelRef
}

type ListActiveModelResourceTypesRequest struct {
	ModelID string
	Filter  *AuthorizationModelResourceTypeFilter
}

type AuthorizationModelResourceTypeFilter struct {
	Name        string
	SourceLayer SourceLayer
}

type ListActiveModelResourceTypesResponse struct {
	ResourceTypes []*AuthorizationModelResourceType
}
