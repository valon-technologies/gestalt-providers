# IndexedDB Authorization

IndexedDB authorization provider for the Gestalt authorization provider
interface.

The proposed provider shape is:

```proto
service Authorization {
  rpc CheckAccess(CheckAccessRequest) returns (CheckAccessResponse);
  rpc CheckAccessMany(CheckAccessManyRequest) returns (CheckAccessManyResponse);

  rpc ListRelationships(ListRelationshipsRequest) returns (ListRelationshipsResponse);
  rpc AddRelationship(AddRelationshipRequest) returns (AddRelationshipResponse);
  rpc DeleteRelationship(DeleteRelationshipRequest) returns (DeleteRelationshipResponse);

  rpc SetAuthorizationState(SetAuthorizationStateRequest) returns (SetAuthorizationStateResponse);

  rpc GetActiveModelRef() returns (GetActiveModelRefResponse);
  rpc SetActiveModel(SetActiveModelRequest) returns (SetActiveModelResponse);
  rpc ListActiveModelResourceTypes(ListActiveModelResourceTypesRequest) returns (ListActiveModelResourceTypesResponse);
}
```
