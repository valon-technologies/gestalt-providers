# IndexedDB Authorization

Stub authorization provider for iterating on a revised authorization provider
interface.

The proposed provider shape is:

```proto
service AuthorizationProvider {
  rpc CheckAccess(CheckAccessRequest) returns (CheckAccessResponse);
  rpc CheckAccessMany(CheckAccessManyRequest) returns (CheckAccessManyResponse);

  rpc ListRelationships(ListRelationshipsRequest) returns (ListRelationshipsResponse);
  rpc AddRelationship(AddRelationshipRequest) returns (AddRelationshipResponse);
  rpc DeleteRelationship(DeleteRelationshipRequest) returns (DeleteRelationshipResponse);
  rpc SetRelationships(SetRelationshipsRequest) returns (SetRelationshipsResponse);

  rpc GetActiveModelRef(google.protobuf.Empty) returns (GetActiveModelRefResponse);
  rpc SetActiveModel(SetActiveModelRequest) returns (SetActiveModelResponse);
  rpc ListActiveModelResourceTypes(ListActiveModelResourceTypesRequest) returns (ListActiveModelResourceTypesResponse);
}
```

All provider methods currently return `UNIMPLEMENTED`.
