# BigQuery Python Protobuf 7 Blockers

This note records the current blocker for a Python source implementation of the
BigQuery plugin.

## Current state

The Python SDK now expects:

- `protobuf>=7.34.1,<8`

The current BigQuery Python port prototype resolves this Google client stack:

- `google-cloud-bigquery==3.41.0`
- `google-api-core==2.30.2`
- `grpcio-status==1.80.0`
- `protobuf==6.31.1`

## Why the resolver stays on protobuf 6

The important constraints are:

- `grpcio-status==1.80.0` requires `protobuf>=6.31.1,<7.0.0`
- `google-cloud-bigquery==3.41.0` depends on `google-api-core[grpc]`
- `google-api-core==2.30.2` pulls in `grpcio-status` through its `grpc` extra

That means the current Google gRPC transport path still caps the environment
below protobuf 7.

There is a second related constraint in the current BigQuery client metadata:

- `google-cloud-bigquery==3.41.0` publishes a `bigquery-v2` extra with
  `protobuf<7.0.0`

## Implication for the plugin

Today, the BigQuery Python plugin cannot depend on both:

- the current Gestalt Python SDK runtime floor of `protobuf>=7.34.1`
- the current `google-cloud-bigquery` dependency chain

without a version conflict.

## Ways forward

One of these needs to happen before the BigQuery Python port can move to the
protobuf 7 SDK line:

1. Upstream support lands in the Google stack.
   Specifically, `grpcio-status` and the BigQuery client path need to accept
   protobuf 7.
2. The plugin stops depending on the Google gRPC client stack.
   A direct REST implementation against the BigQuery HTTP API would avoid the
   current `grpcio-status` constraint entirely.
3. The SDK runtime floor changes again.
   That would re-open protobuf 6 compatibility, but it would undo the SDK-side
   protobuf 7 move and is not the desired direction.

## Recommendation

Treat this as an upstream dependency blocker, not a local lockfile bug.

If the goal is to ship a BigQuery Python plugin before the Google client stack
accepts protobuf 7, the realistic fallback is to use the REST API directly
instead of `google-cloud-bigquery`.
