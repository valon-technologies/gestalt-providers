// Browser stub for @connectrpc/connect-node. The gestalt SDK's ./client export
// statically reaches a gRPC transport factory that dynamically imports
// connect-node (Node-only: node:http2, node:zlib, node:util). The console only
// uses the REST transport, so the gRPC path is never taken at runtime; this
// stub lets the production browser bundle build without pulling Node builtins.
// Calling these stubs throws, since they should never be invoked.
export function createGrpcTransport(): never {
  throw new Error(
    "createGrpcTransport is not available in the browser; use the REST transport",
  );
}

export class Http2SessionManager {
  constructor() {
    throw new Error(
      "Http2SessionManager is not available in the browser; use the REST transport",
    );
  }
}
