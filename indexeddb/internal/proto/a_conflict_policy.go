package proto

import "os"

var _ = func() struct{} {
	// This private compatibility package mirrors the SDK's internal IndexedDB
	// wire descriptors while older provider internals are migrated to native
	// SDK interfaces.
	_ = os.Setenv("GOLANG_PROTOBUF_REGISTRATION_CONFLICT", "ignore")
	return struct{}{}
}()
