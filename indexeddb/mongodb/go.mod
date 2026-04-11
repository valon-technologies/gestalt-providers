module github.com/valon-technologies/gestalt-providers/indexeddb/mongodb

go 1.26

require (
	github.com/valon-technologies/gestalt/sdk/go v0.0.0-00010101000000-000000000000
	go.mongodb.org/mongo-driver/v2 v2.5.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.17.6 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/valon-technologies/gestalt/sdk/go => ../../../gestalt/sdk/go
