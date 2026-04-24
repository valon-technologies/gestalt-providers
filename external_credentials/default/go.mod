module github.com/valon-technologies/gestalt-providers/external_credentials/default

go 1.26

require (
	github.com/google/uuid v1.6.0
	github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb v0.0.0-00010101000000-000000000000
	github.com/valon-technologies/gestalt/sdk/go v0.0.0-00010101000000-000000000000
	golang.org/x/crypto v0.48.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.1 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/microsoft/go-mssqldb v1.9.8 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/valon-technologies/gestalt-providers/indexeddb/internal v0.0.0-00010101000000-000000000000 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	modernc.org/libc v1.70.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.48.2 // indirect
)

replace github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb => ../../indexeddb/relationaldb

replace github.com/valon-technologies/gestalt-providers/indexeddb/internal => ../../indexeddb/internal

replace github.com/valon-technologies/gestalt-providers/indexeddb/contracttest => ../../indexeddb/contracttest

replace github.com/valon-technologies/gestalt/sdk/go => ../../../gestalt/sdk/go
