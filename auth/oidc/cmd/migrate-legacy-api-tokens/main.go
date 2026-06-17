package main

import (
	"os"

	"github.com/valon-technologies/gestalt-providers/auth/oidc"
)

func main() {
	os.Exit(oidc.RunLegacyAPITokenMigrationCLI(os.Args[1:]))
}
