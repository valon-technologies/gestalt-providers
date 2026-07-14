package temporal

import (
	"encoding/base64"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	sdktemporal "go.temporal.io/sdk/temporal"
)

const (
	temporalRunWorkflowType = "TemporalRun"
)

var (
	searchAttrScopeID              = sdktemporal.NewSearchAttributeKeyKeyword("GestaltScopeId")
	searchAttrProviderName         = sdktemporal.NewSearchAttributeKeyKeyword("GestaltProviderName")
	searchAttrRunStatus            = sdktemporal.NewSearchAttributeKeyKeyword("GestaltRunStatus")
	searchAttrCreatedBySubjectHash = sdktemporal.NewSearchAttributeKeyKeyword("GestaltCreatedBySubjectHash")
	searchAttrDefinitionID         = sdktemporal.NewSearchAttributeKeyKeyword("GestaltDefinitionId")
	searchAttrDefinitionGeneration = sdktemporal.NewSearchAttributeKeyInt64("GestaltDefinitionGeneration")
	searchAttrWorkflowKeyHash      = sdktemporal.NewSearchAttributeKeyKeyword("GestaltWorkflowKeyHash")
	searchAttrOwnerKeyHash         = sdktemporal.NewSearchAttributeKeyKeyword("GestaltOwnerKeyHash")
	searchAttrTargetApps           = sdktemporal.NewSearchAttributeKeyKeywordList("GestaltTargetApps")
)

func workflowRunSearchAttributes(scopeID string, run *gestalt.WorkflowRun) sdktemporal.SearchAttributes {
	return sdktemporal.NewSearchAttributes(workflowRunSearchAttributeUpdates(scopeID, run)...)
}

func workflowRunSearchAttributeUpdates(scopeID string, run *gestalt.WorkflowRun) []sdktemporal.SearchAttributeUpdate {
	if run == nil {
		return nil
	}
	updates := []sdktemporal.SearchAttributeUpdate{}
	if value := strings.TrimSpace(scopeID); value != "" {
		updates = append(updates, searchAttrScopeID.ValueSet(value))
	}
	if value := strings.TrimSpace(run.ProviderName); value != "" {
		updates = append(updates, searchAttrProviderName.ValueSet(value))
	}
	if value := workflowRunStatusName(run.Status); value != "" && value != "unspecified" {
		updates = append(updates, searchAttrRunStatus.ValueSet(value))
	}
	if value := searchAttributeHash(run.CreatedBy); value != "" {
		updates = append(updates, searchAttrCreatedBySubjectHash.ValueSet(value))
	}
	if value := strings.TrimSpace(run.DefinitionID); value != "" {
		updates = append(updates, searchAttrDefinitionID.ValueSet(value))
	}
	if run.DefinitionGeneration > 0 {
		updates = append(updates, searchAttrDefinitionGeneration.ValueSet(run.DefinitionGeneration))
	}
	if value := searchAttributeHash(run.WorkflowKey); value != "" {
		updates = append(updates, searchAttrWorkflowKeyHash.ValueSet(value))
	}
	if value := searchAttributeHash(targetOwnerKeyInput(run.Target)); value != "" {
		updates = append(updates, searchAttrOwnerKeyHash.ValueSet(value))
	}
	if apps := workflowTargetAppNames(run.Target); len(apps) > 0 {
		updates = append(updates, searchAttrTargetApps.ValueSet(apps))
	}
	return updates
}

func workflowRunSearchAttributesFromInput(input runWorkflowInput, status gestalt.WorkflowRunStatus) sdktemporal.SearchAttributes {
	run := &gestalt.WorkflowRun{
		Status:               status,
		Target:               input.Target,
		CreatedBy:            input.CreatedBy,
		WorkflowKey:          strings.TrimSpace(input.WorkflowKey),
		ProviderName:         strings.TrimSpace(input.ProviderName),
		DefinitionID:         strings.TrimSpace(input.DefinitionID),
		DefinitionGeneration: input.DefinitionGeneration,
	}
	return workflowRunSearchAttributes(input.ScopeID, run)
}

func workflowTargetAppNames(target *gestalt.BoundWorkflowTarget) []string {
	if target == nil {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, step := range target.Steps {
		if step.App == nil {
			continue
		}
		name := strings.TrimSpace(step.App.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func searchAttributeHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return hashID(value)
}

func encodeTemporalListPageToken(token []byte) string {
	if len(token) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(token)
}

func decodeTemporalListPageToken(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	return base64.RawURLEncoding.DecodeString(raw)
}

func temporalVisibilityQuote(value string) string {
	return "'" + strings.ReplaceAll(strings.TrimSpace(value), "'", "\\'") + "'"
}
