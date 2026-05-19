package temporal

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func validateExecutionReferenceInput(ref *gestalt.WorkflowExecutionReference) (*gestalt.WorkflowExecutionReference, error) {
	if ref == nil {
		return nil, fmt.Errorf("reference is required")
	}
	out := *ref
	out.ID = strings.TrimSpace(out.ID)
	out.ProviderName = strings.TrimSpace(out.ProviderName)
	out.SubjectID = strings.TrimSpace(out.SubjectID)
	out.SubjectKind = strings.TrimSpace(out.SubjectKind)
	out.DisplayName = strings.TrimSpace(out.DisplayName)
	out.AuthSource = strings.TrimSpace(out.AuthSource)
	out.CredentialSubjectID = strings.TrimSpace(out.CredentialSubjectID)
	out.CallerPluginName = strings.TrimSpace(out.CallerPluginName)
	out.SourceDefinitionID = strings.TrimSpace(out.SourceDefinitionID)
	out.TargetDigest = strings.TrimSpace(out.TargetDigest)
	out.ProviderPlanDigest = strings.TrimSpace(out.ProviderPlanDigest)
	out.PermissionsDigest = strings.TrimSpace(out.PermissionsDigest)
	out.SemanticsVersion = strings.TrimSpace(out.SemanticsVersion)
	out.Seal = strings.TrimSpace(out.Seal)
	target, err := normalizeTarget(out.Target)
	if err != nil {
		return nil, err
	}
	out.Target = target.Target
	if out.ID == "" {
		return nil, fmt.Errorf("id is required")
	}
	if out.ProviderName == "" {
		return nil, fmt.Errorf("provider_name is required")
	}
	if out.SubjectID == "" {
		return nil, fmt.Errorf("subject_id is required")
	}
	out.Permissions = clonePermissionInputs(out.Permissions)
	if out.RevokedAt != nil && out.RevokedAt.IsZero() {
		out.RevokedAt = nil
	}
	return &out, nil
}

func publishedEventExecutionReference(providerName, referenceKey string, trigger *gestalt.BoundWorkflowEventTrigger, actor *gestalt.WorkflowActor, createdAt time.Time) (*gestalt.WorkflowExecutionReference, error) {
	if !actorHasSubject(actor) || trigger == nil {
		return nil, nil
	}
	permissions, err := eventExecutionReferencePermissions(trigger)
	if err != nil {
		return nil, err
	}
	subjectID := strings.TrimSpace(actor.SubjectID)
	ref := &gestalt.WorkflowExecutionReference{
		ID:                  "event_ref:" + hashID(referenceKey),
		ProviderName:        strings.TrimSpace(providerName),
		Target:              trigger.Target,
		SubjectID:           subjectID,
		CredentialSubjectID: subjectID,
		Permissions:         permissions,
		CreatedAt:           createdAt.UTC(),
		SubjectKind:         strings.TrimSpace(actor.SubjectKind),
		DisplayName:         strings.TrimSpace(actor.DisplayName),
		AuthSource:          strings.TrimSpace(actor.AuthSource),
	}
	return ref, nil
}

func eventExecutionReferencePermissions(trigger *gestalt.BoundWorkflowEventTrigger) ([]gestalt.WorkflowAccessPermission, error) {
	permissions := executionReferencePermissionsForTarget(trigger.Target)
	if !isConfigManagedActorInput(trigger.CreatedBy) {
		return permissions, nil
	}
	extra, err := configuredEventRunPermissions(pluginTargetInput(trigger.Target))
	if err != nil {
		return nil, err
	}
	return mergeAccessPermissions(permissions, extra), nil
}

func executionReferencePermissionsForTarget(target *gestalt.BoundWorkflowTarget) []gestalt.WorkflowAccessPermission {
	if target == nil {
		return nil
	}
	if len(target.Steps) > 0 {
		actions := make([]string, 0, len(target.Steps)*2)
		for _, step := range target.Steps {
			if step.Plugin != nil {
				actions = append(actions, workflowStepActionID(step.ID, workflowStepPluginActionSuffix))
			}
			if step.Agent != nil {
				actions = append(actions, workflowStepActionID(step.ID, workflowStepAgentActionSuffix))
			}
			if step.OutputDelivery != nil {
				actions = append(actions, workflowStepActionID(step.ID, workflowStepDeliveryActionSuffix))
			}
		}
		sort.Strings(actions)
		return []gestalt.WorkflowAccessPermission{{
			Plugin:  "__gestalt.workflow.step_actions__",
			Actions: actions,
		}}
	}
	if agent := target.Agent; agent != nil {
		set := map[string]map[string]struct{}{}
		for _, tool := range agent.ToolRefs {
			addPermission(set, strings.TrimSpace(tool.Plugin), strings.TrimSpace(tool.Operation))
		}
		if delivery := agent.OutputDelivery; delivery != nil {
			addDeliveryPermission(set, delivery)
		}
		if delivery := agent.SessionReadyDelivery; delivery != nil {
			addDeliveryPermission(set, delivery)
		}
		return permissionsFromSet(set)
	}
	if plugin := target.Plugin; plugin != nil {
		pluginName := strings.TrimSpace(plugin.PluginName)
		if pluginName == "" {
			return nil
		}
		permission := gestalt.WorkflowAccessPermission{Plugin: pluginName}
		if op := strings.TrimSpace(plugin.Operation); op != "" {
			permission.Operations = []string{op}
		}
		return []gestalt.WorkflowAccessPermission{permission}
	}
	return nil
}

func addDeliveryPermission(set map[string]map[string]struct{}, delivery *gestalt.WorkflowOutputDelivery) {
	if delivery == nil {
		return
	}
	deliveryTarget := delivery.Target
	if deliveryTarget == nil {
		return
	}
	addPermission(set, strings.TrimSpace(deliveryTarget.PluginName), strings.TrimSpace(deliveryTarget.Operation))
}

func configuredEventRunPermissions(input map[string]any) ([]gestalt.WorkflowAccessPermission, error) {
	rawGestalt, ok := input[gestaltInputKey]
	if !ok || rawGestalt == nil {
		return nil, nil
	}
	gestaltConfig, ok := rawGestalt.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", gestaltInputKey)
	}
	rawPermissions, ok := gestaltConfig[eventRunPermissionsKey]
	if !ok || rawPermissions == nil {
		return nil, nil
	}
	items, ok := rawPermissions.([]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s must be a list", gestaltInputKey, eventRunPermissionsKey)
	}
	out := make([]gestalt.WorkflowAccessPermission, 0, len(items))
	for i, item := range items {
		value, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.%s[%d] must be an object", gestaltInputKey, eventRunPermissionsKey, i)
		}
		pluginName := strings.TrimSpace(stringAny(value["plugin"]))
		if pluginName == "" {
			return nil, fmt.Errorf("%s.%s[%d].plugin is required", gestaltInputKey, eventRunPermissionsKey, i)
		}
		opsRaw, ok := value["operations"].([]any)
		if !ok || len(opsRaw) == 0 {
			return nil, fmt.Errorf("%s.%s[%d].operations is required", gestaltInputKey, eventRunPermissionsKey, i)
		}
		ops := make([]string, 0, len(opsRaw))
		for _, raw := range opsRaw {
			op := strings.TrimSpace(stringAny(raw))
			if op != "" {
				ops = append(ops, op)
			}
		}
		if len(ops) == 0 {
			return nil, fmt.Errorf("%s.%s[%d].operations is required", gestaltInputKey, eventRunPermissionsKey, i)
		}
		out = append(out, gestalt.WorkflowAccessPermission{Plugin: pluginName, Operations: ops})
	}
	return out, nil
}

func pluginTargetInput(target *gestalt.BoundWorkflowTarget) map[string]any {
	if target == nil || target.Plugin == nil || target.Plugin.Input == nil {
		return nil
	}
	if value, ok := target.Plugin.Input.(map[string]any); ok {
		return value
	}
	raw, err := json.Marshal(target.Plugin.Input)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func stringAny(value any) string {
	text, _ := value.(string)
	return text
}

func mergeAccessPermissions(groups ...[]gestalt.WorkflowAccessPermission) []gestalt.WorkflowAccessPermission {
	set := map[string]map[string]struct{}{}
	for _, group := range groups {
		for _, permission := range group {
			for _, op := range permission.Operations {
				addPermission(set, permission.Plugin, op)
			}
			if len(permission.Operations) == 0 {
				addPermission(set, permission.Plugin, "")
			}
		}
	}
	return permissionsFromSet(set)
}

func addPermission(set map[string]map[string]struct{}, pluginName, operation string) {
	pluginName = strings.TrimSpace(pluginName)
	operation = strings.TrimSpace(operation)
	if pluginName == "" {
		return
	}
	ops := set[pluginName]
	if ops == nil {
		ops = map[string]struct{}{}
		set[pluginName] = ops
	}
	if operation != "" {
		ops[operation] = struct{}{}
	}
}

func permissionsFromSet(set map[string]map[string]struct{}) []gestalt.WorkflowAccessPermission {
	plugins := make([]string, 0, len(set))
	for plugin := range set {
		plugins = append(plugins, plugin)
	}
	sort.Strings(plugins)
	out := make([]gestalt.WorkflowAccessPermission, 0, len(plugins))
	for _, plugin := range plugins {
		ops := make([]string, 0, len(set[plugin]))
		for op := range set[plugin] {
			ops = append(ops, op)
		}
		sort.Strings(ops)
		out = append(out, gestalt.WorkflowAccessPermission{Plugin: plugin, Operations: ops})
	}
	return out
}

func clonePermissionInputs(in []gestalt.WorkflowAccessPermission) []gestalt.WorkflowAccessPermission {
	if len(in) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowAccessPermission, 0, len(in))
	for _, permission := range in {
		out = append(out, gestalt.WorkflowAccessPermission{
			Plugin:     strings.TrimSpace(permission.Plugin),
			Operations: append([]string(nil), permission.Operations...),
		})
	}
	return out
}
