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
	out.CallerAppName = strings.TrimSpace(out.CallerAppName)
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
	extra, err := configuredEventRunPermissions(appTargetInput(trigger.Target))
	if err != nil {
		return nil, err
	}
	return mergeAccessPermissions(permissions, extra), nil
}

func executionReferencePermissionsForTarget(target *gestalt.BoundWorkflowTarget) []gestalt.WorkflowAccessPermission {
	if target == nil {
		return nil
	}
	set := map[string]map[string]struct{}{}
	for _, step := range target.Steps {
		if step.App != nil {
			addPermission(set, strings.TrimSpace(step.App.Name), strings.TrimSpace(step.App.Operation))
		}
		if step.Agent == nil {
			continue
		}
		for _, tool := range step.Agent.Tools {
			addPermission(set, strings.TrimSpace(tool.App), strings.TrimSpace(tool.Operation))
		}
	}
	return permissionsFromSet(set)
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
		appName := strings.TrimSpace(stringAny(value["app"]))
		if appName == "" {
			appName = strings.TrimSpace(stringAny(value["plugin"]))
		}
		if appName == "" {
			return nil, fmt.Errorf("%s.%s[%d].app is required", gestaltInputKey, eventRunPermissionsKey, i)
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
		out = append(out, gestalt.WorkflowAccessPermission{App: appName, Operations: ops})
	}
	return out, nil
}

func appTargetInput(target *gestalt.BoundWorkflowTarget) map[string]any {
	app := firstWorkflowAppStep(target)
	if app == nil {
		return nil
	}
	return workflowValueToMap(app.Input)
}

func workflowValueToMap(value gestalt.WorkflowValue) map[string]any {
	switch {
	case value.Object != nil:
		out := make(map[string]any, len(value.Object))
		for key, nested := range value.Object {
			out[key] = workflowValueToAny(nested)
		}
		return out
	case value.LiteralSet:
		if m, ok := value.Literal.(map[string]any); ok {
			return m
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func workflowValueToAny(value gestalt.WorkflowValue) any {
	switch {
	case value.LiteralSet:
		return value.Literal
	case value.Object != nil:
		out := make(map[string]any, len(value.Object))
		for key, nested := range value.Object {
			out[key] = workflowValueToAny(nested)
		}
		return out
	case value.Array != nil:
		out := make([]any, 0, len(value.Array))
		for _, nested := range value.Array {
			out = append(out, workflowValueToAny(nested))
		}
		return out
	default:
		return nil
	}
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
				addPermission(set, permission.App, op)
			}
			if len(permission.Operations) == 0 {
				addPermission(set, permission.App, "")
			}
		}
	}
	return permissionsFromSet(set)
}

func addPermission(set map[string]map[string]struct{}, appName, operation string) {
	appName = strings.TrimSpace(appName)
	operation = strings.TrimSpace(operation)
	if appName == "" {
		return
	}
	ops := set[appName]
	if ops == nil {
		ops = map[string]struct{}{}
		set[appName] = ops
	}
	if operation != "" {
		ops[operation] = struct{}{}
	}
}

func permissionsFromSet(set map[string]map[string]struct{}) []gestalt.WorkflowAccessPermission {
	apps := make([]string, 0, len(set))
	for app := range set {
		apps = append(apps, app)
	}
	sort.Strings(apps)
	out := make([]gestalt.WorkflowAccessPermission, 0, len(apps))
	for _, app := range apps {
		ops := make([]string, 0, len(set[app]))
		for op := range set[app] {
			ops = append(ops, op)
		}
		sort.Strings(ops)
		out = append(out, gestalt.WorkflowAccessPermission{App: app, Operations: ops})
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
			App:        strings.TrimSpace(permission.App),
			Operations: append([]string(nil), permission.Operations...),
		})
	}
	return out
}
