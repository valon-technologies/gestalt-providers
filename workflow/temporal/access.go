package temporal

import (
	"fmt"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func validateExecutionReference(ref *proto.WorkflowExecutionReference) (*proto.WorkflowExecutionReference, error) {
	if ref == nil {
		return nil, fmt.Errorf("reference is required")
	}
	out := cloneExecutionReference(ref)
	out.Id = strings.TrimSpace(out.GetId())
	out.ProviderName = strings.TrimSpace(out.GetProviderName())
	out.SubjectId = strings.TrimSpace(out.GetSubjectId())
	out.SubjectKind = strings.TrimSpace(out.GetSubjectKind())
	out.DisplayName = strings.TrimSpace(out.GetDisplayName())
	out.AuthSource = strings.TrimSpace(out.GetAuthSource())
	out.CredentialSubjectId = strings.TrimSpace(out.GetCredentialSubjectId())
	out.CallerPluginName = strings.TrimSpace(out.GetCallerPluginName())
	target, err := normalizeTarget(out.GetTarget())
	if err != nil {
		return nil, err
	}
	out.Target = target.Target
	if out.GetId() == "" {
		return nil, fmt.Errorf("id is required")
	}
	if out.GetProviderName() == "" {
		return nil, fmt.Errorf("provider_name is required")
	}
	if out.GetSubjectId() == "" {
		return nil, fmt.Errorf("subject_id is required")
	}
	out.Permissions = clonePermissions(out.GetPermissions())
	if out.GetCreatedAt() != nil && !out.GetCreatedAt().IsValid() {
		out.CreatedAt = nil
	}
	if out.GetRevokedAt() != nil && !out.GetRevokedAt().IsValid() {
		out.RevokedAt = nil
	}
	return out, nil
}

func publishedEventExecutionReference(providerName, referenceKey string, trigger *proto.BoundWorkflowEventTrigger, actor *proto.WorkflowActor, createdAt time.Time) (*proto.WorkflowExecutionReference, error) {
	if !actorHasSubject(actor) || trigger == nil {
		return nil, nil
	}
	permissions, err := eventExecutionReferencePermissions(trigger)
	if err != nil {
		return nil, err
	}
	subjectID := strings.TrimSpace(actor.GetSubjectId())
	return &proto.WorkflowExecutionReference{
		Id:                  "event_ref:" + hashID(referenceKey),
		ProviderName:        strings.TrimSpace(providerName),
		Target:              cloneTarget(trigger.GetTarget()),
		SubjectId:           subjectID,
		SubjectKind:         strings.TrimSpace(actor.GetSubjectKind()),
		DisplayName:         strings.TrimSpace(actor.GetDisplayName()),
		AuthSource:          strings.TrimSpace(actor.GetAuthSource()),
		CredentialSubjectId: subjectID,
		Permissions:         permissions,
		CreatedAt:           gestalt.TimestampFromTime(createdAt.UTC()),
	}, nil
}

func eventExecutionReferencePermissions(trigger *proto.BoundWorkflowEventTrigger) ([]*proto.WorkflowAccessPermission, error) {
	permissions := executionReferencePermissionsForTarget(trigger.GetTarget())
	if !isConfigManagedActor(trigger.GetCreatedBy()) {
		return permissions, nil
	}
	extra, err := configuredEventRunPermissions(pluginTargetInput(trigger.GetTarget()))
	if err != nil {
		return nil, err
	}
	return mergeAccessPermissions(permissions, extra), nil
}

func executionReferencePermissionsForTarget(target *proto.BoundWorkflowTarget) []*proto.WorkflowAccessPermission {
	if target == nil {
		return nil
	}
	if agent := target.GetAgent(); agent != nil {
		set := map[string]map[string]struct{}{}
		for _, tool := range agent.GetToolRefs() {
			addPermission(set, strings.TrimSpace(tool.GetPlugin()), strings.TrimSpace(tool.GetOperation()))
		}
		if delivery := agent.GetOutputDelivery(); delivery != nil {
			addDeliveryPermission(set, delivery)
		}
		if delivery := agent.GetSessionReadyDelivery(); delivery != nil {
			addDeliveryPermission(set, delivery)
		}
		return permissionsFromSet(set)
	}
	if plugin := target.GetPlugin(); plugin != nil {
		pluginName := strings.TrimSpace(plugin.GetPluginName())
		if pluginName == "" {
			return nil
		}
		permission := &proto.WorkflowAccessPermission{Plugin: pluginName}
		if op := strings.TrimSpace(plugin.GetOperation()); op != "" {
			permission.Operations = []string{op}
		}
		return []*proto.WorkflowAccessPermission{permission}
	}
	return nil
}

func addDeliveryPermission(set map[string]map[string]struct{}, delivery *proto.WorkflowOutputDelivery) {
	if delivery == nil {
		return
	}
	deliveryTarget := delivery.GetTarget()
	addPermission(set, strings.TrimSpace(deliveryTarget.GetPluginName()), strings.TrimSpace(deliveryTarget.GetOperation()))
}

func configuredEventRunPermissions(input map[string]any) ([]*proto.WorkflowAccessPermission, error) {
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
	out := make([]*proto.WorkflowAccessPermission, 0, len(items))
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
		out = append(out, &proto.WorkflowAccessPermission{Plugin: pluginName, Operations: ops})
	}
	return out, nil
}

func pluginTargetInput(target *proto.BoundWorkflowTarget) map[string]any {
	if target == nil || target.GetPlugin() == nil || target.GetPlugin().GetInput() == nil {
		return nil
	}
	return target.GetPlugin().GetInput().AsMap()
}

func stringAny(value any) string {
	text, _ := value.(string)
	return text
}

func mergeAccessPermissions(groups ...[]*proto.WorkflowAccessPermission) []*proto.WorkflowAccessPermission {
	set := map[string]map[string]struct{}{}
	for _, group := range groups {
		for _, permission := range group {
			if permission == nil {
				continue
			}
			for _, op := range permission.GetOperations() {
				addPermission(set, permission.GetPlugin(), op)
			}
			if len(permission.GetOperations()) == 0 {
				addPermission(set, permission.GetPlugin(), "")
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

func permissionsFromSet(set map[string]map[string]struct{}) []*proto.WorkflowAccessPermission {
	plugins := make([]string, 0, len(set))
	for plugin := range set {
		plugins = append(plugins, plugin)
	}
	sort.Strings(plugins)
	out := make([]*proto.WorkflowAccessPermission, 0, len(plugins))
	for _, plugin := range plugins {
		ops := make([]string, 0, len(set[plugin]))
		for op := range set[plugin] {
			ops = append(ops, op)
		}
		sort.Strings(ops)
		out = append(out, &proto.WorkflowAccessPermission{Plugin: plugin, Operations: ops})
	}
	return out
}

func clonePermissions(in []*proto.WorkflowAccessPermission) []*proto.WorkflowAccessPermission {
	if len(in) == 0 {
		return nil
	}
	out := make([]*proto.WorkflowAccessPermission, 0, len(in))
	for _, permission := range in {
		if permission == nil {
			continue
		}
		ops := append([]string(nil), permission.GetOperations()...)
		out = append(out, &proto.WorkflowAccessPermission{
			Plugin:     strings.TrimSpace(permission.GetPlugin()),
			Operations: ops,
		})
	}
	return out
}
