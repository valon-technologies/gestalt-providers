package temporal

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
)

func TestPromotionRoutingConverged(t *testing.T) {
	snapshot := workerDeploymentRoutingSnapshot{
		CurrentBuildID:     "revision-1",
		RoutingUpdateState: enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED,
	}
	if !promotionRoutingConverged(snapshot, "revision-1") {
		t.Fatal("expected completed routing to converge")
	}
}

func TestPromotionRoutingNotConvergedWhileInProgress(t *testing.T) {
	snapshot := workerDeploymentRoutingSnapshot{
		CurrentBuildID:     "revision-1",
		RoutingUpdateState: enumspb.ROUTING_CONFIG_UPDATE_STATE_IN_PROGRESS,
	}
	if promotionRoutingConverged(snapshot, "revision-1") {
		t.Fatal("expected in-progress routing to remain unconverged")
	}
}
