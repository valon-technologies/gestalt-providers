package temporal

import (
	"context"
	"fmt"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type workerDeploymentRoutingSnapshot struct {
	ConflictToken      []byte
	CurrentBuildID     string
	RoutingUpdateState enumspb.RoutingConfigUpdateState
}

type workerDeploymentRoutingReader interface {
	ReadRoutingSnapshot(ctx context.Context, deploymentName string) (workerDeploymentRoutingSnapshot, error)
}

type workflowServiceRoutingReader struct {
	namespace string
	apiKey    string
	conn      *grpc.ClientConn
	service   workflowservicepb.WorkflowServiceClient
}

func newWorkflowServiceRoutingReader(cfg config) (*workflowServiceRoutingReader, error) {
	conn, err := grpc.NewClient(
		cfg.HostPort,
		grpc.WithTransportCredentials(credentials.NewTLS(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial temporal workflow service: %w", err)
	}
	return &workflowServiceRoutingReader{
		namespace: cfg.Namespace,
		apiKey:    cfg.APIKey,
		conn:      conn,
		service:   workflowservicepb.NewWorkflowServiceClient(conn),
	}, nil
}

func (r *workflowServiceRoutingReader) Close() error {
	if r == nil || r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

func (r *workflowServiceRoutingReader) ReadRoutingSnapshot(
	ctx context.Context,
	deploymentName string,
) (workerDeploymentRoutingSnapshot, error) {
	if r == nil || r.service == nil {
		return workerDeploymentRoutingSnapshot{}, fmt.Errorf("workflow service routing reader is not configured")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+r.apiKey)
	resp, err := r.service.DescribeWorkerDeployment(ctx, &workflowservicepb.DescribeWorkerDeploymentRequest{
		Namespace:      r.namespace,
		DeploymentName: deploymentName,
	})
	if err != nil {
		return workerDeploymentRoutingSnapshot{}, err
	}
	return routingSnapshotFromProto(resp.GetConflictToken(), resp.GetWorkerDeploymentInfo()), nil
}

func routingSnapshotFromProto(
	conflictToken []byte,
	info *deploymentpb.WorkerDeploymentInfo,
) workerDeploymentRoutingSnapshot {
	snapshot := workerDeploymentRoutingSnapshot{
		ConflictToken:      conflictToken,
		RoutingUpdateState: enumspb.ROUTING_CONFIG_UPDATE_STATE_UNSPECIFIED,
	}
	if info == nil {
		return snapshot
	}
	snapshot.RoutingUpdateState = info.GetRoutingConfigUpdateState()
	if info.RoutingConfig != nil && info.RoutingConfig.CurrentDeploymentVersion != nil {
		snapshot.CurrentBuildID = info.RoutingConfig.CurrentDeploymentVersion.BuildId
	}
	return snapshot
}

func promotionRoutingConverged(snapshot workerDeploymentRoutingSnapshot, buildID string) bool {
	if snapshot.CurrentBuildID != buildID {
		return false
	}
	return snapshot.RoutingUpdateState == enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED
}
