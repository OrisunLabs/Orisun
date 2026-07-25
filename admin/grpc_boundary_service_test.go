package admin

import (
	"context"
	"testing"

	adminevents "github.com/OrisunLabs/Orisun/admin/events"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/goccy/go-json"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminBoundaryRPCsEmitDefinitionEvents(t *testing.T) {
	for _, test := range []struct {
		name                 string
		existedBeforeCatalog bool
		invoke               func(*AdminServiceServer) (*grpcapi.BoundaryInfo, error)
	}{
		{
			name: "new storage",
			invoke: func(server *AdminServiceServer) (*grpcapi.BoundaryInfo, error) {
				response, err := server.CreateBoundary(adminTestContext(), &grpcapi.CreateBoundaryRequest{
					Name:        "sales",
					Description: "Sales domain",
					Placement:   &grpcapi.BoundaryPlacementInput{Backend: "postgres", Namespace: "tenant_data"},
				})
				if err != nil {
					return nil, err
				}
				return response.Boundary, nil
			},
		},
		{
			name:                 "existing storage",
			existedBeforeCatalog: true,
			invoke: func(server *AdminServiceServer) (*grpcapi.BoundaryInfo, error) {
				response, err := server.CreateBoundary(adminTestContext(), &grpcapi.CreateBoundaryRequest{
					Name:                 "sales",
					Description:          "Sales domain",
					Placement:            &grpcapi.BoundaryPlacementInput{Backend: "postgres", Namespace: "tenant_data"},
					ExistedBeforeCatalog: true,
				})
				if err != nil {
					return nil, err
				}
				return response.Boundary, nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			saver := &capturingBoundarySaver{}
			server := NewGRPCAdminServerWithBoundaryCommands(nil, "orisun_admin", nil, nil, nil, nil, saver, emptyBoundaryRetriever{})
			boundary, err := test.invoke(server)
			if err != nil {
				t.Fatalf("RPC error = %v", err)
			}
			if len(saver.events) != 1 || saver.events[0].EventType != adminevents.EventTypeBoundaryCreated {
				t.Fatalf("saved events = %#v, want one %s", saver.events, adminevents.EventTypeBoundaryCreated)
			}
			if boundary.Name != "sales" || boundary.Placement.Namespace != "tenant_data" {
				t.Fatalf("boundary = %#v", boundary)
			}
			if boundary.Status != grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_PROVISIONING {
				t.Fatalf("status = %s", boundary.Status)
			}
			if boundary.ExistedBeforeCatalog != test.existedBeforeCatalog {
				t.Fatalf("existed_before_catalog = %t, want %t", boundary.ExistedBeforeCatalog, test.existedBeforeCatalog)
			}
			if boundary.DefinitionPosition.CommitPosition != 12 || boundary.DefinitionPosition.PreparePosition != 13 {
				t.Fatalf("definition position = %#v", boundary.DefinitionPosition)
			}
		})
	}
}

func TestCreateBoundaryRPCMapsAlreadyExists(t *testing.T) {
	retriever := emptyBoundaryRetriever{existing: true}
	server := NewGRPCAdminServerWithBoundaryCommands(nil, "orisun_admin", nil, nil, nil, nil, &capturingBoundarySaver{}, retriever)
	_, err := server.CreateBoundary(adminTestContext(), &grpcapi.CreateBoundaryRequest{
		Name:      "sales",
		Placement: &grpcapi.BoundaryPlacementInput{Backend: "postgres", Namespace: "tenant_data"},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateBoundary() code = %s, error = %v", status.Code(err), err)
	}
}

func TestCreateBoundaryRPCRequiresPlacement(t *testing.T) {
	server := NewGRPCAdminServerWithBoundaryCommands(nil, "orisun_admin", nil, nil, nil, nil, &capturingBoundarySaver{}, emptyBoundaryRetriever{})
	_, err := server.CreateBoundary(adminTestContext(), &grpcapi.CreateBoundaryRequest{Name: "sales"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateBoundary() code = %s, error = %v", status.Code(err), err)
	}
}

func TestAdminBoundaryCatalogRPCs(t *testing.T) {
	created, err := json.Marshal(boundaryevents.BoundaryCreated{
		Boundary:  "sales",
		Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "tenant_data"},
	})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := json.Marshal(boundaryevents.BoundaryActivated{Boundary: "sales"})
	if err != nil {
		t.Fatal(err)
	}
	retriever := emptyBoundaryRetriever{events: orisun.ReadEventBatch{
		{EventType: adminevents.EventTypeBoundaryCreated, Data: string(created), CommitPosition: 1, PreparePosition: 1},
		{EventType: adminevents.EventTypeBoundaryActivated, Data: string(activated), CommitPosition: 2, PreparePosition: 2},
	}}
	server := NewGRPCAdminServerWithBoundaryCommands(nil, "orisun_admin", nil, nil, nil, nil, &capturingBoundarySaver{}, retriever)

	list, err := server.ListBoundaries(operationsTestContext(), &grpcapi.ListBoundariesRequest{})
	if err != nil || len(list.Boundaries) != 1 || list.Boundaries[0].Status != grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE {
		t.Fatalf("ListBoundaries() = %#v, %v", list, err)
	}
	get, err := server.GetBoundary(operationsTestContext(), &grpcapi.GetBoundaryRequest{Name: "sales"})
	if err != nil || get.Boundary.Name != "sales" {
		t.Fatalf("GetBoundary() = %#v, %v", get, err)
	}
}

func TestGetBoundaryRPCMapsNotFound(t *testing.T) {
	server := NewGRPCAdminServerWithBoundaryCommands(nil, "orisun_admin", nil, nil, nil, nil, &capturingBoundarySaver{}, emptyBoundaryRetriever{})
	_, err := server.GetBoundary(operationsTestContext(), &grpcapi.GetBoundaryRequest{Name: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetBoundary() code = %s, error = %v", status.Code(err), err)
	}
}

type capturingBoundarySaver struct {
	events orisun.PreparedEventBatch
}

func (s *capturingBoundarySaver) SavePrepared(
	_ context.Context,
	events orisun.PreparedEventBatch,
	_ string,
	_ *orisun.Position,
	_ *orisun.Query,
) (string, int64, error) {
	s.events = append(orisun.PreparedEventBatch(nil), events...)
	return "12", 13, nil
}

type emptyBoundaryRetriever struct {
	existing bool
	events   orisun.ReadEventBatch
}

func (r emptyBoundaryRetriever) GetBatch(context.Context, *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	return r.events, nil
}

func (r emptyBoundaryRetriever) GetLatestByCriteria(_ context.Context, query orisun.LatestByCriteriaQuery) (orisun.LatestByCriteriaBatch, error) {
	matches := make([]orisun.LatestCriterionMatch, len(query.Criteria))
	if r.existing {
		matches[0] = orisun.LatestCriterionMatch{
			Found: true,
			Event: orisun.ReadEvent{EventType: adminevents.EventTypeBoundaryCreated},
		}
	}
	return orisun.LatestByCriteriaBatch{
		Matches:                matches,
		ContextCommitPosition:  -1,
		ContextPreparePosition: -1,
	}, nil
}
