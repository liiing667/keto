package check_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	rts "github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2"
	"github.com/ory/keto/internal/check"
	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/ketoapi"
)

type reczzCountingSnapshotManager struct {
	relationtuple.Manager
	opens atomic.Int32
}

func (m *reczzCountingSnapshotManager) WithLatestSnapshot(ctx context.Context, f func(context.Context) error) error {
	m.opens.Add(1)
	return m.Manager.WithLatestSnapshot(ctx, f)
}

type reczzSnapshotRegistry struct {
	driver.Registry
	manager *reczzCountingSnapshotManager
}

func (r *reczzSnapshotRegistry) RelationTupleManager() relationtuple.Manager {
	return r.manager
}

type reczzReconnectGRPCServer struct {
	rts.UnimplementedCheckServiceServer
	handler *check.Handler
}

func (s *reczzReconnectGRPCServer) Check(ctx context.Context, req *rts.CheckRequest) (*rts.CheckResponse, error) {
	resp, err := s.handler.Check(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestReconnectAfterRevocationReczz(t *testing.T) {
	reg := driver.NewSqliteTestRegistry(t, driver.WithNamespaces([]*namespace.Namespace{{Name: "ns"}}))
	rt := testhelpers.APITupleFromString(t, "ns:o#r@s")
	testhelpers.MapAndInsertTuples(t, reg, rt)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	rts.RegisterCheckServiceServer(server, &reczzReconnectGRPCServer{handler: check.NewHandler(reg)})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	dial := func() *grpc.ClientConn {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		return conn
	}

	checkOnce := func(name string, want bool) string {
		conn := dial()
		defer conn.Close()
		c := rts.NewCheckServiceClient(conn)
		resp, err := c.Check(t.Context(), &rts.CheckRequest{Tuple: rt.ToProto()})
		require.NoError(t, err)
		t.Logf("%s allowed=%v snaptoken=%q", name, resp.Allowed, resp.Snaptoken)
		require.Equal(t, want, resp.Allowed)
		require.NotEmpty(t, resp.Snaptoken)
		require.NotEqual(t, "not yet implemented", resp.Snaptoken)
		return resp.Snaptoken
	}

	normalToken := checkOnce("normal", true)

	its, err := reg.Mapper().FromTuple(t.Context(), rt)
	require.NoError(t, err)
	require.NoError(t, reg.RelationTupleManager().DeleteRelationTuples(t.Context(), its...))

	revokedToken := checkOnce("reconnect-after-revoke", false)
	checkOnce("second-reconnect-after-revoke", false)
	require.NotEqual(t, normalToken, revokedToken, "revocation must produce a newer snapshot marker")
}

func TestBatchCheckSharesSnapshotReczz(t *testing.T) {
	reg := driver.NewSqliteTestRegistry(t, driver.WithNamespaces([]*namespace.Namespace{{Name: "ns"}}))
	rt := testhelpers.APITupleFromString(t, "ns:o#batch@s")
	testhelpers.MapAndInsertTuples(t, reg, rt)

	manager := &reczzCountingSnapshotManager{Manager: reg.RelationTupleManager()}
	engine := check.NewEngine(&reczzSnapshotRegistry{Registry: reg, manager: manager})

	results, err := engine.BatchCheck(t.Context(), []*ketoapi.RelationTuple{rt, rt}, 10)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, check.IsMember, results[0].Membership)
	require.Equal(t, check.IsMember, results[1].Membership)
	require.Equal(t, int32(1), manager.opens.Load(), "all workers must share one authorization snapshot")
	require.NotEmpty(t, results[0].SnapshotToken)
	require.Equal(t, results[0].SnapshotToken, results[1].SnapshotToken)
}
