// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package check_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/ory/x/httprouterx"
	"github.com/stretchr/testify/require"

	rts "github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2"
	"github.com/ory/keto/internal/check"
	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/ketoapi"
)

func TestReconnectConsumersShareAuthorizationBasis(t *testing.T) {
	t.Parallel()

	reg := driver.NewSqliteTestRegistry(t, driver.WithNamespaces([]*namespace.Namespace{{Name: "ns"}}))
	h := check.NewHandler(reg)

	router := httprouterx.NewRouterPublic()
	h.RegisterReadRoutes(router)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	rt := testhelpers.APITupleFromString(t, "ns:reconnect#view@user1")
	testhelpers.MapAndInsertTuples(t, reg, rt)

	grpcCheck := func() (*rts.CheckResponse, error) {
		resp, err := h.Check(t.Context(), connect.NewRequest(&rts.CheckRequest{
			Tuple: rt.ToProto(),
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	httpCheck := func() (bool, string) {
		resp, err := server.Client().Get(server.URL + check.RouteBase + "?" + rt.ToURLQuery().Encode())
		require.NoError(t, err)
		defer resp.Body.Close()

		var body struct {
			Allowed   bool   `json:"allowed"`
			Snaptoken string `json:"snaptoken"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		return body.Allowed, body.Snaptoken
	}

	firstGRPC, err := grpcCheck()
	require.NoError(t, err)
	require.True(t, firstGRPC.Allowed)
	require.NotEmpty(t, firstGRPC.Snaptoken)
	require.NotEqual(t, "not yet implemented", firstGRPC.Snaptoken)

	firstHTTPAllowed, firstHTTPToken := httpCheck()
	require.True(t, firstHTTPAllowed)
	require.NotEmpty(t, firstHTTPToken)
	require.Equal(t, firstGRPC.Snaptoken, firstHTTPToken,
		"gRPC and HTTP consumers must identify the same current authorization revision")

	firstBasis, ok := relationtuple.ParseAuthorizationBasisToken(firstGRPC.Snaptoken)
	require.True(t, ok)
	require.NotZero(t, firstBasis.TupleRevision)

	// Re-reading unchanged state must not mint a new identity. This is the
	// property a wall-clock marker cannot provide.
	secondGRPC, err := grpcCheck()
	require.NoError(t, err)
	require.True(t, secondGRPC.Allowed)
	require.Equal(t, firstGRPC.Snaptoken, secondGRPC.Snaptoken)

	internalTuples, err := reg.Mapper().FromTuple(t.Context(), rt)
	require.NoError(t, err)
	require.NoError(t, reg.RelationTupleManager().DeleteRelationTuples(t.Context(), internalTuples...))

	revokedGRPC, err := grpcCheck()
	require.NoError(t, err)
	require.False(t, revokedGRPC.Allowed)
	require.NotEmpty(t, revokedGRPC.Snaptoken)
	require.NotEqual(t, firstGRPC.Snaptoken, revokedGRPC.Snaptoken,
		"the post-revocation decision must carry a new authorization revision")

	revokedHTTPAllowed, revokedHTTPToken := httpCheck()
	require.False(t, revokedHTTPAllowed)
	require.Equal(t, revokedGRPC.Snaptoken, revokedHTTPToken)
	require.NotEqual(t, firstHTTPToken, revokedHTTPToken)

	revokedBasis, ok := relationtuple.ParseAuthorizationBasisToken(revokedGRPC.Snaptoken)
	require.True(t, ok)
	require.Greater(t, revokedBasis.TupleRevision, firstBasis.TupleRevision)
	require.Equal(t, "historical",
		relationtuple.CompareAuthorizationBasis(revokedBasis, firstGRPC.Snaptoken))
	require.Equal(t, "current",
		relationtuple.CompareAuthorizationBasis(revokedBasis, revokedGRPC.Snaptoken))

	// A second reconnect after the revocation keeps observing the same current
	// revision instead of producing another marker merely because time passed.
	thirdGRPC, err := grpcCheck()
	require.NoError(t, err)
	require.False(t, thirdGRPC.Allowed)
	require.Equal(t, revokedGRPC.Snaptoken, thirdGRPC.Snaptoken)
}

func TestBatchCheckResultsShareAuthorizationBasis(t *testing.T) {
	t.Parallel()

	reg := driver.NewSqliteTestRegistry(t, driver.WithNamespaces([]*namespace.Namespace{{Name: "ns"}}))
	rt := testhelpers.APITupleFromString(t, "ns:batch#view@user1")
	testhelpers.MapAndInsertTuples(t, reg, rt)

	results, err := reg.PermissionEngine().BatchCheck(t.Context(), []*ketoapi.RelationTuple{rt, rt}, 10)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, check.IsMember, results[0].Membership)
	require.Equal(t, check.IsMember, results[1].Membership)
	require.NotEmpty(t, results[0].SnapshotToken)
	require.Equal(t, results[0].SnapshotToken, results[1].SnapshotToken)
}
