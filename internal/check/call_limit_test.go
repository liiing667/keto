// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package check_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/check"
	"github.com/ory/keto/internal/check/step"
	"github.com/ory/keto/internal/check/trace"
	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/schema"
)

func newRegistryForOPL(t *testing.T, opl string, opts ...driver.TestRegistryOption) *driver.RegistryDefault {
	t.Helper()
	parsed, errs := schema.Parse(opl)
	require.Empty(t, errs)
	nss := make([]*namespace.Namespace, len(parsed))
	for i := range parsed {
		nss[i] = &parsed[i]
	}
	return driver.NewSqliteTestRegistry(t, append([]driver.TestRegistryOption{driver.WithNamespaces(nss)}, opts...)...)
}

// requireCyclicError asserts err is the call-limit backstop, not just any error
// (e.g. a context cancellation), by checking its reason.
func requireCyclicError(t *testing.T, err error) {
	t.Helper()
	var re interface{ Reason() string }
	require.ErrorAs(t, err, &re)
	require.Contains(t, re.Reason(), "cyclic permit reference")
}

func TestCallLimit(t *testing.T) {
	t.Parallel()

	type outcome int
	const (
		cyclicDenial outcome = iota
		member
		depthLimited
	)

	for _, tc := range []struct {
		name      string
		opl       string
		opts      []driver.TestRegistryOption
		tuples    []string
		check     string
		restDepth int
		want      outcome
	}{
		{
			name: "invert cycle aborts regardless of configured depth",
			opl: `
				class User implements Namespace {}
				class Resource implements Namespace {
					permits = {
						edit: (ctx: Context): boolean => !this.permits.view(ctx),
						view: (ctx: Context): boolean => !this.permits.edit(ctx),
					}
				}`,
			opts:      []driver.TestRegistryOption{driver.WithConfig("limit.max_read_depth", 65535)},
			check:     "Resource:doc#edit@User:alice",
			restDepth: 65535,
			want:      cyclicDenial,
		},
		{
			name: "branching cycle aborts decisively",
			opl: `
				class User implements Namespace {}
				class R implements Namespace {
					permits = {
						a: (ctx: Context): boolean => this.permits.b(ctx) && this.permits.c(ctx),
						b: (ctx: Context): boolean => this.permits.a(ctx) && this.permits.a(ctx),
						c: (ctx: Context): boolean => this.permits.a(ctx) && this.permits.a(ctx),
					}
				}`,
			check:     "R:x#a@User:u",
			restDepth: 5,
			want:      cyclicDenial,
		},
		{
			name: "self-loop is denied as a cycle",
			opl: `
				class User implements Namespace {}
				class Resource implements Namespace {
					permits = {
						edit: (ctx: Context): boolean => this.permits.edit(ctx),
					}
				}`,
			check:     "Resource:doc#edit@User:alice",
			restDepth: 5,
			want:      cyclicDenial,
		},
		{
			name: "legit role chain resolves",
			opl: `
				class User implements Namespace {}
				class Doc implements Namespace {
					related: {
						l0: User[]; l1: User[]; l2: User[]; l3: User[];
					}
					permits = {
						p0: (ctx: Context): boolean => this.permits.p1(ctx) || this.related.l0.includes(ctx.subject),
						p1: (ctx: Context): boolean => this.permits.p2(ctx) || this.related.l1.includes(ctx.subject),
						p2: (ctx: Context): boolean => this.permits.p3(ctx) || this.related.l2.includes(ctx.subject),
						p3: (ctx: Context): boolean => this.related.l3.includes(ctx.subject),
					}
				}`,
			tuples:    []string{"Doc:d#l3@User:alice"},
			check:     "Doc:d#p0@User:alice",
			restDepth: 5,
			want:      member,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reg := newRegistryForOPL(t, tc.opl, tc.opts...)
			if len(tc.tuples) > 0 {
				testhelpers.MapAndInsertTuplesFromString(t, reg, tc.tuples)
			}
			rt := testhelpers.TupleFromString(t, tc.check)

			assertResult := func(t *testing.T, res check.Result) {
				switch tc.want {
				case cyclicDenial:
					require.NoError(t, res.Err)
					require.Equal(t, check.NotMember, res.Membership)
					require.Empty(t, res.Limitation)
				case member:
					require.NoError(t, res.Err)
					require.Equal(t, check.IsMember, res.Membership)
				case depthLimited:
					require.NoError(t, res.Err)
					require.Equal(t, check.MembershipUnknown, res.Membership)
					require.Equal(t, check.LimitationMaxDepthExceeded, res.Limitation)
				}
			}

			// Both executors wire the middleware separately.
			t.Run("check engine", func(t *testing.T) {
				assertResult(t, check.NewEngine(reg).CheckRelationTuple(t.Context(), rt, tc.restDepth))
			})
			t.Run("trace engine", func(t *testing.T) {
				res, tree := trace.NewEngine(reg).CheckRelationTupleWithTrace(t.Context(), rt, tc.restDepth)
				require.NotNil(t, tree)
				assertResult(t, res)
			})
		})
	}
}

// TestCycleDetection verifies that cyclic permit references are resolved as a
// controlled denial (NotMember, no error) instead of exhausting the recursion
// budget, and that the answer is stable across repeated evaluations.
func TestCycleDetection(t *testing.T) {
	t.Parallel()

	opl := `
		class User implements Namespace {}
		class Resource implements Namespace {
			related: {
				owners: User[]
			}
			permits = {
				edit: (ctx: Context): boolean => this.related.owners.includes(ctx.subject) || this.permits.review(ctx),
				review: (ctx: Context): boolean => this.permits.edit(ctx),
			}
		}`
	reg := newRegistryForOPL(t, opl,
		driver.WithConfig("limit.max_read_depth", 65535),
	)
	testhelpers.MapAndInsertTuplesFromString(t, reg, []string{
		"Resource:doc#owners@User:alice",
	})

	// A subject whose only path loops must be denied, deterministically and
	// without surfacing an error, on every evaluation.
	denied := testhelpers.TupleFromString(t, "Resource:doc#edit@User:bob")
	for i := 0; i < 5; i++ {
		res := check.NewEngine(reg).CheckRelationTuple(t.Context(), denied, 65535)
		require.NoError(t, res.Err, "iteration %d", i)
		require.Equal(t, check.NotMember, res.Membership, "iteration %d", i)
		require.Empty(t, res.Limitation, "iteration %d", i)
	}

	// The direct-membership branch still wins for a legitimate owner even
	// though an alternative branch of the same permit is cyclic.
	owner := testhelpers.TupleFromString(t, "Resource:doc#edit@User:alice")
	ownerRes := check.NewEngine(reg).CheckRelationTuple(t.Context(), owner, 65535)
	require.NoError(t, ownerRes.Err)
	require.Equal(t, check.IsMember, ownerRes.Membership)

	// The trace engine must apply the same semantics.
	traceRes, tree := trace.NewEngine(reg).CheckRelationTupleWithTrace(t.Context(), denied, 65535)
	require.NotNil(t, tree)
	require.NoError(t, traceRes.Err)
	require.Equal(t, check.NotMember, traceRes.Membership)
}

// TestCycleDetectionDiamondShape makes sure that two sibling branches
// converging on the same intermediate relation (a diamond) are not mistaken
// for a cycle: the shared sub-check is visited on independent DFS paths, not
// twice on the same path.
func TestCycleDetectionDiamondShape(t *testing.T) {
	t.Parallel()

	opl := `
		class User implements Namespace {}
		class Doc implements Namespace {
			related: {
				viewers: User[]
			}
			permits = {
				view: (ctx: Context): boolean => this.permits.a(ctx) || this.permits.b(ctx),
				a: (ctx: Context): boolean => this.permits.shared(ctx),
				b: (ctx: Context): boolean => this.permits.shared(ctx),
				shared: (ctx: Context): boolean => this.related.viewers.includes(ctx.subject),
			}
		}`
	reg := newRegistryForOPL(t, opl, driver.WithConfig("limit.max_read_depth", 65535))
	testhelpers.MapAndInsertTuplesFromString(t, reg, []string{"Doc:d#viewers@User:u"})

	member := check.NewEngine(reg).CheckRelationTuple(t.Context(),
		testhelpers.TupleFromString(t, "Doc:d#view@User:u"), 65535)
	require.NoError(t, member.Err)
	require.Equal(t, check.IsMember, member.Membership)

	denied := check.NewEngine(reg).CheckRelationTuple(t.Context(),
		testhelpers.TupleFromString(t, "Doc:d#view@User:other"), 65535)
	require.NoError(t, denied.Err)
	require.Equal(t, check.NotMember, denied.Membership)
}

// backstopRegistry wires an executor with only the call-budget middleware,
// simulating a configuration where cycle detection is not active.
type backstopRegistry struct {
	driver.Registry
}

func (r *backstopRegistry) Checker() check.Checker {
	return step.NewExecutor(r.Registry, step.CallLimitMiddleware())
}

// TestCallBudgetBackstop verifies that the call-budget middleware still
// aborts a runaway evaluation as a decisive error when it is wired without
// cycle detection. It is the last line of defense against stack exhaustion.
func TestCallBudgetBackstop(t *testing.T) {
	t.Parallel()

	opl := `
		class User implements Namespace {}
		class R2 implements Namespace {
			permits = {
				a: (ctx: Context): boolean => !this.permits.b(ctx),
				b: (ctx: Context): boolean => !this.permits.a(ctx),
			}
		}`
	reg := newRegistryForOPL(t, opl, driver.WithConfig("limit.max_read_depth", 65535))
	rt := testhelpers.TupleFromString(t, "R2:x#a@User:u")

	res := check.NewEngine(&backstopRegistry{Registry: reg}).CheckRelationTuple(t.Context(), rt, 65535)
	requireCyclicError(t, res.Err)
	require.Equal(t, check.MembershipUnknown, res.Membership)
}
