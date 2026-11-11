// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package check_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/check"
	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/driver/config"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/internal/x/dbx"
	"github.com/ory/keto/ketoapi"
)

type snapshotVersionContextKey struct{}

type batchSnapshotManager struct {
	relationtuple.Manager

	mu         sync.Mutex
	generation int
	opens      int
}

func (m *batchSnapshotManager) WithLatestSnapshot(ctx context.Context, f func(context.Context) error) error {
	m.mu.Lock()
	generation := m.generation
	m.opens++
	m.mu.Unlock()

	return f(context.WithValue(ctx, snapshotVersionContextKey{}, generation))
}

func (m *batchSnapshotManager) advance() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation++
}

func (m *batchSnapshotManager) openCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opens
}

type batchSnapshotRegistry struct {
	driver.Registry
	manager *batchSnapshotManager
	checker check.Checker
}

func (r *batchSnapshotRegistry) RelationTupleManager() relationtuple.Manager {
	return r.manager
}

func (r *batchSnapshotRegistry) Checker() check.Checker {
	return r.checker
}

type snapshotChecker struct {
	manager        *batchSnapshotManager
	triggerWrite   bool
	writeOnce      sync.Once
	writeAt        int
	errorIndex     int
	delay          func(index int) time.Duration
	blockOnCancel  bool
	started        chan<- struct{}
	active         atomic.Int64
	maxActive      atomic.Int64
	completionMu   sync.Mutex
	completionSeen []int
}

func (c *snapshotChecker) CheckRelationTuple(ctx context.Context, r *relationtuple.RelationTuple, _ int) check.Result {
	index, err := strconv.Atoi(strings.TrimPrefix(r.Relation, "r"))
	if err != nil {
		return check.Result{Membership: check.MembershipUnknown, Err: err}
	}

	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maxActive := c.maxActive.Load()
		if active <= maxActive || c.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}

	if c.started != nil {
		c.started <- struct{}{}
	}
	if c.blockOnCancel {
		<-ctx.Done()
		return check.Result{Membership: check.MembershipUnknown, Err: ctx.Err()}
	}
	if c.triggerWrite && index >= c.writeAt {
		c.writeOnce.Do(c.manager.advance)
	}
	if c.delay != nil {
		select {
		case <-time.After(c.delay(index)):
		case <-ctx.Done():
			return check.Result{Membership: check.MembershipUnknown, Err: ctx.Err()}
		}
	}

	c.completionMu.Lock()
	c.completionSeen = append(c.completionSeen, index)
	c.completionMu.Unlock()

	if index == c.errorIndex {
		return check.Result{
			Membership: check.MembershipUnknown,
			Err:        fmt.Errorf("check %d failed", index),
		}
	}

	generation, _ := ctx.Value(snapshotVersionContextKey{}).(int)
	if index%2 == generation%2 {
		return check.Result{Membership: check.IsMember}
	}
	return check.Result{Membership: check.NotMember}
}

func batchTestTuples(t *testing.T, count int) []*ketoapi.RelationTuple {
	t.Helper()
	tuples := make([]*ketoapi.RelationTuple, count)
	for i := range tuples {
		tuples[i] = testhelpers.APITupleFromString(t, fmt.Sprintf("ns:object%d#r%d@user", i, i))
	}
	return tuples
}

func newBatchSnapshotEngine(t *testing.T, checker *snapshotChecker, parallel int) (*check.Engine, *batchSnapshotManager) {
	t.Helper()

	reg := driver.NewSqliteTestRegistry(t,
		driver.WithNamespaces([]*namespace.Namespace{{Name: "ns"}}),
		driver.WithConfig(config.KeyBatchCheckParallelizationLimit, parallel),
	)
	manager := &batchSnapshotManager{Manager: reg.RelationTupleManager()}
	return check.NewEngine(&batchSnapshotRegistry{
		Registry: reg,
		manager:  manager,
		checker:  checker,
	}), manager
}

func TestBatchCheckSharedSnapshotAcrossTuples(t *testing.T) {
	t.Parallel()

	const tupleCount = 100
	const parallel = 5

	checker := &snapshotChecker{
		triggerWrite: true,
		writeAt:      0,
		errorIndex:   7,
		delay: func(index int) time.Duration {
			return time.Duration(tupleCount-index) * time.Millisecond
		},
	}
	engine, manager := newBatchSnapshotEngine(t, checker, parallel)
	checker.manager = manager

	results, err := engine.BatchCheck(t.Context(), batchTestTuples(t, tupleCount), 10)
	require.NoError(t, err)
	require.Len(t, results, tupleCount)
	assert.Equal(t, 1, manager.openCount(), "the entire batch must use one snapshot")
	assert.LessOrEqual(t, checker.maxActive.Load(), int64(parallel), "batch parallelization limit must be preserved")

	for i, result := range results {
		if i == checker.errorIndex {
			require.Equal(t, check.MembershipUnknown, result.Membership)
			require.EqualError(t, result.Err, fmt.Sprintf("check %d failed", i))
			continue
		}
		require.NoErrorf(t, result.Err, "result %d", i)
		if i%2 == 0 {
			assert.Equalf(t, check.IsMember, result.Membership, "result %d used a newer snapshot", i)
		} else {
			assert.Equalf(t, check.NotMember, result.Membership, "result %d used a newer snapshot", i)
		}
	}

	checker.completionMu.Lock()
	completionOrder := slices.Clone(checker.completionSeen)
	checker.completionMu.Unlock()
	ascending := make([]int, tupleCount)
	for i := range ascending {
		ascending[i] = i
	}
	assert.False(t, slices.Equal(ascending, completionOrder), "checks should complete out of input order")
}

func TestBatchCheckMappingErrorsRemainIndexed(t *testing.T) {
	t.Parallel()

	checker := &snapshotChecker{errorIndex: -1}
	engine, _ := newBatchSnapshotEngine(t, checker, 2)

	tuples := batchTestTuples(t, 3)
	tuples[1] = testhelpers.APITupleFromString(t, "unknown:object#r1@user")

	results, err := engine.BatchCheck(t.Context(), tuples, 10)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Error(t, results[1].Err)
	require.Equal(t, check.MembershipUnknown, results[1].Membership)
	require.NoError(t, results[0].Err)
	require.NoError(t, results[2].Err)
	assert.Equal(t, check.IsMember, results[0].Membership)
	assert.Equal(t, check.IsMember, results[2].Membership)
}

func TestBatchCheckCancellationStopsWorkers(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{}, 1)
	checker := &snapshotChecker{
		blockOnCancel: true,
		started:       started,
	}
	engine, _ := newBatchSnapshotEngine(t, checker, 1)

	errCh := make(chan error, 1)
	go func() {
		_, err := engine.BatchCheck(ctx, batchTestTuples(t, batchCancelTupleCount), 10)
		errCh <- err
	}()

	select {
	case <-started:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("batch check did not start")
	}

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("batch check did not stop after cancellation")
	}
}

const batchCancelTupleCount = 10

func TestSingleCheckStillUsesLatestSnapshot(t *testing.T) {
	t.Parallel()

	checker := &snapshotChecker{errorIndex: -1}
	engine, manager := newBatchSnapshotEngine(t, checker, 1)
	checker.manager = manager
	manager.advance()

	result := engine.CheckRelationTuple(t.Context(), testhelpers.TupleFromString(t, "ns:object0#r0@user"), 10)
	require.NoError(t, result.Err)
	assert.Equal(t, check.NotMember, result.Membership)
	assert.Equal(t, 1, manager.openCount())
}

func TestBatchCheckSharedSnapshotSQLIsolation(t *testing.T) {
	t.Parallel()

	const opl = `
		class User implements Namespace {}
		class Folder implements Namespace {
			related: {
				members: User[]
			}
			permits = {
				isMember: (ctx: Context) => this.related.members.includes(ctx.subject),
			}
		}
		class File implements Namespace {
			related: {
				parent: Folder[]
			}
			permits = {
				view: (ctx: Context) => this.related.parent.traverse(p => p.permits.isMember(ctx)),
			}
		}
	`
	inputTuples := []string{
		"File:readme#parent@Folder:docs",
		"Folder:docs#members@User:u1",
	}

	var snapshotDSNs []*dbx.DsnT
	for _, dsn := range dbx.GetDSNs(t) {
		switch dsn.Name {
		case "cockroach", "mysql", "postgres":
			snapshotDSNs = append(snapshotDSNs, dsn)
		}
	}
	if len(snapshotDSNs) == 0 {
		t.Skip("sqlite is excluded from batch snapshot isolation test; run without -short to start CockroachDB/MySQL/PostgreSQL")
	}

	for _, dsn := range snapshotDSNs {
		t.Run("dsn="+dsn.Name, func(t *testing.T) {
			t.Parallel()

			reg := driver.NewTestRegistry(t, dsn,
				driver.WithOPL(opl),
				driver.WithMapperNamespace(testhelpers.CustomMapperNamespace),
				driver.WithConfig(config.KeyFeatureFlagStrictMode, true),
				driver.WithConfig(config.KeyBatchCheckParallelizationLimit, 1),
			)
			testhelpers.MapAndInsertTuplesFromString(t, reg, inputTuples)

			var once sync.Once
			middleware := func(mCtx context.Context, s check.Step, req check.CheckRequest, next func(context.Context, check.Step, check.CheckRequest) check.Result) check.Result {
				if s.Kind() == check.StepIsAllowed && req.Tuple.Namespace == "Folder" && req.Tuple.Relation == "isMember" {
					once.Do(func() {
						assert.NoError(t, reg.RelationTupleManager().DeleteRelationTuples(t.Context(),
							testhelpers.TupleFromString(t, "Folder:docs#members@User:u1"),
						))
					})
				}
				return next(mCtx, s, req)
			}
			engine := check.NewEngine(&checkerOverrideRegistry{Registry: reg, mw: middleware})

			tuple := testhelpers.APITupleFromString(t, "File:readme#view@User:u1")
			results, err := engine.BatchCheck(t.Context(), []*ketoapi.RelationTuple{tuple, tuple}, 5)
			require.NoError(t, err)
			require.Len(t, results, 2)
			assert.Equal(t, check.IsMember, results[0].Membership)
			assert.Equal(t, check.IsMember, results[1].Membership,
				"both checks must use the snapshot opened at the start of the batch")
		})
	}
}
