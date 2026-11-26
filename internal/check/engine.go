// Copyright © 2023 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package check

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/ory/herodot"
	"github.com/ory/x/logrusx"
	"github.com/ory/x/otelx"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/ory/keto/x/events"

	"github.com/ory/keto/internal/driver/config"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/ketoapi"
)

type (
	EngineProvider interface {
		PermissionEngine() *Engine
	}

	Engine struct {
		dep EngineDependencies
	}

	EngineDependencies interface {
		relationtuple.ManagerProvider
		relationtuple.MapperProvider
		CheckerProvider
		config.Provider
		logrusx.Provider
		otelx.Provider
	}

	// Type alias for shorter signatures.
	relationTuple = relationtuple.RelationTuple
)

func errLimitationExceeded() *herodot.DefaultError {
	return &herodot.DefaultError{
		StatusField:   http.StatusText(http.StatusUnprocessableEntity),
		ErrorField:    "The check could not be completed",
		CodeField:     http.StatusUnprocessableEntity,
		GRPCCodeField: codes.FailedPrecondition,
	}
}

// NewEngine creates an Engine that delegates checks to checker.
func NewEngine(d EngineDependencies) *Engine {
	return &Engine{dep: d}
}

// CheckIsMember checks if the relation tuple's subject has the relation on the
// object in the namespace either directly or indirectly and returns a boolean
// result.
func (e *Engine) CheckIsMember(ctx context.Context, r *relationTuple, restDepth int) (bool, error) {
	allowed, _, err := e.CheckIsMemberWithSnapshot(ctx, r, restDepth)
	return allowed, err
}

// CheckIsMemberWithSnapshot is [Engine.CheckIsMember] but also returns the
// evaluated Result, including its authorization basis token.
func (e *Engine) CheckIsMemberWithSnapshot(ctx context.Context, r *relationTuple, restDepth int) (bool, Result, error) {
	result := e.CheckRelationTuple(ctx, r, restDepth)
	if result.Err != nil {
		return false, result, result.Err
	}
	if result.Membership == MembershipUnknown && result.Limitation != "" && e.dep.Config(ctx).StrictMode() {
		return false, result, errLimitationExceeded().WithReason(string(result.Limitation))
	}
	return result.Membership == IsMember, result, nil
}

// CheckRelationTuple checks if the relation tuple's subject has the relation on
// the object in the namespace either directly or indirectly and returns a check
// result.
func (e *Engine) CheckRelationTuple(ctx context.Context, r *relationTuple, restDepth int) (res Result) {
	txErr := e.dep.RelationTupleManager().WithLatestSnapshot(ctx, func(ctx context.Context) error {
		res = e.evaluateRelationTuple(ctx, r, restDepth)
		return res.Err
	})
	if txErr != nil && res.Err == nil {
		res.Membership = MembershipUnknown
		res.Err = herodot.ErrInternalServerError().WithWrap(txErr)

		e.dep.Logger().WithError(txErr).WithField("tuple", r.String()).Error("could not open a consistent read snapshot for the check")
	}
	return res
}

// evaluateRelationTuple evaluates one tuple inside the snapshot scope opened
// by the caller. BatchCheck uses this to pin every worker to one authorization
// basis.
func (e *Engine) evaluateRelationTuple(ctx context.Context, r *relationTuple, restDepth int) (res Result) {
	ctx, span := e.dep.Tracer(ctx).Tracer().Start(ctx, "Engine.CheckRelationTuple")
	defer otelx.End(span, &res.Err)

	res = e.dep.Checker().CheckRelationTuple(ctx, r, restDepth)
	e.annotateAuthorizationBasis(ctx, &res)
	if res.SnapshotToken != "" {
		span.SetAttributes(
			attribute.String("keto.authorization_basis", res.SnapshotToken),
			attribute.Int64("keto.authorization_revision", int64(res.AuthorizationBasis.TupleRevision)),
		)
	}
	if res.Err == nil {
		span.AddEvent(events.NewPermissionsChecked(ctx))
	}
	return res
}

func (e *Engine) annotateAuthorizationBasis(ctx context.Context, res *Result) {
	basis, ok := relationtuple.AuthorizationBasisFromContext(ctx)
	if !ok {
		// Preserve compatibility with managers that only attach an opaque token.
		res.SnapshotToken = relationtuple.SnapshotTokenFromContext(ctx)
		return
	}

	if basis.PolicyFingerprint == "" {
		basis.PolicyFingerprint = e.policyFingerprint(ctx)
	}
	res.AuthorizationBasis = basis
	res.SnapshotToken = basis.Token()
}

func (e *Engine) policyFingerprint(ctx context.Context) string {
	manager, err := e.dep.Config(ctx).NamespaceManager()
	if err != nil || manager == nil {
		return ""
	}
	namespaces, err := manager.Namespaces(ctx)
	if err != nil {
		return ""
	}

	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Name < namespaces[j].Name
	})

	h := sha256.New()
	for _, namespace := range namespaces {
		if namespace == nil {
			continue
		}
		_, _ = h.Write([]byte(namespace.Name))
		_ = h.Write([]byte{0})
		if relations, err := json.Marshal(namespace.Relations); err == nil {
			_, _ = h.Write(relations)
		}
		_ = h.Write([]byte{0})
		_, _ = h.Write(namespace.Config)
		_ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BatchCheck makes parallelized check requests for tuples. The check results
// are returned as a slice where the result index matches the input tuple index.
//
// The whole batch runs in one latest-snapshot scope. A revocation or policy
// reload while workers are running therefore cannot make two items in the same
// batch observe different authorization generations.
func (e *Engine) BatchCheck(
	ctx context.Context,
	tuples []*ketoapi.RelationTuple,
	maxDepth int,
) ([]Result, error) {
	results := make([]Result, len(tuples))
	if len(tuples) == 0 {
		return results, nil
	}

	txErr := e.dep.RelationTupleManager().WithLatestSnapshot(ctx, func(snapshotCtx context.Context) error {
		if basis, ok := relationtuple.AuthorizationBasisFromContext(snapshotCtx); ok {
			basis.PolicyFingerprint = e.policyFingerprint(snapshotCtx)
			snapshotCtx = relationtuple.WithAuthorizationBasis(snapshotCtx, basis)
		}

		eg, egCtx := errgroup.WithContext(snapshotCtx)
		eg.SetLimit(e.dep.Config(snapshotCtx).BatchCheckParallelizationLimit())

		mapper := e.dep.ReadOnlyMapper()
		for i, tuple := range tuples {
			i, tuple := i, tuple
			eg.Go(func() error {
				internalTuple, err := mapper.FromTuple(egCtx, tuple)
				if err != nil {
					results[i] = Result{
						Membership: MembershipUnknown,
						Err:        err,
					}
					return egCtx.Err()
				}
				results[i] = e.evaluateRelationTuple(egCtx, internalTuple[0], maxDepth)
				return egCtx.Err()
			})
		}

		return eg.Wait()
	})
	if txErr != nil {
		if errors.Is(txErr, context.Canceled) || errors.Is(txErr, context.DeadlineExceeded) {
			return nil, txErr
		}
		e.dep.Logger().WithError(txErr).Error("could not open a consistent read snapshot for the batch check")
		return nil, herodot.ErrInternalServerError().WithWrap(txErr)
	}
	return results, nil
}
