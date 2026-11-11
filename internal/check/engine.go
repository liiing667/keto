// Copyright © 2023 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package check

import (
	"context"
	"errors"
	"net/http"

	"github.com/ory/herodot"
	"github.com/ory/x/logrusx"
	"github.com/ory/x/otelx"
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
	result := e.CheckRelationTuple(ctx, r, restDepth)
	if result.Err != nil {
		return false, result.Err
	}
	if result.Membership == MembershipUnknown && result.Limitation != "" && e.dep.Config(ctx).StrictMode() {
		return false, errLimitationExceeded().WithReason(string(result.Limitation))
	}
	return result.Membership == IsMember, nil
}

// CheckRelationTuple checks if the relation tuple's subject has the relation on
// the object in the namespace either directly or indirectly and returns a check
// result.
func (e *Engine) CheckRelationTuple(ctx context.Context, r *relationTuple, restDepth int) (res Result) {
	txErr := e.dep.RelationTupleManager().WithLatestSnapshot(ctx, func(ctx context.Context) error {
		res = e.checkRelationTuple(ctx, r, restDepth)
		return res.Err
	})
	if txErr != nil && res.Err == nil {
		res.Membership = MembershipUnknown
		res.Err = herodot.ErrInternalServerError().WithWrap(txErr)

		e.dep.Logger().WithError(txErr).WithField("tuple", r.String()).Error("could not open a consistent read snapshot for the check")
	}
	return res
}

// checkRelationTuple evaluates one tuple inside the caller's snapshot scope.
func (e *Engine) checkRelationTuple(ctx context.Context, r *relationTuple, restDepth int) (res Result) {
	ctx, span := e.dep.Tracer(ctx).Tracer().Start(ctx, "Engine.CheckRelationTuple")
	defer otelx.End(span, &res.Err)

	res = e.dep.Checker().CheckRelationTuple(ctx, r, restDepth)
	if res.Err == nil {
		span.AddEvent(events.NewPermissionsChecked(ctx))
	}
	return res
}

// BatchCheck makes parallelized check requests for tuples against one shared
// read-only snapshot. The check results are returned as a slice where the
// result index matches the input tuple index.
func (e *Engine) BatchCheck(
	ctx context.Context,
	tuples []*ketoapi.RelationTuple,
	maxDepth int,
) ([]Result, error) {
	results := make([]Result, len(tuples))

	txErr := e.dep.RelationTupleManager().WithLatestSnapshot(ctx, func(ctx context.Context) error {
		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(e.dep.Config(ctx).BatchCheckParallelizationLimit())

		mapper := e.dep.ReadOnlyMapper()
		for i, tuple := range tuples {
			if egCtx.Err() != nil {
				break
			}
			i, tuple := i, tuple
			eg.Go(func() error {
				if err := egCtx.Err(); err != nil {
					return err
				}

				internalTuple, err := mapper.FromTuple(egCtx, tuple)
				if err != nil {
					results[i] = Result{
						Membership: MembershipUnknown,
						Err:        err,
					}
				} else {
					results[i] = e.checkRelationTuple(egCtx, internalTuple[0], maxDepth)
				}
				return egCtx.Err()
			})
		}

		if err := eg.Wait(); err != nil {
			return err
		}
		return ctx.Err()
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
