// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package step

import (
	"context"

	"github.com/ory/keto/internal/check"
)

// cyclePathKeyType is the context key for the DFS resolution path.
type cyclePathKeyType struct{}

var cyclePathKey cyclePathKeyType

// resolutionPath is the linked list of relation tuples currently being
// resolved along a single depth-first path. Each entry shadows the entries of
// its parent, so sibling branches do not see each other's tuples.
type resolutionPath struct {
	key    string
	parent *resolutionPath
}

// contains reports whether key is already present on this path, i.e. the same
// (namespace, object, relation, subject) tuple is being resolved further up
// the call stack.
func (p *resolutionPath) contains(key string) bool {
	for cur := p; cur != nil; cur = cur.parent {
		if cur.key == key {
			return true
		}
	}
	return false
}

// CycleDetectionMiddleware short-circuits a permission check as soon as the
// same relation tuple is entered twice along one resolution path. Such a
// repeat can only be produced by a cyclic permit reference in the namespace
// configuration (e.g. two permits that delegate to each other); the repeated
// branch can never contribute new evidence, so it is a controlled denial
// (NotMember) rather than an error or a stack overflow.
//
// Only IsAllowedStep entries are tracked: every OPL hop (computed userset,
// inversion, tuple-to-subject-set) is dispatched through IsAllowedStep, while
// the structural steps (rewrite, direct, expand ...) are the means of
// resolving one entry and must not themselves mark the path. The path is
// stored in the context as a per-branch linked list, so independent branches
// exploring the same tuple do not falsely report a cycle.
func CycleDetectionMiddleware() check.Middleware {
	return func(ctx context.Context, s check.Step, req check.CheckRequest, next func(context.Context, check.Step, check.CheckRequest) check.Result) check.Result {
		if s.Kind() != check.StepIsAllowed || req.Tuple == nil {
			return next(ctx, s, req)
		}

		path, _ := ctx.Value(cyclePathKey).(*resolutionPath)
		key := req.Tuple.String()
		if path.contains(key) {
			return check.ResultNotMember
		}

		return next(context.WithValue(ctx, cyclePathKey, &resolutionPath{key: key, parent: path}), s, req)
	}
}
