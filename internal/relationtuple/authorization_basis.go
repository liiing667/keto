// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

type (
	authorizationBasisContextKey struct{}
	snapshotTokenContextKey      struct{}

	// AuthorizationBasis identifies the authorization state used for a check.
	//
	// TupleRevision is the persistent generation of the relation tuple store.
	// PolicyFingerprint identifies the loaded namespace/OPL configuration. The
	// zero value means that the caller could not obtain a persistent generation.
	AuthorizationBasis struct {
		TupleRevision     uint64
		PolicyFingerprint string
	}
)

// WithAuthorizationBasis attaches the authorization basis to ctx.
func WithAuthorizationBasis(ctx context.Context, basis AuthorizationBasis) context.Context {
	return context.WithValue(ctx, authorizationBasisContextKey{}, basis)
}

// AuthorizationBasisFromContext returns the authorization basis attached by a
// persistence manager, if any.
func AuthorizationBasisFromContext(ctx context.Context) (AuthorizationBasis, bool) {
	basis, ok := ctx.Value(authorizationBasisContextKey{}).(AuthorizationBasis)
	return basis, ok
}

// WithSnapshotToken attaches an opaque snapshot token to ctx. Prefer
// WithAuthorizationBasis when the components of the basis are known.
func WithSnapshotToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, snapshotTokenContextKey{}, token)
}

// SnapshotTokenFromContext returns an opaque snapshot token attached by a
// persistence manager, if any.
func SnapshotTokenFromContext(ctx context.Context) string {
	token, _ := ctx.Value(snapshotTokenContextKey{}).(string)
	return token
}

// Token returns the stable, opaque identifier of this authorization basis.
//
// The token deliberately does not contain the time at which a read happened.
// Two reads of the same tuple and policy generation therefore produce the same
// token, while a committed revocation or policy change produces another token.
func (b AuthorizationBasis) Token() string {
	payload := fmt.Sprintf("keto-authorization-basis/v1\nrevision=%d\npolicy=%s", b.TupleRevision, b.PolicyFingerprint)
	return "kab1." + base64.RawURLEncoding.EncodeToString([]byte(payload))
}

// ParseAuthorizationBasisToken decodes a token produced by Token.
func ParseAuthorizationBasisToken(token string) (AuthorizationBasis, bool) {
	encoded, ok := strings.CutPrefix(token, "kab1.")
	if !ok {
		return AuthorizationBasis{}, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return AuthorizationBasis{}, false
	}

	var basis AuthorizationBasis
	lines := strings.Split(string(raw), "\n")
	if len(lines) != 3 || lines[0] != "keto-authorization-basis/v1" {
		return AuthorizationBasis{}, false
	}

	revision, ok := strings.CutPrefix(lines[1], "revision=")
	if !ok {
		return AuthorizationBasis{}, false
	}
	parsed, err := strconv.ParseUint(revision, 10, 64)
	if err != nil {
		return AuthorizationBasis{}, false
	}
	basis.TupleRevision = parsed

	policy, ok := strings.CutPrefix(lines[2], "policy=")
	if !ok {
		return AuthorizationBasis{}, false
	}
	basis.PolicyFingerprint = policy
	return basis, true
}

// CompareAuthorizationBasis reports how supplied relates to current.
//
// An empty or unparseable supplied token is "unknown". Equal bases are
// "current"; a parseable but different basis is "historical" from the current
// server's point of view.
func CompareAuthorizationBasis(current AuthorizationBasis, supplied string) string {
	if supplied == "" {
		return "unknown"
	}
	old, ok := ParseAuthorizationBasisToken(supplied)
	if !ok {
		return "unknown"
	}
	if old == current {
		return "current"
	}
	return "historical"
}
