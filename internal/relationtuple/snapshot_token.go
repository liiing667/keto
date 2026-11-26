// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"context"
	"encoding/base64"
	"time"
)

type snapshotTokenContextKey struct{}

// WithSnapshotToken attaches the opaque identifier of the snapshot used for a
// read. The token is intentionally not interpreted by callers.
func WithSnapshotToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, snapshotTokenContextKey{}, token)
}

// SnapshotTokenFromContext returns the snapshot identifier attached by the
// persistence layer, or an empty string when the manager does not provide one.
func SnapshotTokenFromContext(ctx context.Context) string {
	token, _ := ctx.Value(snapshotTokenContextKey{}).(string)
	return token
}

// NewSnapshotToken encodes the wall-clock time at which a read transaction
// established its snapshot. It is an audit marker, not a resumable Zookie:
// request-supplied tokens are not yet used to select an older snapshot.
func NewSnapshotToken(at time.Time) string {
	return "v1." + base64.RawURLEncoding.EncodeToString([]byte(at.UTC().Format(time.RFC3339Nano)))
}
