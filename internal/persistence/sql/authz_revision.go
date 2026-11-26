// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/x/sqlcon"

	"github.com/ory/keto/internal/relationtuple"
)

type (
	authorizationRevisionWriteScopeKey struct{}

	// AuthorizationRevision is the persistent generation counter for one
	// network's relation tuple state.
	AuthorizationRevision struct {
		NetworkID uuid.UUID `db:"nid"`
		Revision  int64     `db:"revision"`
		UpdatedAt time.Time `db:"updated_at"`
	}

	authorizationRevisions []AuthorizationRevision
)

func (AuthorizationRevision) TableName() string {
	return "keto_authorization_revisions"
}

func withDeferredAuthorizationRevision(ctx context.Context) context.Context {
	return context.WithValue(ctx, authorizationRevisionWriteScopeKey{}, true)
}

func authorizationRevisionDeferred(ctx context.Context) bool {
	deferred, _ := ctx.Value(authorizationRevisionWriteScopeKey{}).(bool)
	return deferred
}

func (p *Persister) loadAuthorizationRevision(ctx context.Context) (AuthorizationRevision, bool, error) {
	nid := p.NetworkID(ctx)
	var rows authorizationRevisions
	if err := p.Connection(ctx).Where("nid = ?", nid).All(&rows); err != nil {
		return AuthorizationRevision{}, false, sqlcon.HandleError(err)
	}
	if len(rows) == 0 {
		return AuthorizationRevision{}, false, nil
	}
	return rows[0], true, nil
}

func (p *Persister) currentAuthorizationBasis(ctx context.Context) (relationtuple.AuthorizationBasis, error) {
	row, found, err := p.loadAuthorizationRevision(ctx)
	if err != nil {
		return relationtuple.AuthorizationBasis{}, err
	}
	if !found {
		return relationtuple.AuthorizationBasis{TupleRevision: 0}, nil
	}
	return relationtuple.AuthorizationBasis{TupleRevision: uint64(row.Revision)}, nil
}

func (p *Persister) withAuthorizationBasis(ctx context.Context) (context.Context, error) {
	if _, ok := relationtuple.AuthorizationBasisFromContext(ctx); ok {
		return ctx, nil
	}

	basis, err := p.currentAuthorizationBasis(ctx)
	if err != nil {
		return ctx, err
	}
	ctx = relationtuple.WithAuthorizationBasis(ctx, basis)
	return relationtuple.WithSnapshotToken(ctx, basis.Token()), nil
}

// advanceAuthorizationRevision bumps the persistent tuple generation unless an
// outer transact call took ownership of the bump.
func (p *Persister) advanceAuthorizationRevision(ctx context.Context) error {
	if authorizationRevisionDeferred(ctx) {
		return nil
	}
	return p.forceAdvanceAuthorizationRevision(ctx)
}

func (p *Persister) forceAdvanceAuthorizationRevision(ctx context.Context) error {
	nid := p.NetworkID(ctx)
	now := time.Now().UTC()

	_, found, err := p.loadAuthorizationRevision(ctx)
	if err != nil {
		return err
	}
	if !found {
		row := AuthorizationRevision{
			NetworkID: nid,
			Revision:  1,
			UpdatedAt: now,
		}
		if err := p.Connection(ctx).Create(&row); err == nil {
			return nil
		}
		// A concurrent first writer may have inserted the row. Fall through to
		// the atomic increment.
	}

	err = p.Connection(ctx).RawQuery(
		"UPDATE keto_authorization_revisions SET revision = revision + 1, updated_at = ? WHERE nid = ?",
		now, nid,
	).Exec()
	return sqlcon.HandleError(err)
}
