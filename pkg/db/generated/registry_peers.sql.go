// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/registry_peers.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRegistryPeer(row interface {
	Scan(dest ...any) error
}, p *RegistryPeer) error {
	return row.Scan(
		&p.ID,
		&p.OwnerUserID,
		&p.Name,
		&p.APIBaseURL,
		&p.BearerToken,
		&p.CredentialHint,
		&p.Status,
		&p.LastUsedAt,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
}

const createRegistryPeer = `-- name: CreateRegistryPeer :one
INSERT INTO registry_peers (owner_user_id, name, api_base_url, bearer_token, credential_hint, status)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
          last_used_at, created_at, updated_at`

type CreateRegistryPeerParams struct {
	OwnerUserID    uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Name           string    `db:"name" json:"name"`
	APIBaseURL     string    `db:"api_base_url" json:"api_base_url"`
	BearerToken    string    `db:"bearer_token" json:"-"`
	CredentialHint string    `db:"credential_hint" json:"credential_hint"`
	Status         string    `db:"status" json:"status"`
}

func (q *Queries) CreateRegistryPeer(ctx context.Context, arg CreateRegistryPeerParams) (RegistryPeer, error) {
	row := q.db.QueryRow(ctx, createRegistryPeer,
		arg.OwnerUserID,
		arg.Name,
		arg.APIBaseURL,
		arg.BearerToken,
		arg.CredentialHint,
		arg.Status,
	)
	var peer RegistryPeer
	err := scanRegistryPeer(row, &peer)
	return peer, err
}

const listRegistryPeersByOwner = `-- name: ListRegistryPeersByOwner :many
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE owner_user_id = $1
ORDER BY created_at DESC`

func (q *Queries) ListRegistryPeersByOwner(ctx context.Context, ownerUserID uuid.UUID) ([]RegistryPeer, error) {
	rows, err := q.db.Query(ctx, listRegistryPeersByOwner, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RegistryPeer
	for rows.Next() {
		var peer RegistryPeer
		if err := scanRegistryPeer(rows, &peer); err != nil {
			return nil, err
		}
		items = append(items, peer)
	}
	return items, rows.Err()
}

const deleteRegistryPeerForOwner = `-- name: DeleteRegistryPeerForOwner :execrows
DELETE FROM registry_peers
WHERE id = $1 AND owner_user_id = $2`

type DeleteRegistryPeerForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) DeleteRegistryPeerForOwner(ctx context.Context, arg DeleteRegistryPeerForOwnerParams) (int64, error) {
	tag, err := q.db.Exec(ctx, deleteRegistryPeerForOwner, arg.ID, arg.OwnerUserID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const getActiveRegistryPeerForOwner = `-- name: GetActiveRegistryPeerForOwner :one
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE id = $1 AND owner_user_id = $2 AND status = 'active'`

type GetActiveRegistryPeerForOwnerParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) GetActiveRegistryPeerForOwner(ctx context.Context, arg GetActiveRegistryPeerForOwnerParams) (RegistryPeer, error) {
	row := q.db.QueryRow(ctx, getActiveRegistryPeerForOwner, arg.ID, arg.OwnerUserID)
	var peer RegistryPeer
	err := scanRegistryPeer(row, &peer)
	return peer, err
}

const listActiveRegistryPeersForAutoRoute = `-- name: ListActiveRegistryPeersForAutoRoute :many
SELECT id, owner_user_id, name, api_base_url, bearer_token, credential_hint, status,
       last_used_at, created_at, updated_at
FROM registry_peers
WHERE owner_user_id = $1 AND status = 'active'
ORDER BY last_used_at ASC NULLS FIRST, created_at ASC
LIMIT 2`

func (q *Queries) ListActiveRegistryPeersForAutoRoute(ctx context.Context, ownerUserID uuid.UUID) ([]RegistryPeer, error) {
	rows, err := q.db.Query(ctx, listActiveRegistryPeersForAutoRoute, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RegistryPeer
	for rows.Next() {
		var peer RegistryPeer
		if err := scanRegistryPeer(rows, &peer); err != nil {
			return nil, err
		}
		items = append(items, peer)
	}
	return items, rows.Err()
}

const markRegistryPeerUsed = `-- name: MarkRegistryPeerUsed :exec
UPDATE registry_peers
SET last_used_at = NOW()
WHERE id = $1 AND owner_user_id = $2`

type MarkRegistryPeerUsedParams struct {
	ID          uuid.UUID `db:"id" json:"id"`
	OwnerUserID uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
}

func (q *Queries) MarkRegistryPeerUsed(ctx context.Context, arg MarkRegistryPeerUsedParams) error {
	_, err := q.db.Exec(ctx, markRegistryPeerUsed, arg.ID, arg.OwnerUserID)
	return err
}

func scanRegistryFederationInvite(row interface {
	Scan(dest ...any) error
}, i *RegistryFederationInvite) error {
	return row.Scan(
		&i.ID,
		&i.OwnerUserID,
		&i.Name,
		&i.APIBaseURL,
		&i.BearerToken,
		&i.TokenPrefix,
		&i.TokenHash,
		&i.CredentialHint,
		&i.Status,
		&i.ExpiresAt,
		&i.ConsumedAt,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
}

const createRegistryFederationInvite = `-- name: CreateRegistryFederationInvite :one
INSERT INTO registry_federation_invites (
    owner_user_id, name, api_base_url, bearer_token,
    token_prefix, token_hash, credential_hint, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, NOW() + ($8::int * INTERVAL '1 second')
)
RETURNING id, owner_user_id, name, api_base_url, bearer_token,
          token_prefix, token_hash, credential_hint, status,
          expires_at, consumed_at, created_at, updated_at`

type CreateRegistryFederationInviteParams struct {
	OwnerUserID      uuid.UUID `db:"owner_user_id" json:"owner_user_id"`
	Name             string    `db:"name" json:"name"`
	APIBaseURL       string    `db:"api_base_url" json:"api_base_url"`
	BearerToken      string    `db:"bearer_token" json:"-"`
	TokenPrefix      string    `db:"token_prefix" json:"token_prefix"`
	TokenHash        string    `db:"token_hash" json:"-"`
	CredentialHint   string    `db:"credential_hint" json:"credential_hint"`
	ExpiresInSeconds int32     `db:"expires_in_seconds" json:"expires_in_seconds"`
}

func (q *Queries) CreateRegistryFederationInvite(ctx context.Context, arg CreateRegistryFederationInviteParams) (RegistryFederationInvite, error) {
	row := q.db.QueryRow(ctx, createRegistryFederationInvite,
		arg.OwnerUserID,
		arg.Name,
		arg.APIBaseURL,
		arg.BearerToken,
		arg.TokenPrefix,
		arg.TokenHash,
		arg.CredentialHint,
		arg.ExpiresInSeconds,
	)
	var invite RegistryFederationInvite
	err := scanRegistryFederationInvite(row, &invite)
	return invite, err
}

const listActiveRegistryFederationInvitesByPrefixForUpdate = `-- name: ListActiveRegistryFederationInvitesByPrefixForUpdate :many
SELECT id, owner_user_id, name, api_base_url, bearer_token,
       token_prefix, token_hash, credential_hint, status,
       expires_at, consumed_at, created_at, updated_at
FROM registry_federation_invites
WHERE token_prefix = $1 AND status = 'active'
FOR UPDATE`

func (q *Queries) ListActiveRegistryFederationInvitesByPrefixForUpdate(ctx context.Context, tokenPrefix string) ([]RegistryFederationInvite, error) {
	rows, err := q.db.Query(ctx, listActiveRegistryFederationInvitesByPrefixForUpdate, tokenPrefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RegistryFederationInvite
	for rows.Next() {
		var invite RegistryFederationInvite
		if err := scanRegistryFederationInvite(rows, &invite); err != nil {
			return nil, err
		}
		items = append(items, invite)
	}
	return items, rows.Err()
}

const markRegistryFederationInviteExpired = `-- name: MarkRegistryFederationInviteExpired :exec
UPDATE registry_federation_invites
SET status = 'expired'
WHERE id = $1 AND status = 'active'`

func (q *Queries) MarkRegistryFederationInviteExpired(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, markRegistryFederationInviteExpired, id)
	return err
}

const markRegistryFederationInviteConsumed = `-- name: MarkRegistryFederationInviteConsumed :execrows
UPDATE registry_federation_invites
SET status = 'consumed', consumed_at = NOW()
WHERE id = $1 AND status = 'active'`

func (q *Queries) MarkRegistryFederationInviteConsumed(ctx context.Context, id uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, markRegistryFederationInviteConsumed, id)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
