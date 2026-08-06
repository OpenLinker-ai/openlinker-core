package usertoken

import "time"

type GrantRequest struct {
	Permission   string         `json:"permission"`
	ResourceType string         `json:"resource_type"`
	ResourceID   *string        `json:"resource_id,omitempty"`
	Constraints  map[string]any `json:"constraints,omitempty"`
}

type GrantResponse struct {
	Permission   string         `json:"permission"`
	ResourceType string         `json:"resource_type"`
	ResourceID   *string        `json:"resource_id,omitempty"`
	Constraints  map[string]any `json:"constraints"`
}

type CreateRequest struct {
	Name      string         `json:"name"`
	Grants    []GrantRequest `json:"grants"`
	Scopes    []string       `json:"scopes,omitempty"`
	ExpiresAt *time.Time     `json:"expires_at"`
}

type UpdateRequest struct {
	Name      *string         `json:"name,omitempty"`
	Grants    *[]GrantRequest `json:"grants,omitempty"`
	Scopes    *[]string       `json:"scopes,omitempty"`
	ExpiresAt *time.Time      `json:"expires_at,omitempty"`
}

type TokenResponse struct {
	ID               string          `json:"id"`
	UserID           string          `json:"user_id"`
	IssuerInstanceID string          `json:"issuer_instance_id"`
	Name             string          `json:"name"`
	Prefix           string          `json:"prefix"`
	Grants           []GrantResponse `json:"grants"`
	Scopes           []string        `json:"scopes"`
	ExpiresAt        *string         `json:"expires_at"`
	LastUsedAt       *string         `json:"last_used_at"`
	RevokedAt        *string         `json:"revoked_at"`
	CreatedAt        string          `json:"created_at"`
	UpdatedAt        string          `json:"updated_at"`
	PlaintextToken   string          `json:"plaintext_token,omitempty"`
}

type ListOptions struct {
	Limit   int32
	Offset  int32
	SortBy  string
	SortDir string
	Status  string
	Query   string
}

type ListResponse struct {
	Items   []TokenResponse `json:"items"`
	Total   int32           `json:"total"`
	Limit   int32           `json:"limit"`
	Offset  int32           `json:"offset"`
	SortBy  string          `json:"sort_by"`
	SortDir string          `json:"sort_dir"`
	HasMore bool            `json:"has_more"`
}

type IntrospectionRequest struct {
	Token string `json:"token"`
}

type IntrospectionResponse struct {
	Active           bool            `json:"active"`
	IssuerInstanceID string          `json:"issuer_instance_id,omitempty"`
	TokenID          string          `json:"token_id,omitempty"`
	UserID           string          `json:"user_id,omitempty"`
	Permissions      []string        `json:"permissions,omitempty"`
	Grants           []GrantResponse `json:"grants,omitempty"`
	ExpiresAt        *string         `json:"expires_at,omitempty"`
}
