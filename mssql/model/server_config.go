package model

import "time"

// ServerConfig holds the connection parameters for a SQL Server instance.
// It is intentionally independent of any Terraform SDK so it can be populated
// from terraform-plugin-sdk/v2 ResourceData today and from
// terraform-plugin-framework plan/state data in the future.
type ServerConfig struct {
	Host    string
	Port    string
	Timeout time.Duration

	// Exactly one of the following auth fields should be non-nil,
	// mirroring the ExactlyOneOf constraint on the HCL server block.
	Login     *LoginConfig
	Azure     *AzureLoginConfig
	ChainAuth *ChainAuthConfig
	MSI       *MSIConfig
}

// LoginConfig holds SQL Server username/password credentials.
type LoginConfig struct {
	Username string
	Password string
}

// AzureLoginConfig holds Azure service-principal credentials.
type AzureLoginConfig struct {
	TenantID     string
	ClientID     string
	ClientSecret string
}

// ChainAuthConfig represents the azuread_default_chain_auth block.
// When UseOIDC is true the provider fetches an OIDC token from env vars;
// when false it falls back to the Azure AD default credential chain.
type ChainAuthConfig struct {
	UseOIDC bool
}

// MSIConfig represents the azuread_managed_identity_auth block.
type MSIConfig struct {
	// UserID is the optional client ID of a user-assigned managed identity.
	UserID string
}
