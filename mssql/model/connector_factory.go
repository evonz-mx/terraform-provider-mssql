package model

// ConnectorFactory creates a SQL Server connector from a ServerConfig.
// The interface accepts a neutral ServerConfig rather than SDK-specific types
// so the same factory can be driven from terraform-plugin-sdk/v2 resources
// and terraform-plugin-framework resources alike.
type ConnectorFactory interface {
	GetConnector(cfg ServerConfig) (interface{}, error)
}
