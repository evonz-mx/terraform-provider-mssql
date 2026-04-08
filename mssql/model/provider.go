package model

import "github.com/rs/zerolog"

// Provider is the interface exposed by the configured mssql provider to
// individual resources and data sources.
type Provider interface {
	GetConnector(cfg ServerConfig) (interface{}, error)
	ResourceLogger(resource, function string) zerolog.Logger
	DataSourceLogger(datasource, function string) zerolog.Logger
}
