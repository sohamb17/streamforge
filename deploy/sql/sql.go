// Package sql embeds the database schema so binaries can apply it on start.
package sql

import _ "embed"

// FeatureHistory is the offline history schema.
//
//go:embed 001_feature_history.sql
var FeatureHistory string
