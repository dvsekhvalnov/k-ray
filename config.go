package engine

import (
	"github.com/dvsekhvalnov/sync4kafka"
)

type Config struct {
	*agent.Config

	// Path to woking dir where database files will be stored,
	// if not exists will be auto-created on start
	DataDir string

	// HTTP port to start embedded web interface
	Port string

	// Internal message processing spool configuration
	Spool struct {

		// Spool queue size
		Size int

		// Number of concurrent workers processing spool queue
		MaxWorkers int
	}
}

func NewConfig() *Config {
	cfg := &Config{
		Config: agent.NewConfig(),
	}

	cfg.Spool.Size = 100
	cfg.Spool.MaxWorkers = 3

	return cfg
}
