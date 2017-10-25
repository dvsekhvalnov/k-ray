package engine

import (
	"github.com/dvsekhvalnov/sync4kafka"
)

type Config struct {
	*agent.Config

	// Path to woking dir where database files will be stored,
	// if not exists will be auto-created on start
	DataDir string

	//HTTP port to start embedded web interface
	Port string
}

func NewConfig() *Config {
	return &Config{
		Config: agent.NewConfig(),
	}
}
