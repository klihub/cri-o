package nri

import (
	nri "github.com/containerd/nri/pkg/adaptation"
)

// Config data for NRI.
type Config struct {
	Disable    bool   `toml:"disable"`
	ConfigPath string `toml:"config_file"`
	SocketPath string `toml:"socket_path"`
	PluginPath string `toml:"plugin_path"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Disable:    true,
		ConfigPath: nri.DefaultConfigPath,
		SocketPath: nri.DefaultSocketPath,
		PluginPath: nri.DefaultPluginPath,
	}
}

// toOptions returns NRI options for this configuration.
func (c *Config) toOptions() []nri.Option {
	opts := []nri.Option{}
	if c.ConfigPath != "" {
		opts = append(opts, nri.WithConfigPath(c.ConfigPath))
	}
	if c.SocketPath != "" {
		opts = append(opts, nri.WithSocketPath(c.SocketPath))
	}
	if c.PluginPath != "" {
		opts = append(opts, nri.WithPluginPath(c.PluginPath))
	}
	return opts
}
