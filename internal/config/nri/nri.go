package nri

import (
	"github.com/pkg/errors"

	nri "github.com/containerd/nri/v2alpha1/pkg/adaptation"
)

// Config represents the CRI-O NRI configuration.
type Config struct {
	Enabled    bool   `toml:"enable_nri"`
	ConfigPath string `toml:"nri_config_file"`
	SocketPath string `toml:"nri_socket_path"`
	PluginPath string `toml:"nri_plugin_dir"`

	config *nri.Config
}

// New returns the default CRI-O NRI configuration.
func New() Config {
	return Config{
		ConfigPath: nri.DefaultConfigPath,
		SocketPath: nri.DefaultSocketPath,
		PluginPath: nri.DefaultPluginPath,
	}
}

// Validate loads and validates the effective runtime NRI configuration.
func (c *Config) Validate(onExecution bool) error {
	var (
		cfg *nri.Config
		err error
	)

	if c.Enabled {
		if cfg, err = nri.ReadConfig(c.ConfigPath); err != nil {
			return errors.Wrapf(err, "failed to load %q", c.ConfigPath)
		}
		if onExecution {
			c.config = cfg
		}
	}

	return nil
}

// Config returns the currently loaded NRI configuration.
func (c *Config) Config() *nri.Config {
	return c.config
}
