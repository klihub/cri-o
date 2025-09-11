package nri

import (
	"slices"
	"time"

	nri "github.com/containerd/nri/pkg/adaptation"
	validator "github.com/containerd/nri/plugins/default-validator"
	"github.com/containerd/otelttrpc"
	"github.com/containerd/ttrpc"
)

// Config represents the CRI-O NRI configuration.
type Config struct {
	Enabled                   bool          `toml:"enable_nri"`
	SocketPath                string        `toml:"nri_listen"`
	PluginPath                string        `toml:"nri_plugin_dir"`
	PluginConfigPath          string        `toml:"nri_plugin_config_dir"`
	PluginRegistrationTimeout time.Duration `toml:"nri_plugin_registration_timeout"`
	PluginRequestTimeout      time.Duration `toml:"nri_plugin_request_timeout"`
	DisableConnections        bool          `toml:"nri_disable_connections"`
	withTracing               bool
	PluginRoles               map[string]*nri.Role    `toml:"plugin_roles"`
	DefaultValidator          *DefaultValidatorConfig `toml:"default_validator"`
}

type DefaultValidatorConfig struct {
	*validator.DefaultValidatorConfig
}

func (c *DefaultValidatorConfig) Enable() bool {
	if c == nil || c.DefaultValidatorConfig == nil {
		return false
	}
	return c.DefaultValidatorConfig.Enable
}

func (c *DefaultValidatorConfig) RejectOCIHookAdjustment() bool {
	if c == nil || c.DefaultValidatorConfig == nil || c.Config == nil {
		return false
	}
	return c.Config.RejectOCIHookAdjustment != nil && *c.Config.RejectOCIHookAdjustment
}

func (c *DefaultValidatorConfig) RejectRuntimeDefaultSeccompAdjustment() bool {
	if c == nil || c.DefaultValidatorConfig == nil || c.Config == nil {
		return false
	}
	return c.Config.RejectRuntimeDefaultSeccompAdjustment != nil && *c.Config.RejectRuntimeDefaultSeccompAdjustment
}

func (c *DefaultValidatorConfig) RejectUnconfinedSeccompAdjustment() bool {
	if c == nil || c.DefaultValidatorConfig == nil || c.Config == nil {
		return false
	}
	return c.Config.RejectUnconfinedSeccompAdjustment != nil && *c.Config.RejectUnconfinedSeccompAdjustment
}

func (c *DefaultValidatorConfig) RejectCustomSeccompAdjustment() bool {
	if c == nil || c.DefaultValidatorConfig == nil || c.Config == nil {
		return false
	}
	return c.Config.RejectCustomSeccompAdjustment != nil && *c.Config.RejectCustomSeccompAdjustment
}

func (c *DefaultValidatorConfig) RejectNamespaceAdjustment() bool {
	if c == nil || c.DefaultValidatorConfig == nil || c.Config == nil {
		return false
	}
	return c.Config.RejectNamespaceAdjustment != nil && *c.Config.RejectNamespaceAdjustment
}

func (c *DefaultValidatorConfig) RequiredPlugins() []string {
	if c == nil || c.DefaultValidatorConfig == nil {
		return nil
	}
	return c.DefaultValidatorConfig.RequiredPlugins
}

func (c *DefaultValidatorConfig) TolerateMissingAnnotation() string {
	if c == nil || c.DefaultValidatorConfig == nil {
		return ""
	}
	return c.DefaultValidatorConfig.TolerateMissingAnnotation
}

// New returns the default CRI-O NRI configuration.
func New() *Config {
	return &Config{
		Enabled:                   true,
		SocketPath:                nri.DefaultSocketPath,
		PluginPath:                nri.DefaultPluginPath,
		PluginConfigPath:          nri.DefaultPluginConfigPath,
		PluginRegistrationTimeout: nri.DefaultPluginRegistrationTimeout,
		PluginRequestTimeout:      nri.DefaultPluginRequestTimeout,
		DefaultValidator:          &DefaultValidatorConfig{},
	}
}

func (c *Config) IsDefaultValidatorDefaultConfig() bool {
	return c.defaultValidatorEqual(New())
}

func (c *Config) defaultValidatorEqual(o *Config) bool {
	cv, ov := c.DefaultValidator, o.DefaultValidator

	if cv.Enable() != ov.Enable() {
		return false
	}

	if cv.RejectOCIHookAdjustment() != ov.RejectOCIHookAdjustment() {
		return false
	}

	if cv.RejectRuntimeDefaultSeccompAdjustment() != ov.RejectRuntimeDefaultSeccompAdjustment() {
		return false
	}

	if cv.RejectUnconfinedSeccompAdjustment() != ov.RejectUnconfinedSeccompAdjustment() {
		return false
	}

	if cv.RejectCustomSeccompAdjustment() != ov.RejectCustomSeccompAdjustment() {
		return false
	}

	if cv.RejectNamespaceAdjustment() != ov.RejectNamespaceAdjustment() {
		return false
	}

	if len(cv.RequiredPlugins()) != len(ov.RequiredPlugins()) {
		return false
	}

	if cv.TolerateMissingAnnotation() != ov.TolerateMissingAnnotation() {
		return false
	}

	if !slices.Equal(
		slices.Sorted(slices.Values(cv.RequiredPlugins())),
		slices.Sorted(slices.Values(ov.RequiredPlugins()))) {
		return false
	}

	return true
}

// Validate loads and validates the effective runtime NRI configuration.
func (c *Config) Validate(onExecution bool) error {
	return nil
}

func (c *Config) WithTracing(enable bool) *Config {
	if c != nil {
		c.withTracing = enable
	}

	return c
}

// ToOptions returns NRI options for this configuration.
func (c *Config) ToOptions() []nri.Option {
	opts := []nri.Option{}
	if c != nil && c.SocketPath != "" {
		opts = append(opts, nri.WithSocketPath(c.SocketPath))
	}

	if c != nil && c.PluginPath != "" {
		opts = append(opts, nri.WithPluginPath(c.PluginPath))
	}

	if c != nil && c.PluginConfigPath != "" {
		opts = append(opts, nri.WithPluginConfigPath(c.PluginConfigPath))
	}

	if c != nil && c.DisableConnections {
		opts = append(opts, nri.WithDisabledExternalConnections())
	}

	if c != nil && len(c.PluginRoles) > 0 {
		roles := []*nri.Role{}
		for _, r := range c.PluginRoles {
			roles = append(roles, r)
		}
		opts = append(opts, nri.WithAuthConfig(&nri.AuthConfig{Roles: roles}))
	}

	if c != nil && c.DefaultValidator != nil {
		opts = append(opts, nri.WithDefaultValidator(c.DefaultValidator.DefaultValidatorConfig))
	}

	if c.withTracing {
		opts = append(opts,
			nri.WithTTRPCOptions(
				[]ttrpc.ClientOpts{
					ttrpc.WithUnaryClientInterceptor(
						otelttrpc.UnaryClientInterceptor(),
					),
				},
				[]ttrpc.ServerOpt{
					ttrpc.WithUnaryServerInterceptor(
						otelttrpc.UnaryServerInterceptor(),
					),
				},
			),
		)
	}

	return opts
}

func (c *Config) ConfigureTimeouts() {
	if c.PluginRegistrationTimeout != 0 {
		nri.SetPluginRegistrationTimeout(c.PluginRegistrationTimeout)
	}

	if c.PluginRequestTimeout != 0 {
		nri.SetPluginRequestTimeout(c.PluginRequestTimeout)
	}
}
