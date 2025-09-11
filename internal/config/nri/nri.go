package nri

import (
	"fmt"
	"maps"
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

// DefaultValidatorConfig is the CRI-O configuration for the built-in NRI
// default validator. The restrictions are configured with CRI-O specific
// keys, while per role overrides reuse the NRI configuration as such.
type DefaultValidatorConfig struct {
	EnableValidator                  bool     `toml:"nri_enable_default_validator"`
	RejectOCIHooks                   bool     `toml:"nri_validator_reject_oci_hook_adjustment"`
	RejectRuntimeDefaultSeccomp      bool     `toml:"nri_validator_reject_runtime_default_seccomp_adjustment"`
	RejectUnconfinedSeccomp          bool     `toml:"nri_validator_reject_unconfined_seccomp_adjustment"`
	RejectCustomSeccomp              bool     `toml:"nri_validator_reject_custom_seccomp_adjustment"`
	RejectNamespaces                 bool     `toml:"nri_validator_reject_namespace_adjustment"`
	RequiredPluginList               []string `toml:"nri_validator_required_plugins"`
	TolerateMissingPluginsAnnotation string   `toml:"nri_validator_tolerate_missing_plugins_annotation"`

	// Roles provide per role overrides to the restrictions above.
	Roles map[string]*validator.Config `toml:"roles"`
}

func (c *DefaultValidatorConfig) Enable() bool {
	return c != nil && c.EnableValidator
}

func (c *DefaultValidatorConfig) RejectOCIHookAdjustment() bool {
	return c != nil && c.RejectOCIHooks
}

func (c *DefaultValidatorConfig) RejectRuntimeDefaultSeccompAdjustment() bool {
	return c != nil && c.RejectRuntimeDefaultSeccomp
}

func (c *DefaultValidatorConfig) RejectUnconfinedSeccompAdjustment() bool {
	return c != nil && c.RejectUnconfinedSeccomp
}

func (c *DefaultValidatorConfig) RejectCustomSeccompAdjustment() bool {
	return c != nil && c.RejectCustomSeccomp
}

func (c *DefaultValidatorConfig) RejectNamespaceAdjustment() bool {
	return c != nil && c.RejectNamespaces
}

func (c *DefaultValidatorConfig) RequiredPlugins() []string {
	if c == nil {
		return nil
	}

	return c.RequiredPluginList
}

func (c *DefaultValidatorConfig) TolerateMissingAnnotation() string {
	if c == nil {
		return ""
	}

	return c.TolerateMissingPluginsAnnotation
}

// RoleOverrides returns the per role overrides of the restrictions.
func (c *DefaultValidatorConfig) RoleOverrides() map[string]*validator.Config {
	if c == nil {
		return nil
	}

	return c.Roles
}

// toNRI returns this configuration as NRI default validator configuration.
func (c *DefaultValidatorConfig) toNRI() *validator.DefaultValidatorConfig {
	if c == nil {
		return nil
	}

	return &validator.DefaultValidatorConfig{
		Enable: c.EnableValidator,
		Config: &validator.Config{
			RejectOCIHookAdjustment:               &c.RejectOCIHooks,
			RejectRuntimeDefaultSeccompAdjustment: &c.RejectRuntimeDefaultSeccomp,
			RejectUnconfinedSeccompAdjustment:     &c.RejectUnconfinedSeccomp,
			RejectCustomSeccompAdjustment:         &c.RejectCustomSeccomp,
			RejectNamespaceAdjustment:             &c.RejectNamespaces,
		},
		Roles:                     c.Roles,
		RequiredPlugins:           c.RequiredPluginList,
		TolerateMissingAnnotation: c.TolerateMissingPluginsAnnotation,
	}
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
	if c == nil || len(c.PluginRoles) == 0 {
		return nil
	}

	if err := (&nri.AuthConfig{Roles: c.PluginRoles}).Validate(); err != nil {
		return fmt.Errorf("invalid NRI plugin roles: %w", err)
	}

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
		opts = append(opts, nri.WithAuthConfig(&nri.AuthConfig{Roles: maps.Clone(c.PluginRoles)}))
	}

	if c != nil && c.DefaultValidator != nil {
		opts = append(opts, nri.WithDefaultValidator(c.DefaultValidator.toNRI()))
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
