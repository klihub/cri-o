package config_test

import (
	"bytes"
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// writeConfig writes the given configuration into a temporary file and
// loads it into the configuration under test.
func writeConfig(data string) error {
	f := t.MustTempFile("crio.conf")
	Expect(os.WriteFile(f, []byte(data), 0o600)).To(Succeed())

	return sut.UpdateFromFile(context.Background(), f)
}

var _ = t.Describe("NRI configuration", func() {
	BeforeEach(beforeEach)

	t.Describe("the default validator table", func() {
		It("should accept the documented configuration keys", func() {
			// Given the keys documented in crio.conf.5.md and written
			// by the configuration template
			err := writeConfig(`
[crio.nri]
enable_nri = true

[crio.nri.default_validator]
nri_enable_default_validator = true
nri_validator_reject_oci_hook_adjustment = true
nri_validator_reject_namespace_adjustment = true
nri_validator_required_plugins = [ "00-watcher" ]
nri_validator_tolerate_missing_plugins_annotation = "tolerate-missing-plugins"
`)

			// Then
			Expect(err).ToNot(HaveOccurred())
			Expect(sut.NRI.Enabled).To(BeTrue())
			Expect(sut.NRI.DefaultValidator.Enable()).To(BeTrue())
			Expect(sut.NRI.DefaultValidator.RejectOCIHookAdjustment()).To(BeTrue())
			Expect(sut.NRI.DefaultValidator.RejectNamespaceAdjustment()).To(BeTrue())
			Expect(sut.NRI.DefaultValidator.RequiredPlugins()).To(ConsistOf("00-watcher"))
			Expect(sut.NRI.DefaultValidator.TolerateMissingAnnotation()).
				To(Equal("tolerate-missing-plugins"))
		})

		It("should accept per role overrides", func() {
			// Given a restrictive default with an exception for a role
			err := writeConfig(`
[crio.nri]
enable_nri = true

[crio.nri.default_validator]
nri_enable_default_validator = true
nri_validator_reject_oci_hook_adjustment = true
  [crio.nri.default_validator.roles.hook_injector]
    reject_oci_hook_adjustment = false
`)

			// Then
			Expect(err).ToNot(HaveOccurred())
			Expect(sut.NRI.DefaultValidator.Roles).To(HaveKey("hook_injector"))

			override := sut.NRI.DefaultValidator.Roles["hook_injector"]
			Expect(override).NotTo(BeNil())
			Expect(override.RejectOCIHookAdjustment).NotTo(BeNil())
			Expect(*override.RejectOCIHookAdjustment).To(BeFalse())
		})
	})

	t.Describe("the plugin roles table", func() {
		It("should accept role entries", func() {
			// Given
			err := writeConfig(`
[crio.nri]
enable_nri = true
  [crio.nri.plugin_roles.hook_injector]
    keys = [ "bJtc4n1zJg+GY39n+oyfNbMzJo1UgtzAxifxireq8W0=" ]
    tags = { hooks = "true" }

  [crio.nri.plugin_roles.namespace_adjuster]
    keys = [ "8gGj+wy7oAuAmasm5K0RPTXLnLQSe8F5x4tp39+OlDo=" ]
`)

			// Then
			Expect(err).ToNot(HaveOccurred())
			Expect(sut.NRI.PluginRoles).To(HaveLen(2))
			Expect(sut.NRI.PluginRoles["hook_injector"].Keys).To(HaveLen(1))
			Expect(sut.NRI.PluginRoles["hook_injector"].Tags).To(HaveKeyWithValue("hooks", "true"))
		})

		It("should reject a role without keys", func() {
			// Given a role which no plugin can ever authenticate to
			Expect(writeConfig(`
[crio.nri]
enable_nri = true
  [crio.nri.plugin_roles.hook_injector]
`)).To(Succeed())

			// When
			err := sut.NRI.Validate(false)

			// Then
			Expect(err).To(HaveOccurred())
		})

		It("should fill in the role name from the configuration key", func() {
			// Given
			Expect(writeConfig(`
[crio.nri]
enable_nri = true
  [crio.nri.plugin_roles.hook_injector]
    keys = [ "bJtc4n1zJg+GY39n+oyfNbMzJo1UgtzAxifxireq8W0=" ]
`)).To(Succeed())

			// When
			Expect(sut.NRI.Validate(false)).To(Succeed())

			// Then
			Expect(sut.NRI.PluginRoles["hook_injector"].Role).To(Equal("hook_injector"))
		})
	})

	t.Describe("configuration round trip", func() {
		It("should preserve plugin roles and validator settings", func() {
			// Given a configuration an administrator could have written
			Expect(writeConfig(`
[crio.nri]
enable_nri = true
  [crio.nri.plugin_roles.hook_injector]
    keys = [ "bJtc4n1zJg+GY39n+oyfNbMzJo1UgtzAxifxireq8W0=" ]
    tags = { hooks = "true" }

[crio.nri.default_validator]
nri_enable_default_validator = true
nri_validator_reject_oci_hook_adjustment = true
  [crio.nri.default_validator.roles.hook_injector]
    reject_oci_hook_adjustment = false
`)).To(Succeed())

			// When it is written out as the template does and read back
			var wr bytes.Buffer
			Expect(sut.WriteTemplate(true, &wr)).To(Succeed())

			f := t.MustTempFile("crio.conf")
			Expect(os.WriteFile(f, wr.Bytes(), 0o600)).To(Succeed())

			reloaded := defaultConfig()
			Expect(reloaded.UpdateFromFile(context.Background(), f)).To(Succeed())

			// Then nothing is lost
			Expect(reloaded.NRI.Enabled).To(BeTrue())
			Expect(reloaded.NRI.PluginRoles).To(HaveLen(1))
			Expect(reloaded.NRI.PluginRoles["hook_injector"].Keys).
				To(Equal(sut.NRI.PluginRoles["hook_injector"].Keys))
			Expect(reloaded.NRI.PluginRoles["hook_injector"].Tags).
				To(HaveKeyWithValue("hooks", "true"))
			Expect(reloaded.NRI.DefaultValidator.Enable()).To(BeTrue())
			Expect(reloaded.NRI.DefaultValidator.RejectOCIHookAdjustment()).To(BeTrue())
			Expect(reloaded.NRI.DefaultValidator.Roles).To(HaveKey("hook_injector"))
		})
	})
})
