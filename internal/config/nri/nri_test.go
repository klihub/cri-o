package nri

import (
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func tempFileWithData(data string) string {
	f := t.MustTempFile("")
	Expect(ioutil.WriteFile(f, []byte(data), 0o644)).To(BeNil())
	return f
}

// NRI configuration tests.
var _ = t.Describe("When parsing NRI config file", func() {
	t.Describe("non-existent file", func() {
		It("should return an error", func() {
			cfg := New()
			cfg.Enabled = true
			cfg.ConfigPath = "non-existent-file"
			err := cfg.Validate(true)
			Expect(err).NotTo(BeNil())
		})
	})

	t.Describe("invalid file format", func() {
		It("should return an error", func() {
			f := tempFileWithData(`fooBar:
  - none
`)
			cfg := New()
			cfg.Enabled = true
			cfg.ConfigPath = f
			err := cfg.Validate(true)
			Expect(err).NotTo(BeNil())
		})
	})

	t.Describe("correct file format", func() {
		It("should not return an error", func() {
			f := tempFileWithData(`enablePlugins:
  - log-all
  - log-containers
disablePlugins:
  - "*"
`)
			cfg := New()
			cfg.Enabled = true
			cfg.ConfigPath = f
			err := cfg.Validate(true)
			Expect(err).To(BeNil())
		})
	})
})
