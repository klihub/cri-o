//go:build !linux
// +build !linux

package nri

import (
	nri "github.com/containerd/nri/v2alpha1/pkg/adaptation"
)

func linuxContainerToNRI(ctr Container) *nri.LinuxContainer {
	return nil
}
