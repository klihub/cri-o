package server

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cri-o/cri-o/pkg/container"
	"github.com/opencontainers/runtime-tools/generate"
	types "k8s.io/cri-api/pkg/apis/runtime/v1"

	"github.com/container-orchestrated-devices/container-device-interface/pkg/cdi"
)

func TestAddOCIBindsForDev(t *testing.T) {
	ctr, err := container.New()
	if err != nil {
		t.Error(err)
	}
	if err := ctr.SetConfig(&types.ContainerConfig{
		Mounts: []*types.Mount{
			{
				ContainerPath: "/dev",
				HostPath:      "/dev",
			},
		},
		Metadata: &types.ContainerMetadata{
			Name: "testctr",
		},
	}, &types.PodSandboxConfig{
		Metadata: &types.PodSandboxMetadata{
			Name: "testpod",
		},
	}); err != nil {
		t.Error(err)
	}

	_, binds, err := addOCIBindMounts(context.Background(), ctr, "", "", nil, false, false, false)
	if err != nil {
		t.Error(err)
	}
	for _, m := range ctr.Spec().Mounts() {
		if m.Destination == "/dev" {
			t.Error("/dev shouldn't be in the spec if it's bind mounted from kube")
		}
	}
	var foundDev bool
	for _, b := range binds {
		if b.Destination == "/dev" {
			foundDev = true
			break
		}
	}
	if !foundDev {
		t.Error("no /dev mount found in spec mounts")
	}
}

func TestAddOCIBindsForSys(t *testing.T) {
	ctr, err := container.New()
	if err != nil {
		t.Error(err)
	}
	if err := ctr.SetConfig(&types.ContainerConfig{
		Mounts: []*types.Mount{
			{
				ContainerPath: "/sys",
				HostPath:      "/sys",
			},
		},
		Metadata: &types.ContainerMetadata{
			Name: "testctr",
		},
	}, &types.PodSandboxConfig{
		Metadata: &types.PodSandboxMetadata{
			Name: "testpod",
		},
	}); err != nil {
		t.Error(err)
	}

	_, binds, err := addOCIBindMounts(context.Background(), ctr, "", "", nil, false, false, false)
	if err != nil {
		t.Error(err)
	}
	var howManySys int
	for _, b := range binds {
		if b.Destination == "/sys" && b.Type != "sysfs" {
			howManySys++
		}
	}
	if howManySys != 1 {
		t.Error("there is not a single /sys bind mount")
	}
}

func TestAddOCIBindsCGroupRW(t *testing.T) {
	ctr, err := container.New()
	if err != nil {
		t.Error(err)
	}

	if err := ctr.SetConfig(&types.ContainerConfig{
		Metadata: &types.ContainerMetadata{
			Name: "testctr",
		},
	}, &types.PodSandboxConfig{
		Metadata: &types.PodSandboxMetadata{
			Name: "testpod",
		},
	}); err != nil {
		t.Error(err)
	}
	_, _, err = addOCIBindMounts(context.Background(), ctr, "", "", nil, false, false, true)
	if err != nil {
		t.Error(err)
	}
	var hasCgroupRW bool
	for _, m := range ctr.Spec().Mounts() {
		if m.Destination == "/sys/fs/cgroup" {
			for _, o := range m.Options {
				if o == "rw" {
					hasCgroupRW = true
				}
			}
		}
	}
	if !hasCgroupRW {
		t.Error("Cgroup mount not added with RW.")
	}

	ctr, err = container.New()
	if err != nil {
		t.Error(err)
	}
	if err := ctr.SetConfig(&types.ContainerConfig{
		Metadata: &types.ContainerMetadata{
			Name: "testctr",
		},
	}, &types.PodSandboxConfig{
		Metadata: &types.PodSandboxMetadata{
			Name: "testpod",
		},
	}); err != nil {
		t.Error(err)
	}
	var hasCgroupRO bool
	_, _, err = addOCIBindMounts(context.Background(), ctr, "", "", nil, false, false, false)
	if err != nil {
		t.Error(err)
	}
	for _, m := range ctr.Spec().Mounts() {
		if m.Destination == "/sys/fs/cgroup" {
			for _, o := range m.Options {
				if o == "ro" {
					hasCgroupRO = true
				}
			}
		}
	}
	if !hasCgroupRO {
		t.Error("Cgroup mount not added with RO.")
	}
}

func TestGetRequestedCDIDevices(t *testing.T) {
	type testCase struct {
		name        string
		annotations map[string]string
		result      []string
		shouldFail  bool
	}
	for _, tc := range []*testCase{
		{
			name: "no annotations",
		},
		{
			name: "invalid CDI device reference",
			annotations: map[string]string{
				cdi.AnnotationPrefix + "devices": "foobar",
			},
			shouldFail: true,
		},
		{
			name: "valid CDI device reference",
			annotations: map[string]string{
				cdi.AnnotationPrefix + "vendor1_devices": "vendor1.com/device=foo",
				cdi.AnnotationPrefix + "vendor2_devices": "vendor2.com/device=bar",
			},
			result: []string{
				"vendor1.com/device=foo",
				"vendor2.com/device=bar",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				devices []string
				err     error
			)

			devices, err = getRequestedCDIDevices(tc.annotations)
			if tc.shouldFail {
				assert.Error(t, err)
				assert.Nil(t, devices)
			} else {
				assert.NoError(t, err)
				sort.Strings(devices)
				assert.Equal(t, tc.result, devices)
			}
		})
	}
}

func TestInjectCDIDevices(t *testing.T) {
	type testCase struct {
		name       string
		devices    []string
		shouldFail bool
	}
	for _, tc := range []*testCase{
		{
			name: "nil devices",
		},
		{
			name:    "empty devices list",
			devices: []string{},
		},
		{
			name: "unresolvable devices",
			devices: []string{
				"vendor1.com/device=foo",
				"vendor2.com/device=bar",
			},
			shouldFail: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				ctx     = context.Background()
				specgen generate.Generator
				err     error
			)

			specgen, err = generate.New("linux")
			assert.NoError(t, err)
			assert.NotNil(t, specgen)

			err = injectCDIDevices(ctx, &specgen, tc.devices)
			if tc.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
