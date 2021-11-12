/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package api

// SetLinuxMemoryLimit records setting the memory limit for a container.
func (u *ContainerUpdate) SetLinuxMemoryLimit(value int64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.Limit = Int64(value)
}

// SetLinuxMemoryReservation records setting the memory reservation for a container.
func (u *ContainerUpdate) SetLinuxMemoryReservation(value int64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.Reservation = Int64(value)
}

// SetLinuxMemorySwap records records setting the memory swap limit for a container.
func (u *ContainerUpdate) SetLinuxMemorySwap(value int64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.Swap = Int64(value)
}

// SetLinuxMemoryKernel records setting the memory kernel limit for a container.
func (u *ContainerUpdate) SetLinuxMemoryKernel(value int64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.Kernel = Int64(value)
}

// SetLinuxMemoryKernelTCP records setting the memory kernel TCP limit for a container.
func (u *ContainerUpdate) SetLinuxMemoryKernelTCP(value int64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.KernelTcp = Int64(value)
}

// SetLinuxMemorySwappiness records setting the memory swappiness for a container.
func (u *ContainerUpdate) SetLinuxMemorySwappiness(value uint64) {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.Swappiness = UInt64(value)
}

// DisableOOMKiller records disabling the OOM killer for a container.
func (u *ContainerUpdate) DisableOOMKiller() {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.DisableOomKiller = Bool(true)
}

// UseHierarchy records enabling hierarchical memory accounting for a container.
func (u *ContainerUpdate) UseHierarchy() {
	u.initLinuxResourcesMemory()
	u.Linux.Resources.Memory.UseHierarchy = Bool(true)
}

// SetCPUShares records setting the scheduler's CPU shares for a container.
func (u *ContainerUpdate) SetCPUShares(value uint64) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.Shares = UInt64(value)
}

// SetCPUQuota records setting the scheduler's CPU quota for a container.
func (u *ContainerUpdate) SetCPUQuota(value int64) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.Quota = Int64(value)
}

// SetCPUPeriod records setting the scheduler's CPU period for a container.
func (u *ContainerUpdate) SetCPUPeriod(value int64) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.Period = UInt64(value)
}

// SetCPURTRuntime records setting the scheduler's realtime runtime for a container.
func (u *ContainerUpdate) SetCPURTRuntime(value int64) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.RealtimeRuntime = Int64(value)
}

// SetCPURTPeriod records setting the scheduler's realtime period for a container.
func (u *ContainerUpdate) SetCPURTPeriod(value uint64) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.RealtimePeriod = UInt64(value)
}

// SetCPUSetCPUs records setting the cpuset CPUs for a container.
func (u *ContainerUpdate) SetCPUSetCPUs(value string) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.Cpus = value
}

// SetCPUSetMems records setting the cpuset memory for a container.
func (u *ContainerUpdate) SetCPUSetMems(value string) {
	u.initLinuxResourcesCPU()
	u.Linux.Resources.Cpu.Mems = value
}

// AddHugepageLimit records adding a hugepage limit for a container.
func (u *ContainerUpdate) AddHugepageLimit(pageSize string, value uint64) {
	u.initLinuxResources()
	u.Linux.Resources.HugepageLimits = append(u.Linux.Resources.HugepageLimits,
		&HugepageLimit{
			PageSize: pageSize,
			Limit:    value,
		})
}

// SetBlockIOClass records setting the Block I/O class for a container.
func (u *ContainerUpdate) SetBlockIOClass(value string) {
	u.initLinuxResources()
	u.Linux.Resources.BlockioClass = String(value)
}

// SetRDTClass records setting the RDT class for a container.
func (u *ContainerUpdate) SetRDTClass(value string) {
	u.initLinuxResources()
	u.Linux.Resources.RdtClass = String(value)
}

// SetIgnoreFailure marks an Update as ignored for failures.
// Such updates will not prevent the related container operation
// from succeeding if the update fails.
func (u *ContainerUpdate) SetIgnoreFailure() {
	u.IgnoreFailure = true
}

//
// Initializing a container update.
//

func (u *ContainerUpdate) initLinux() {
	if u.Linux == nil {
		u.Linux = &LinuxContainerUpdate{}
	}
}

func (u *ContainerUpdate) initLinuxResources() {
	u.initLinux()
	if u.Linux.Resources == nil {
		u.Linux.Resources = &LinuxResources{}
	}
}

func (u *ContainerUpdate) initLinuxResourcesMemory() {
	u.initLinuxResources()
	if u.Linux.Resources.Memory == nil {
		u.Linux.Resources.Memory = &LinuxMemory{}
	}
}

func (u *ContainerUpdate) initLinuxResourcesCPU() {
	u.initLinuxResources()
	if u.Linux.Resources.Cpu == nil {
		u.Linux.Resources.Cpu = &LinuxCPU{}
	}
}
