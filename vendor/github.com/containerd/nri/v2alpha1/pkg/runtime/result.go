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

package runtime

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type result struct {
	request resultRequest
	reply   resultReply
	updates map[string]*ContainerUpdate
	setters map[string]string
}

type resultRequest struct {
	create *CreateContainerRequest
	update *UpdateContainerRequest
}

type resultReply struct {
	adjust *ContainerAdjustment
	update []*ContainerUpdate
}

func collectCreateContainerResult(request *CreateContainerRequest) *result {
	if request.Container.Labels == nil {
		request.Container.Labels = map[string]string{}
	}
	if request.Container.Annotations == nil {
		request.Container.Annotations = map[string]string{}
	}
	if request.Container.Mounts == nil {
		request.Container.Mounts = []*Mount{}
	}
	if request.Container.Env == nil {
		request.Container.Env = []string{}
	}
	if request.Container.Hooks == nil {
		request.Container.Hooks = &Hooks{}
	}
	if request.Container.Linux == nil {
		request.Container.Linux = &LinuxContainer{}
	}
	if request.Container.Linux.Devices == nil {
		request.Container.Linux.Devices = []*LinuxDevice{}
	}
	if request.Container.Linux.Resources == nil {
		request.Container.Linux.Resources = &LinuxResources{}
	}
	if request.Container.Linux.Resources.Memory == nil {
		request.Container.Linux.Resources.Memory = &LinuxMemory{}
	}
	if request.Container.Linux.Resources.Cpu == nil {
		request.Container.Linux.Resources.Cpu = &LinuxCPU{}
	}

	return &result{
		request: resultRequest{
			create: request,
		},
		reply: resultReply{
			adjust: &ContainerAdjustment{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
				Mounts:      []*Mount{},
				Env:         []*KeyValue{},
				Hooks:       &Hooks{},
				Linux: &LinuxContainerAdjustment{
					Devices: []*LinuxDevice{},
					Resources: &LinuxResources{
						Memory:         &LinuxMemory{},
						Cpu:            &LinuxCPU{},
						HugepageLimits: []*HugepageLimit{},
					},
				},
			},
		},
		updates: map[string]*ContainerUpdate{},
		setters: map[string]string{},
	}
}

func collectUpdateContainerResult(request *UpdateContainerRequest) *result {
	if request != nil {
		if request.LinuxResources == nil {
			request.LinuxResources = &LinuxResources{}
		}
		if request.LinuxResources.Memory == nil {
			request.LinuxResources.Memory = &LinuxMemory{}
		}
		if request.LinuxResources.Cpu == nil {
			request.LinuxResources.Cpu = &LinuxCPU{}
		}
	}

	return &result{
		request: resultRequest{
			update: request,
		},
		reply: resultReply{
			update: []*ContainerUpdate{},
		},
		updates: map[string]*ContainerUpdate{},
		setters: map[string]string{},
	}
}

func collectStopContainerResult() *result {
	return collectUpdateContainerResult(nil)
}

func (r *result) createContainerResponse() *CreateContainerResponse {
	return &CreateContainerResponse{
		Adjust: r.reply.adjust,
		Update: r.reply.update,
	}
}

func (r *result) updateContainerResponse() *UpdateContainerResponse {
	requested := r.updates[r.request.update.Container.Id]
	return &UpdateContainerResponse{
		Update: append(r.reply.update, requested),
	}
}

func (r *result) stopContainerResponse() *StopContainerResponse {
	return &StopContainerResponse{
		Update: r.reply.update,
	}
}

func (r *result) apply(response interface{}, plugin string) error {
	switch rpl := response.(type) {
	case *CreateContainerResponse:
		if rpl == nil {
			return nil
		}
		if err := r.adjust(rpl.Adjust, plugin); err != nil {
			return err
		}
		if err := r.update(rpl.Update, plugin); err != nil {
			return err
		}
	case *UpdateContainerResponse:
		if rpl == nil {
			return nil
		}
		if err := r.update(rpl.Update, plugin); err != nil {
			return err
		}
	case *StopContainerResponse:
		if rpl == nil {
			return nil
		}
		if err := r.update(rpl.Update, plugin); err != nil {
			return err
		}
	default:
		return errors.Errorf("cannot apply response of invalid type %T", response)
	}

	return nil
}

func (r *result) adjust(rpl *ContainerAdjustment, plugin string) error {
	if rpl == nil {
		return nil
	}
	if err := r.adjustLabels(rpl.Labels, plugin); err != nil {
		return err
	}
	if err := r.adjustAnnotations(rpl.Annotations, plugin); err != nil {
		return err
	}
	if err := r.adjustMounts(rpl.Mounts, plugin); err != nil {
		return err
	}
	if err := r.adjustEnv(rpl.Env, plugin); err != nil {
		return err
	}
	if err := r.adjustHooks(rpl.Hooks, plugin); err != nil {
		return err
	}
	if rpl.Linux != nil {
		if err := r.adjustDevices(rpl.Linux.Devices, plugin); err != nil {
			return err
		}
		if err := r.adjustResources(rpl.Linux.Resources, plugin); err != nil {
			return err
		}
		if err := r.adjustCgroupsPath(rpl.Linux.CgroupsPath, plugin); err != nil {
			return err
		}
	}

	return nil
}

func (r *result) update(updates []*ContainerUpdate, plugin string) error {
	for _, u := range updates {
		reply, err := r.getContainerUpdate(u, plugin)
		if err != nil {
			return err
		}
		if err := r.updateResources(reply, u, plugin); err != nil && !u.IgnoreFailure {
			return err
		}
	}

	return nil
}

func (r *result) adjustLabels(labels map[string]string, plugin string) error {
	if len(labels) == 0 {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id
	del := map[string]struct{}{}
	for k := range labels {
		if key, marked := IsMarkedForRemoval(k); marked {
			del[key] = struct{}{}
			delete(labels, k)
		}
	}

	for k, v := range labels {
		if _, ok := del[k]; ok {
			r.clearOwner(id, "label", k, plugin)
			delete(create.Container.Labels, k)
			r.reply.adjust.Labels[MarkForRemoval(k)] = ""
		}
		if err := r.setOwner(id, "label", k, plugin); err != nil {
			return err
		}
		create.Container.Labels[k] = v
		r.reply.adjust.Labels[k] = v
		delete(del, k)
	}

	for k := range del {
		r.reply.adjust.Labels[MarkForRemoval(k)] = ""
	}

	return nil
}

func (r *result) adjustAnnotations(annotations map[string]string, plugin string) error {
	if len(annotations) == 0 {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id
	del := map[string]struct{}{}
	for k := range annotations {
		if key, marked := IsMarkedForRemoval(k); marked {
			del[key] = struct{}{}
			delete(annotations, k)
		}
	}

	for k, v := range annotations {
		if _, ok := del[k]; ok {
			r.clearOwner(id, "annotation", k, plugin)
			delete(create.Container.Annotations, k)
			r.reply.adjust.Annotations[MarkForRemoval(k)] = ""
		}
		if err := r.setOwner(id, "annotation", k, plugin); err != nil {
			return err
		}
		create.Container.Annotations[k] = v
		r.reply.adjust.Annotations[k] = v
		delete(del, k)
	}

	for k := range del {
		r.reply.adjust.Annotations[MarkForRemoval(k)] = ""
	}

	return nil
}

func (r *result) adjustMounts(mounts []*Mount, plugin string) error {
	if len(mounts) == 0 {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id

	// first merge new adjustments with existing ones
	adj := map[string]*Mount{}
	for _, m := range r.reply.adjust.Mounts {
		key, _ := m.IsMarkedForRemoval() // clean path from any markers
		adj[key] = m
	}

	mod := map[string]*Mount{}
	for _, m := range mounts {
		key, marked := m.IsMarkedForRemoval()
		if marked {
			r.clearOwner(id, "mount", key, plugin)
		}
		if a, ok := adj[key]; ok {
			*a = *m
		} else {
			r.reply.adjust.Mounts = append(r.reply.adjust.Mounts, m)
		}
		mod[key] = m
	}

	// next adjust existing container mounts (delete, modify, or keep intact)
	updated := []*Mount{}
	for _, c := range create.Container.Mounts {
		if m, ok := mod[c.Destination]; ok {
			delete(mod, c.Destination)
			key, marked := m.IsMarkedForRemoval()
			if marked {
				continue
			}
			if err := r.setOwner(id, "mount", key, plugin); err != nil {
				return err
			}
			c = m
		}
		updated = append(updated, c)
	}

	// finally append the remaining new adjustments in their original order
	for _, m := range mounts {
		key, marked := m.IsMarkedForRemoval()
		if marked {
			continue
		}
		if _, ok := mod[key]; ok {
			updated = append(updated, m)
		}
	}

	create.Container.Mounts = updated

	return nil
}

func (r *result) adjustDevices(devices []*LinuxDevice, plugin string) error {
	if len(devices) == 0 {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id

	// first merge new adjustments with existing ones
	adj := map[string]*LinuxDevice{}
	for _, d := range r.reply.adjust.Linux.Devices {
		key, _ := d.IsMarkedForRemoval() // clean path from any markers
		adj[key] = d
	}

	mod := map[string]*LinuxDevice{}
	for _, d := range devices {
		key, marked := d.IsMarkedForRemoval()
		if marked {
			r.clearOwner(id, "device", key, plugin)
		}
		if a, ok := adj[key]; ok {
			*a = *d
		} else {
			r.reply.adjust.Linux.Devices = append(r.reply.adjust.Linux.Devices, d)
		}
		mod[key] = d
	}

	// next adjust existing container devices (delete, modify, or keep intact)
	updated := []*LinuxDevice{}
	for _, c := range create.Container.Linux.Devices {
		if d, ok := mod[c.Path]; ok {
			delete(mod, c.Path)
			key, marked := d.IsMarkedForRemoval()
			if marked {
				continue
			}
			if err := r.setOwner(id, "device", key, plugin); err != nil {
				return err
			}
			c = d
		}
		updated = append(updated, c)
	}

	// finally append the remaining new adjustments in their original order
	for _, d := range devices {
		key, marked := d.IsMarkedForRemoval()
		if marked {
			continue
		}
		if _, ok := mod[key]; ok {
			updated = append(updated, d)
		}
	}

	create.Container.Linux.Devices = updated

	return nil
}

func (r *result) adjustEnv(env []*KeyValue, plugin string) error {
	if len(env) == 0 {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id

	// first merge new adjustments with existing ones
	adj := map[string]*KeyValue{}
	for _, e := range r.reply.adjust.Env {
		key, _ := e.IsMarkedForRemoval() // clean name from any markers
		adj[key] = e
	}

	mod := map[string]*KeyValue{}
	for _, e := range env {
		key, marked := e.IsMarkedForRemoval()
		if marked {
			r.clearOwner(id, "env", key, plugin)
		}
		if a, ok := adj[key]; ok {
			*a = *e
		} else {
			r.reply.adjust.Env = append(r.reply.adjust.Env, e)
		}
		mod[key] = e
	}

	// next adjust existing container environment (delete, modify, or keep intact)
	updated := []string{}
	for _, c := range create.Container.Env {
		keyval := strings.SplitN(c, "=", 2)
		if len(keyval) < 2 {
			continue
		}
		k := keyval[0]
		if e, ok := mod[k]; ok {
			delete(mod, k)
			key, marked := e.IsMarkedForRemoval()
			if marked {
				continue
			}
			if err := r.setOwner(id, "env", key, plugin); err != nil {
				return err
			}
			c = e.ToOCI()
		}
		updated = append(updated, c)
	}

	// finally append the remaining new adjustments in their original order
	for _, e := range env {
		key, marked := e.IsMarkedForRemoval()
		if marked {
			continue
		}
		if _, ok := mod[key]; ok {
			updated = append(updated, e.ToOCI())
		}
	}

	create.Container.Env = updated

	return nil
}

func (r *result) adjustHooks(hooks *Hooks, plugin string) error {
	if hooks == nil {
		return nil
	}

	reply := r.reply.adjust
	container := r.request.create.Container

	if h := hooks.Prestart; len(h) > 0 {
		reply.Hooks.Prestart = append(reply.Hooks.Prestart, h...)
		container.Hooks.Prestart = append(container.Hooks.Prestart, h...)
	}
	if h := hooks.Poststart; len(h) > 0 {
		reply.Hooks.Poststart = append(reply.Hooks.Poststart, h...)
		container.Hooks.Poststart = append(container.Hooks.Poststart, h...)
	}
	if h := hooks.Poststop; len(h) > 0 {
		reply.Hooks.Poststop = append(reply.Hooks.Poststop, h...)
		container.Hooks.Poststop = append(container.Hooks.Poststop, h...)
	}
	if h := hooks.CreateRuntime; len(h) > 0 {
		reply.Hooks.CreateRuntime = append(reply.Hooks.CreateRuntime, h...)
		container.Hooks.CreateRuntime = append(container.Hooks.CreateRuntime, h...)
	}
	if h := hooks.CreateContainer; len(h) > 0 {
		reply.Hooks.CreateContainer = append(reply.Hooks.CreateContainer, h...)
		container.Hooks.CreateContainer = append(container.Hooks.CreateContainer, h...)
	}
	if h := hooks.StartContainer; len(h) > 0 {
		reply.Hooks.StartContainer = append(reply.Hooks.StartContainer, h...)
		container.Hooks.StartContainer = append(container.Hooks.StartContainer, h...)
	}

	return nil
}

func (r *result) adjustResources(resources *LinuxResources, plugin string) error {
	if resources == nil {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id
	container := create.Container.Linux.Resources
	reply := r.reply.adjust.Linux.Resources

	if mem := resources.Memory; mem != nil {
		if v := mem.GetLimit(); v != nil {
			if err := r.setOwner(id, "memory limit", "", plugin); err != nil {
				return err
			}
			container.Memory.Limit = Int64(v.GetValue())
			reply.Memory.Limit = Int64(v.GetValue())
		}
		if v := mem.GetReservation(); v != nil {
			if err := r.setOwner(id, "memory reservation", "", plugin); err != nil {
				return err
			}
			container.Memory.Reservation = Int64(v.GetValue())
			reply.Memory.Reservation = Int64(v.GetValue())
		}
		if v := mem.GetSwap(); v != nil {
			if err := r.setOwner(id, "memory swap limit", "", plugin); err != nil {
				return err
			}
			container.Memory.Swap = Int64(v.GetValue())
			reply.Memory.Swap = Int64(v.GetValue())
		}
		if v := mem.GetKernel(); v != nil {
			if err := r.setOwner(id, "memory kernel limit", "", plugin); err != nil {
				return err
			}
			container.Memory.Kernel = Int64(v.GetValue())
			reply.Memory.Kernel = Int64(v.GetValue())
		}
		if v := mem.GetKernelTcp(); v != nil {
			if err := r.setOwner(id, "memory kernel TCP limit", "", plugin); err != nil {
				return err
			}
			container.Memory.KernelTcp = Int64(v.GetValue())
			reply.Memory.KernelTcp = Int64(v.GetValue())
		}
		if v := mem.GetSwappiness(); v != nil {
			if err := r.setOwner(id, "memory swappiness", "", plugin); err != nil {
				return err
			}
			container.Memory.Swappiness = UInt64(v.GetValue())
			reply.Memory.Swappiness = UInt64(v.GetValue())
		}
		if v := mem.GetDisableOomKiller(); v != nil {
			if err := r.setOwner(id, "memory disable-OOM-killer", "", plugin); err != nil {
				return err
			}
			container.Memory.DisableOomKiller = Bool(v.GetValue())
			reply.Memory.DisableOomKiller = Bool(v.GetValue())
		}
		if v := mem.GetUseHierarchy(); v != nil {
			if err := r.setOwner(id, "memory use-hierarchy", "", plugin); err != nil {
				return err
			}
			container.Memory.UseHierarchy = Bool(v.GetValue())
			reply.Memory.UseHierarchy = Bool(v.GetValue())
		}
	}
	if cpu := resources.Cpu; cpu != nil {
		if v := cpu.GetShares(); v != nil {
			if err := r.setOwner(id, "CPU shares", "", plugin); err != nil {
				return err
			}
			container.Cpu.Shares = UInt64(v.GetValue())
			reply.Cpu.Shares = UInt64(v.GetValue())
		}
		if v := cpu.GetQuota(); v != nil {
			if err := r.setOwner(id, "CPU quota", "", plugin); err != nil {
				return err
			}
			container.Cpu.Quota = Int64(v.GetValue())
			reply.Cpu.Quota = Int64(v.GetValue())
		}
		if v := cpu.GetPeriod(); v != nil {
			if err := r.setOwner(id, "CPU period", "", plugin); err != nil {
				return err
			}
			container.Cpu.Period = UInt64(v.GetValue())
			reply.Cpu.Period = UInt64(v.GetValue())
		}
		if v := cpu.GetRealtimeRuntime(); v != nil {
			if err := r.setOwner(id, "CPU realtime runtime", "", plugin); err != nil {
				return err
			}
			container.Cpu.RealtimeRuntime = Int64(v.GetValue())
			reply.Cpu.RealtimeRuntime = Int64(v.GetValue())
		}
		if v := cpu.GetRealtimePeriod(); v != nil {
			if err := r.setOwner(id, "CPU realtime period", "", plugin); err != nil {
				return err
			}
			container.Cpu.RealtimePeriod = UInt64(v.GetValue())
			reply.Cpu.RealtimePeriod = UInt64(v.GetValue())
		}
		if v := cpu.GetCpus(); v != "" {
			if err := r.setOwner(id, "cpuset CPUs", "", plugin); err != nil {
				return err
			}
			container.Cpu.Cpus = v
			reply.Cpu.Cpus = v
		}
		if v := cpu.GetMems(); v != "" {
			if err := r.setOwner(id, "cpuset memory", "", plugin); err != nil {
				return err
			}
			container.Cpu.Mems = v
			reply.Cpu.Mems = v
		}
	}

	for _, l := range resources.HugepageLimits {
		if err := r.setOwner(id, "hugepage limit", l.PageSize, plugin); err != nil {
			return err
		}
		container.HugepageLimits = append(container.HugepageLimits, l)
		reply.HugepageLimits = append(reply.HugepageLimits, l)
	}

	if v := resources.GetBlockioClass(); v != nil {
		if err := r.setOwner(id, "Block I/O class", "", plugin); err != nil {
			return err
		}
		container.BlockioClass = String(v.GetValue())
		reply.BlockioClass = String(v.GetValue())
	}
	if v := resources.GetRdtClass(); v != nil {
		if err := r.setOwner(id, "RDT class", "", plugin); err != nil {
			return err
		}
		container.RdtClass = String(v.GetValue())
		reply.RdtClass = String(v.GetValue())
	}

	return nil
}

func (r *result) adjustCgroupsPath(path, plugin string) error {
	if path == "" {
		return nil
	}

	create, id := r.request.create, r.request.create.Container.Id

	if err := r.setOwner(id, "cgroups path", "", plugin); err != nil {
		return err
	}

	create.Container.Linux.CgroupsPath = path
	r.reply.adjust.Linux.CgroupsPath = path

	return nil
}

func (r *result) updateResources(reply, u *ContainerUpdate, plugin string) error {
	if u.Linux == nil || u.Linux.Resources == nil {
		return nil
	}

	var resources *LinuxResources
	request, id := r.request.update, u.ContainerId

	// operate on a copy: we won't touch anything on (ignored) failures
	if request != nil && request.Container.Id == id {
		resources = request.LinuxResources.Copy()
	} else {
		resources = reply.Linux.Resources.Copy()
	}

	if mem := u.Linux.Resources.Memory; mem != nil {
		if v := mem.GetLimit(); v != nil {
			if err := r.setOwner(id, "memory limit", "", plugin); err != nil {
				return err
			}
			resources.Memory.Limit = Int64(v.GetValue())
		}
		if v := mem.GetReservation(); v != nil {
			if err := r.setOwner(id, "memory reservation", "", plugin); err != nil {
				return err
			}
			resources.Memory.Reservation = Int64(v.GetValue())
		}
		if v := mem.GetSwap(); v != nil {
			if err := r.setOwner(id, "memory swap limit", "", plugin); err != nil {
				return err
			}
			resources.Memory.Swap = Int64(v.GetValue())
		}
		if v := mem.GetKernel(); v != nil {
			if err := r.setOwner(id, "memory kernel limit", "", plugin); err != nil {
				return err
			}
			resources.Memory.Kernel = Int64(v.GetValue())
		}
		if v := mem.GetKernelTcp(); v != nil {
			if err := r.setOwner(id, "memory kernel TCP limit", "", plugin); err != nil {
				return err
			}
			resources.Memory.KernelTcp = Int64(v.GetValue())
		}
		if v := mem.GetSwappiness(); v != nil {
			if err := r.setOwner(id, "memory swappiness", "", plugin); err != nil {
				return err
			}
			resources.Memory.Swappiness = UInt64(v.GetValue())
		}
		if v := mem.GetDisableOomKiller(); v != nil {
			if err := r.setOwner(id, "memory disable-OOM-killer", "", plugin); err != nil {
				return err
			}
			resources.Memory.DisableOomKiller = Bool(v.GetValue())
		}
		if v := mem.GetUseHierarchy(); v != nil {
			if err := r.setOwner(id, "memory use-hierarchy", "", plugin); err != nil {
				return err
			}
			resources.Memory.UseHierarchy = Bool(v.GetValue())
		}
	}
	if cpu := u.Linux.Resources.Cpu; cpu != nil {
		if v := cpu.GetShares(); v != nil {
			if err := r.setOwner(id, "CPU shares", "", plugin); err != nil {
				return err
			}
			resources.Cpu.Shares = UInt64(v.GetValue())
		}
		if v := cpu.GetQuota(); v != nil {
			if err := r.setOwner(id, "CPU quota", "", plugin); err != nil {
				return err
			}
			resources.Cpu.Quota = Int64(v.GetValue())
		}
		if v := cpu.GetPeriod(); v != nil {
			if err := r.setOwner(id, "CPU period", "", plugin); err != nil {
				return err
			}
			resources.Cpu.Period = UInt64(v.GetValue())
		}
		if v := cpu.GetRealtimeRuntime(); v != nil {
			if err := r.setOwner(id, "CPU realtime runtime", "", plugin); err != nil {
				return err
			}
			resources.Cpu.RealtimeRuntime = Int64(v.GetValue())
		}
		if v := cpu.GetRealtimePeriod(); v != nil {
			if err := r.setOwner(id, "CPU realtime period", "", plugin); err != nil {
				return err
			}
			resources.Cpu.RealtimePeriod = UInt64(v.GetValue())
		}
		if v := cpu.GetCpus(); v != "" {
			if err := r.setOwner(id, "cpuset CPUs", "", plugin); err != nil {
				return err
			}
			resources.Cpu.Cpus = v
		}
		if v := cpu.GetMems(); v != "" {
			if err := r.setOwner(id, "cpuset memory", "", plugin); err != nil {
				return err
			}
			resources.Cpu.Mems = v
		}
	}

	for _, l := range u.Linux.Resources.HugepageLimits {
		if err := r.setOwner(id, "hugepage limit", l.PageSize, plugin); err != nil {
			return err
		}
		resources.HugepageLimits = append(resources.HugepageLimits, l)
	}

	if v := u.Linux.Resources.GetBlockioClass(); v != nil {
		if err := r.setOwner(id, "Block I/O class", "", plugin); err != nil {
			return err
		}
		resources.BlockioClass = String(v.GetValue())
	}
	if v := u.Linux.Resources.GetRdtClass(); v != nil {
		if err := r.setOwner(id, "RDT class", "", plugin); err != nil {
			return err
		}
		resources.RdtClass = String(v.GetValue())
	}

	// update request/reply from copy on success
	*reply.Linux.Resources = *resources

	if request != nil && request.Container.Id == id {
		request.LinuxResources = resources.Copy()
	}

	return nil
}

func (r *result) getContainerUpdate(u *ContainerUpdate, plugin string) (*ContainerUpdate, error) {
	id := u.ContainerId
	if r.request.create != nil && r.request.create.Container != nil {
		if r.request.create.Container.Id == id {
			return nil, errors.Errorf("plugin %q asked update of %q during creation",
				plugin, id)
		}
	}

	if update, ok := r.updates[id]; ok {
		update.IgnoreFailure = update.IgnoreFailure && u.IgnoreFailure
		return update, nil
	}

	update := &ContainerUpdate{
		ContainerId: id,
		Linux: &LinuxContainerUpdate{
			Resources: &LinuxResources{
				Memory:         &LinuxMemory{},
				Cpu:            &LinuxCPU{},
				HugepageLimits: []*HugepageLimit{},
			},
		},
		IgnoreFailure: u.IgnoreFailure,
	}

	r.updates[id] = update

	// for update requests delay appending the requested container (in the response getter)
	if r.request.update != nil && r.request.update.Container.Id != id {
		r.reply.update = append(r.reply.update, update)
	}

	return update, nil
}

func (r *result) setOwner(id, kind, qualif, plugin string) error {
	key := r.makeKey(id, kind, qualif)
	if owner, ok := r.setters[key]; ok {
		if qualif != "" {
			kind = fmt.Sprintf("%s %q", kind, qualif)
		}
		return errors.Errorf("plugins %q and %q both tried to set %s of container %q",
			owner, plugin, kind, id)
	}
	r.setters[key] = plugin
	return nil
}

func (r *result) clearOwner(id, kind, qualif, plugin string) {
	key := r.makeKey(id, kind, qualif)
	delete(r.setters, key)
}

func (r *result) makeKey(id, kind, qualif string) string {
	if id == "" {
		if r.request.create != nil {
			id = r.request.create.Container.Id
		} else if r.request.update != nil {
			id = r.request.update.Container.Id
		}
	}
	return id + ":" + kind + "/" + qualif
}
