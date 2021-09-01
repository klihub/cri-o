package server

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cri-o/cri-o/internal/lib/sandbox"
	"github.com/cri-o/cri-o/internal/log"
	"github.com/cri-o/cri-o/internal/oci"
	critypes "github.com/cri-o/cri-o/server/cri/types"

	"github.com/containers/podman/v3/pkg/annotations"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	nrilog "github.com/containerd/nri/v2alpha1/pkg/log"
	nri "github.com/containerd/nri/v2alpha1/pkg/runtime"
	"github.com/containerd/nri/v2alpha1/pkg/runtime/adapt"
	"github.com/sirupsen/logrus"
)

// nriRuntime is the cri-o/NRI adaptation.
type nriRuntime struct {
	lock   sync.Mutex   // serialize NRI request processing
	server *Server      // associated server
	nri    *nri.Runtime // NRI runtime abstraction

	// pending updates for containers being created
	pending map[string][]*nri.ContainerUpdate
	// current state of containers as reported by us to plugins
	status map[string]specs.ContainerState
}

// Set up CRI-O server/NRI adaptation.
func (s *Server) setupNRI(name, version string) error {
	if !s.Config().NRI.Enabled {
		logrus.Infof("NRI is disabled")
		return nil
	}

	nrilog.Set(&nriLogger{})

	n := &nriRuntime{
		server:  s,
		pending: make(map[string][]*nri.ContainerUpdate),
		status:  make(map[string]specs.ContainerState),
	}

	runtime, err := nri.New(name, version, n.synchronize, n.updateContainers,
		nri.WithConfig(s.Config().NRI.Config()),
		nri.WithPluginPath(s.Config().NRI.PluginPath),
		nri.WithSocketPath(s.Config().NRI.SocketPath))

	if err != nil {
		return errors.Wrap(err, "failed to set up NRI adaptation")
	}

	n.nri = runtime
	s.nri = n

	n.startContainerMonitor()

	if err := n.nri.Start(); err != nil {
		return errors.Wrap(err, "failed to start NRI runtime")
	}

	return nil
}

func (n *nriRuntime) isEnabled() bool {
	return n != nil
}

// Local hack for container state monitoring.
// XXX TODO:
//     Probably StartExitMonitor() should be improved instead
//     to allow listening/subscribing for exit events as those
//     are the state changes we really should care about.
func (n *nriRuntime) startContainerMonitor() {
	ctx := context.Background()

	n.lock.Lock()
	ctrList, err := n.server.ContainerServer.ListContainers()
	if err == nil {
		for _, octr := range ctrList {
			if !octr.Created() {
				continue
			}
			n.setStatus(octr.ID(), octr.State().Status)
		}
	}
	n.lock.Unlock()

	go func() {
		for {
			time.Sleep(2 * time.Second)
			n.lock.Lock()
			for id, status := range n.status {
				if status != specs.StateRunning {
					continue
				}
				c, err := n.server.GetContainerFromShortID(id)
				if err == nil && c.IsAlive() == nil {
					continue
				}
				logrus.Infof("NRI-Runtime:ContainerMonitor: container %s is gone...", c.ID())
				pod := n.server.getSandbox(c.Sandbox())
				if err := n.stopContainer(ctx, c, nil, pod); err != nil {
					logrus.Warnf("NRI-Runtime:ContainerMonitor: failed to clean up %s: %v",
						c.ID(), err)
				}
			}
			n.lock.Unlock()
		}
	}()
}

// synchronize a plugin with the state of the runtime state.
//
// Notes:
//   It is implicitly assumed here that the result of synchronization
//   is of no concern to any other existing plugins. Generally this
//   should be true, since during synchronisation only native/compute
//   resources (CPU, memory mostly) can be updated and there should be
//   at most 1 plugin assigning those resources to a container.
func (n *nriRuntime) synchronize(ctx context.Context, cb nri.SyncCB) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:synchronize()")

	podList := n.server.ContainerServer.ListSandboxes()
	ctrList, err := n.server.ContainerServer.ListContainers()
	if err != nil {
		return err
	}

	pods := nriPodSandboxSlice(podList)
	containers := nriContainerSlice(ctrList)
	updates, err := cb(ctx, pods, containers)
	if err != nil {
		return err
	}

	_, err = n.applyUpdates(ctx, updates, false)
	if err != nil {
		return err
	}

	return nil
}

// update a set of containers requested by a plugin.
//
// Notes:
//   It is implicitly assumed here that the result of unsolicited
//   container adjustment by any plugin is of no concern to any
//   other plugins.
func (n *nriRuntime) updateContainers(ctx context.Context, req []*nri.ContainerUpdate) ([]*nri.ContainerUpdate, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:adjustContainers()")

	failed, err := n.applyUpdates(ctx, req, true)

	return failed, err
}

// RunPodSandbox relays pod creation requests to NRI/plugins.
func (n *nriRuntime) RunPodSandbox(ctx context.Context, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:RunPodSandbox(%s)", pod.ID())

	request := &nri.RunPodSandboxRequest{
		Pod: adaptPodSandbox(pod),
	}

	err := n.nri.RunPodSandbox(ctx, request)

	return err
}

// StopPodSandbox relays pod stop requests to NRI/plugins.
func (n *nriRuntime) StopPodSandbox(ctx context.Context, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:StopPodSandbox(%s)", pod.ID())

	request := &nri.StopPodSandboxRequest{
		Pod: adaptPodSandbox(pod),
	}

	err := n.nri.StopPodSandbox(ctx, request)

	return err
}

// RemovePodSandbox relays pod removal requests to NRI/plugins.
func (n *nriRuntime) RemovePodSandbox(ctx context.Context, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:RemovePodSandbox(%s)", pod.ID())

	request := &nri.RemovePodSandboxRequest{
		Pod: adaptPodSandbox(pod),
	}

	err := n.nri.RemovePodSandbox(ctx, request)

	return err
}

// CreateContainer relays container creation requests to NRI/plugins.
func (n *nriRuntime) CreateContainer(ctx context.Context, spec *specs.Spec, octr *oci.Container, pod *sandbox.Sandbox) (*nri.ContainerAdjustment, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:CreateContainer(%s)", octr.ID())

	request := &nri.CreateContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, spec),
	}

	response, err := n.nri.CreateContainer(ctx, request)
	if err != nil {
		return nil, err
	}

	_, err = n.applyUpdates(ctx, response.Update, false)
	if err != nil {
		return nil, err
	}

	n.setStatus(octr.ID(), specs.StateCreated)

	return response.Adjust, nil
}

func (n *nriRuntime) RollbackCreateContainer(ctx context.Context, spec *specs.Spec, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.stopContainer(ctx, octr, spec, pod)
}

// PostCreateContainer relays container post-create events to NRI/plugins.
func (n *nriRuntime) PostCreateContainer(ctx context.Context, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:PostCreateContainer(%s)", octr.ID())

	request := &nri.PostCreateContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, nil),
	}

	err := n.nri.PostCreateContainer(ctx, request)

	return err
}

// StartContainer relays container start requests to NRI/plugins.
func (n *nriRuntime) StartContainer(ctx context.Context, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:StartContainer(%s)", octr.ID())

	request := &nri.StartContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, nil),
	}

	err := n.nri.StartContainer(ctx, request)

	n.setStatus(octr.ID(), specs.StateRunning)

	return err
}

// PostStartContainer relays container post-start events to NRI/plugins.
func (n *nriRuntime) PostStartContainer(ctx context.Context, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:PostStartContainer(%s)", octr.ID())

	request := &nri.PostStartContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, nil),
	}

	err := n.nri.PostStartContainer(ctx, request)

	return err
}

// UpdateContainer relays container update requests to NRI/plugins.
func (n *nriRuntime) UpdateContainer(ctx context.Context, octr *oci.Container, r *critypes.LinuxContainerResources) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:UpdateContainer(%s)", octr.ID())

	pod := n.server.getSandbox(octr.Sandbox())

	request := &nri.UpdateContainerRequest{
		Pod:            adaptPodSandbox(pod),
		Container:      adaptContainer(octr, nil),
		LinuxResources: nriLinuxResources(r),
	}

	response, err := n.nri.UpdateContainer(ctx, request)
	if err != nil {
		return err
	}

	_, err = n.applyUpdates(ctx, response.Update, false)

	return err
}

// PostUpdateContainer relays container post-update events to NRI/plugins.
func (n *nriRuntime) PostUpdateContainer(ctx context.Context, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:PostUpdateContainer(%s)", octr.ID())

	request := &nri.PostUpdateContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, nil),
	}

	err := n.nri.PostUpdateContainer(ctx, request)

	return err
}

// StopContainer relays container stop requests to NRI/plugins.
func (n *nriRuntime) StopContainer(ctx context.Context, octr *oci.Container, pod *sandbox.Sandbox) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.stopContainer(ctx, octr, nil, pod)
}

// stopContainer relays container stop requests to NRI/plugins.
func (n *nriRuntime) stopContainer(ctx context.Context, octr *oci.Container, spec *specs.Spec, pod *sandbox.Sandbox) error {
	log.Infof(ctx, "NRI-Runtime:StopContainer(%s)", octr.ID())

	request := &nri.StopContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, spec),
	}

	response, err := n.nri.StopContainer(ctx, request)
	if err != nil {
		return err
	}

	n.setStatus(octr.ID(), specs.StateStopped)

	_, err = n.applyUpdates(ctx, response.Update, false)
	return err
}

// RemoveContainer relays container removal requests to NRI/plugins.
func (n *nriRuntime) RemoveContainer(ctx context.Context, octr *oci.Container) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:RemoveContainer(%s)", octr.ID())

	delete(n.pending, octr.ID())

	pod := n.server.getSandbox(octr.Sandbox())

	status, _ := n.getStatus(octr.ID())
	if status != specs.StateStopped {
		err := n.stopContainer(ctx, octr, nil, pod)
		if err != nil {
			log.Warnf(ctx, "NRI-Runtime:RemoveContainer(%s) failed to clean up: %v",
				octr.ID(), err)
		}
	}

	request := &nri.RemoveContainerRequest{
		Pod:       adaptPodSandbox(pod),
		Container: adaptContainer(octr, nil),
	}

	err := n.nri.RemoveContainer(ctx, request)

	n.setStatus(octr.ID(), specs.ContainerState(""))

	return err
}

func (n *nriRuntime) setStatus(containerID string, status specs.ContainerState) {
	prev := n.status[containerID]
	if prev == "" {
		prev = "unknown"
	}
	curr := status
	switch status {
	case "":
		delete(n.status, containerID)
		curr = "exited"
	default:
		n.status[containerID] = status
	}
	logrus.Infof("NRI-Runtime: container %s state change: %s -> %s", containerID, prev, curr)
}

func (n *nriRuntime) getStatus(containerID string) (status specs.ContainerState, ok bool) {
	status, ok = n.status[containerID]
	return
}

// ApplyPendingUpdates applies any pending updates to the given container.
func (n *nriRuntime) ApplyPendingUpdates(ctx context.Context, containerID string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Infof(ctx, "NRI-Runtime:ApplyPendingUpdates(%s)", containerID)

	updates, ok := n.pending[containerID]
	if !ok {
		return nil
	}

	delete(n.pending, containerID)
	_, err := n.applyUpdates(ctx, updates, false)
	return err
}

func (n *nriRuntime) applyUpdates(ctx context.Context, updates []*nri.ContainerUpdate, collectFailed bool) ([]*nri.ContainerUpdate, error) {
	var failed []*nri.ContainerUpdate

	for _, u := range updates {
		if u.Linux == nil || u.Linux.Resources == nil {
			continue
		}

		if pending, ok := n.pending[u.ContainerId]; ok {
			n.pending[u.ContainerId] = append(pending, u)
			continue
		}

		octr, err := n.server.GetContainerFromShortID(u.ContainerId)
		if err != nil {
			log.Warnf(ctx, "Failed to find container %s to update", u.ContainerId)
			continue
		}

		err = n.updateContainer(ctx, octr, u)
		if err != nil {
			if !collectFailed {
				return nil, err
			} else {
				failed = append(failed, u)
			}
		}

		log.Infof(ctx, "=> apply updates to container %s", u.ContainerId)
	}

	if len(failed) > 0 {
		return failed, errors.New("some containers failed to update")
	}

	return nil, nil
}

func (n *nriRuntime) updateContainer(ctx context.Context, octr *oci.Container, u *nri.ContainerUpdate) error {
	state := octr.State().Status
	if state != oci.ContainerStateRunning && state != oci.ContainerStateCreated {
		return nil
	}

	resources := u.Linux.Resources.ToOCI()
	err := n.server.Runtime().UpdateContainer(ctx, octr, resources)
	if err != nil {
		if u.IgnoreFailure {
			return nil
		}
		return errors.Wrapf(err, "failed to update container %s", u.ContainerId)
	}

	n.server.UpdateContainerLinuxResources(octr, resources)
	return nil
}

//
// Runtime/NRI type adaptation
//

func nriPodSandbox(pod *sandbox.Sandbox) *nri.PodSandbox {
	if pod == nil {
		return &nri.PodSandbox{}
	}
	return &nri.PodSandbox{
		Id:             pod.ID(),
		Name:           pod.Metadata().Name,
		Uid:            pod.Metadata().UID,
		Namespace:      pod.Metadata().Namespace,
		Annotations:    pod.Annotations(),
		Labels:         pod.Labels(),
		CgroupParent:   pod.CgroupParent(),
		RuntimeHandler: pod.RuntimeHandler(),
	}
}

func nriPodSandboxSlice(podList []*sandbox.Sandbox) []*nri.PodSandbox {
	pods := []*nri.PodSandbox{}
	for _, pod := range podList {
		if !pod.Created() {
			continue
		}
		pods = append(pods, nriPodSandbox(pod))
	}
	return pods
}

func nriContainer(ctr *oci.Container, spec *specs.Spec) *nri.Container {
	if spec == nil {
		ctrSpec := ctr.Spec()
		spec = &ctrSpec
	}
	c := crioContainer{
		spec: spec,
		ctr:  ctr,
	}
	return c.toNRI()
}

func nriContainerSlice(containerList []*oci.Container) []*nri.Container {
	containers := []*nri.Container{}

	for _, octr := range containerList {
		switch octr.State().Status {
		case oci.ContainerStateCreated, oci.ContainerStateRunning, oci.ContainerStatePaused:
		default:
			continue
		}

		containers = append(containers, nriContainer(octr, nil))
	}

	return containers
}

func nriLinuxResources(crir *critypes.LinuxContainerResources) *nri.LinuxResources {
	if crir == nil {
		return nil
	}
	shares, quota, period := uint64(crir.CPUShares), crir.CPUQuota, uint64(crir.CPUPeriod)
	r := &nri.LinuxResources{
		Cpu: &nri.LinuxCPU{
			Shares: nri.UInt64(&shares),
			Quota:  nri.Int64(&quota),
			Period: nri.UInt64(&period),
			Cpus:   crir.CPUsetCPUs,
			Mems:   crir.CPUsetMems,
		},
		Memory: &nri.LinuxMemory{
			Limit: nri.Int64(&crir.MemoryLimitInBytes),
		},
	}
	for _, l := range crir.HugepageLimits {
		r.HugepageLimits = append(r.HugepageLimits,
			&nri.HugepageLimit{
				PageSize: l.PageSize,
				Limit:    l.Limit,
			})
	}
	return r
}

func adaptPodSandbox(pod *sandbox.Sandbox) *nri.PodSandbox {
	return adapt.PodToNRI(&crioPodSandbox{
		Sandbox: pod,
	})
}

func adaptContainer(ctr *oci.Container, spec *specs.Spec) *nri.Container {
	return adapt.ContainerToNRI(&crioContainer{
		ctr:  ctr,
		spec: spec,
	})
}

type crioPodSandbox struct {
	*sandbox.Sandbox
}

func (p *crioPodSandbox) GetID() string {
	return p.ID()
}

func (p *crioPodSandbox) GetName() string {
	return p.Metadata().Name
}

func (p *crioPodSandbox) GetUID() string {
	return p.Metadata().UID
}

func (p *crioPodSandbox) GetNamespace() string {
	return p.Metadata().Namespace
}

func (p *crioPodSandbox) GetAnnotations() map[string]string {
	return p.Annotations()
}

func (p *crioPodSandbox) GetLabels() map[string]string {
	return p.Labels()
}

func (p *crioPodSandbox) GetCgroupParent() string {
	return p.CgroupParent()
}

func (p *crioPodSandbox) GetRuntimeHandler() string {
	return p.RuntimeHandler()
}

type crioContainer struct {
	ctr  *oci.Container
	spec *specs.Spec
}

func (c *crioContainer) toNRI() *nri.Container {
	return &nri.Container{
		Id:           c.GetID(),
		PodSandboxId: c.GetPodSandboxID(),
		Name:         c.GetName(),
		State:        c.GetState(),
		Labels:       c.GetLabels(),
		Annotations:  c.GetAnnotations(),
		Args:         c.GetArgs(),
		Env:          c.GetEnv(),
		Mounts:       c.GetMounts(),
		Hooks:        c.GetHooks(),
		Linux: &nri.LinuxContainer{
			Namespaces:  c.GetLinuxNamespaces(),
			Devices:     c.GetLinuxDevices(),
			Resources:   c.GetLinuxResources(),
			OomScoreAdj: nri.Int(c.GetOOMScoreAdj()),
			CgroupsPath: c.GetCgroupsPath(),
		},
	}
}

func (c *crioContainer) GetSpec() *specs.Spec {
	if c.spec != nil {
		return c.spec
	}
	if c.ctr != nil {
		spec := c.ctr.Spec()
		return &spec
	}
	return &specs.Spec{}
}

func (c *crioContainer) GetID() string {
	return c.GetSpec().Annotations[annotations.ContainerID]
}

func (c *crioContainer) GetPodSandboxID() string {
	return c.GetSpec().Annotations[annotations.SandboxID]
}

func (c *crioContainer) GetName() string {
	return c.GetSpec().Annotations["io.kubernetes.container.name"]
}

func (c *crioContainer) GetState() nri.ContainerState {
	if c.ctr != nil {
		switch c.ctr.State().Status {
		case oci.ContainerStateCreated:
			return nri.ContainerStateCreated
		case oci.ContainerStatePaused:
			return nri.ContainerStatePaused
		case oci.ContainerStateRunning:
			return nri.ContainerStateRunning
		case oci.ContainerStateStopped:
			return nri.ContainerStateStopped
		}
	}

	return nri.ContainerStateUnknown
}

func (c *crioContainer) GetLabels() map[string]string {
	if blob, ok := c.GetSpec().Annotations[annotations.Labels]; ok {
		labels := map[string]string{}
		if err := json.Unmarshal([]byte(blob), &labels); err == nil {
			return labels
		}
	}
	return nil
}

func (c *crioContainer) GetAnnotations() map[string]string {
	return c.GetSpec().Annotations
}

func (c *crioContainer) GetArgs() []string {
	if p := c.GetSpec().Process; p != nil {
		return nri.DupStringSlice(p.Args)
	}
	return nil
}

func (c *crioContainer) GetEnv() []string {
	if p := c.GetSpec().Process; p != nil {
		return nri.DupStringSlice(p.Env)
	}
	return nil
}

func (c *crioContainer) GetMounts() []*nri.Mount {
	return nri.FromOCIMounts(c.GetSpec().Mounts)
}

func (c *crioContainer) GetHooks() *nri.Hooks {
	return nri.FromOCIHooks(c.GetSpec().Hooks)
}

func (c *crioContainer) GetLinuxNamespaces() []*nri.LinuxNamespace {
	spec := c.GetSpec()
	if spec.Linux != nil {
		return nri.FromOCILinuxNamespaces(spec.Linux.Namespaces)
	}
	return nil
}

func (c *crioContainer) GetLinuxDevices() []*nri.LinuxDevice {
	spec := c.GetSpec()
	if spec.Linux != nil {
		return nri.FromOCILinuxDevices(spec.Linux.Devices)
	}
	return nil
}

func (c *crioContainer) GetLinuxResources() *nri.LinuxResources {
	spec := c.GetSpec()
	if spec.Linux == nil {
		return nil
	}
	return nri.FromOCILinuxResources(spec.Linux.Resources, spec.Annotations)
}

func (c *crioContainer) GetOOMScoreAdj() *int {
	if c.GetSpec().Process != nil {
		return c.GetSpec().Process.OOMScoreAdj
	}
	return nil
}

func (c *crioContainer) GetCgroupsPath() string {
	if c.GetSpec().Linux == nil {
		return ""
	}
	return c.GetSpec().Linux.CgroupsPath
}

//
// NRI/Runtime logging adaptation
//

const (
	nriLogPrefix = "NRI: "
)

type nriLogger struct{}

func (*nriLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	log.Debugf(ctx, nriLogPrefix+format, args...)
}

func (*nriLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	log.Infof(ctx, nriLogPrefix+format, args...)
}

func (*nriLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	log.Warnf(ctx, nriLogPrefix+format, args...)
}

func (*nriLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Errorf(ctx, nriLogPrefix+format, args...)
}
