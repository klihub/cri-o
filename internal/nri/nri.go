package nri

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	config "github.com/cri-o/cri-o/internal/config/nri"
	"github.com/cri-o/cri-o/internal/log"
	"github.com/sirupsen/logrus"

	nri "github.com/containerd/nri/pkg/adaptation"
	"github.com/cri-o/cri-o/internal/version"
)

// API provides an API for interfacing NRI from the rest of cri-o. It is
// agnostic to the internal representation of pods and containers. A
// corresponding Domain interface provides the abstraction for those
// functions where such knowledge is necessary. server.Server registers
// this Domain interface for us.
//
// Since we only deal with CRI pods and containers in cri-o, this split
// to two separate interfaces is artificial. The reason we have it is to
// keep the NRI adaptation structurally as close to other runtimes as
// possible, with the aim to lower the overall mental cost of maintaining
// the runtime adaptations.
type API interface {
	// IsEnabled returns true if the NRI interface is enabled and initialized.
	IsEnabled() bool

	// Start start the NRI interface, allowing external NRI plugins to
	// connect, register, and hook themselves into the lifecycle events
	// of pods and containers.
	Start() error

	// Stop stops the NRI interface.
	Stop()

	// RunPodSandbox relays pod creation events to NRI.
	RunPodSandbox(context.Context, PodSandbox) error

	// StopPodSandbox relays pod shutdown events to NRI.
	StopPodSandbox(context.Context, PodSandbox) error

	// RemovePodSandbox relays pod removal events to NRI.
	RemovePodSandbox(context.Context, PodSandbox) error

	// CreateContainer relays container creation requests to NRI.
	CreateContainer(context.Context, PodSandbox, Container) (*nri.ContainerAdjustment, error)

	// PostCreateContainer relays successful container creation events to NRI.
	PostCreateContainer(context.Context, PodSandbox, Container) error

	// StartContainer relays container start request notifications to NRI.
	StartContainer(context.Context, PodSandbox, Container) error

	// PostStartContainer relays successful container startup events to NRI.
	PostStartContainer(context.Context, PodSandbox, Container) error

	// UpdateContainer relays container update requests to NRI.
	UpdateContainer(context.Context, PodSandbox, Container, *nri.LinuxResources) (*nri.LinuxResources, error)

	// PostUpdateContainer relays successful container update events to NRI.
	PostUpdateContainer(context.Context, PodSandbox, Container) error

	// StopContainer relays container stop requests to NRI.
	StopContainer(context.Context, PodSandbox, Container) error

	// NotifyContainerExit tiggers an NRI stop request for a container NRI event.
	NotifyContainerExit(context.Context, PodSandbox, Container)

	// StopContainer relays container removal events to NRI.
	RemoveContainer(context.Context, PodSandbox, Container) error
}

type State int

const (
	Created State = iota + 1
	Running
	Stopped
	Removed
)

type local struct {
	sync.Mutex
	cfg *config.Config
	nri *nri.Adaptation

	state map[string]State

	exitC   chan *exitEvent
	updateC chan []*nri.ContainerUpdate
	stopC   chan struct{}
}

type exitEvent struct {
	pod PodSandbox
	ctr Container
}

var _ API = &local{}

func (l *local) Lock(caller ...string) {
	if len(caller) > 0 {
		logrus.Infof("locking NRI for %s", strings.Join(caller, " "))
	}
	l.Mutex.Lock()
	if len(caller) > 0 {
		logrus.Infof("locked NRI for %s", strings.Join(caller, " "))
	}
}

func (l *local) Unlock(caller ...string) {
	l.Mutex.Unlock()
	if len(caller) > 0 {
		logrus.Infof("unlocked NRI for %s", strings.Join(caller, " "))
	}
}

func (l *local) setupUpdater() {
	l.updateC = make(chan []*nri.ContainerUpdate, 32)
	l.stopC = make(chan struct{})

	go l.asyncUpdater()
}

func (l *local) asyncUpdater() {
	for {
		ctx := context.Background()
		select {
		case _ = <-l.stopC:
			return
		case updates, ok := <-l.updateC:
			if ok {
				if _, err := l.applyUpdates(ctx, updates); err != nil {
					logrus.Warnf("container update failed: %v", err)
				}
			}
		}
	}
}

func (l *local) asyncUpdate(updates []*nri.ContainerUpdate) {
	l.updateC <- updates
}

func (l *local) setupExitHandler() {
	l.exitC = make(chan *exitEvent, 32)

	go l.asyncExitHandler()
}

func (l *local) asyncExitHandler() {
	for {
		ctx := context.Background()
		select {
		case _ = <-l.stopC:
			return
		case e, ok := <-l.exitC:
			if ok {
				l.Lock("ExitEvent", e.ctr.GetID())
				l.stopContainer(ctx, e.pod, e.ctr)
				l.Unlock("ExitEvent", e.ctr.GetID())
			}
		}
	}
}

func (l *local) asyncExit(pod PodSandbox, ctr Container) {
	go func() {
		time.Sleep(1 * time.Second)
		l.exitC <- &exitEvent{
			pod: pod,
			ctr: ctr,
		}
	}()
}

// New creates an instance of the NRI interface with the given configuration.
func New(cfg *config.Config) (*local, error) {
	logrus.Info("Create NRI interface")

	l := &local{
		cfg: cfg,
	}

	if !cfg.Enabled {
		logrus.Info("NRI interface is disabled in the configuration.")
		return l, nil
	}

	vInfo, err := version.Get(false)
	if err != nil {
		return nil, fmt.Errorf("failed to determine version: %v", err)
	}

	var (
		runtimeName    = "cri-o"
		runtimeVersion = vInfo.Version
		opts           = cfg.ToOptions()
		syncFn         = l.syncPlugin
		updateFn       = l.updateFromPlugin
	)

	cfg.ConfigureTimeouts()

	l.nri, err = nri.New(runtimeName, runtimeVersion, syncFn, updateFn, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize NRI interface: %w", err)
	}

	l.state = make(map[string]State)

	l.setupUpdater()
	l.setupExitHandler()

	return l, nil
}

func (l *local) Start() error {
	if !l.IsEnabled() {
		return nil
	}

	if err := l.nri.Start(); err != nil {
		return fmt.Errorf("failed to start NRI interface: %w", err)
	}

	return nil
}

func (l *local) Stop() {
	if !l.IsEnabled() {
		return
	}

	l.Lock()
	defer l.Unlock()

	close(l.stopC)
	l.nri.Stop()
	l.nri = nil
}

func (l *local) RunPodSandbox(ctx context.Context, pod PodSandbox) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("RunPodSandbox", pod.GetID())
	defer l.Unlock("RunPodSandbox", pod.GetID())

	request := &nri.RunPodSandboxRequest{
		Pod: podSandboxToNRI(pod),
	}

	err := l.nri.RunPodSandbox(ctx, request)

	return err
}

func (l *local) StopPodSandbox(ctx context.Context, pod PodSandbox) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("StopPodSandbox", pod.GetID())
	defer l.Unlock("StopPodSandbox", pod.GetID())

	request := &nri.StopPodSandboxRequest{
		Pod: podSandboxToNRI(pod),
	}

	err := l.nri.StopPodSandbox(ctx, request)

	return err
}

func (l *local) RemovePodSandbox(ctx context.Context, pod PodSandbox) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("RemovePodSandbox", pod.GetID())
	defer l.Unlock("RemovePodSandbox", pod.GetID())

	request := &nri.RemovePodSandboxRequest{
		Pod: podSandboxToNRI(pod),
	}

	err := l.nri.RemovePodSandbox(ctx, request)

	return err
}

func (l *local) CreateContainer(ctx context.Context, pod PodSandbox, ctr Container) (*nri.ContainerAdjustment, error) {
	if !l.IsEnabled() {
		return nil, nil
	}

	l.Lock("CreateContainer", ctr.GetID())
	defer l.Unlock("CreateContainer", ctr.GetID())

	request := &nri.CreateContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	response, err := l.nri.CreateContainer(ctx, request)
	l.setState(request.Container.Id, Created)
	if err != nil {
		return nil, err
	}

	l.asyncUpdate(response.Update)

	return response.Adjust, nil
}

func (l *local) PostCreateContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("PostCreateContainer", ctr.GetID())
	defer l.Unlock("PostCreateContainer", ctr.GetID())

	request := &nri.PostCreateContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	err := l.nri.PostCreateContainer(ctx, request)

	return err
}

func (l *local) StartContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("StartContainer", ctr.GetID())
	defer l.Unlock("StartContainer", ctr.GetID())

	request := &nri.StartContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	err := l.nri.StartContainer(ctx, request)

	l.setState(request.Container.Id, Running)

	return err
}

func (l *local) PostStartContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("PostStartContainer", ctr.GetID())
	defer l.Unlock("PostStartContainer", ctr.GetID())

	request := &nri.PostStartContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	err := l.nri.PostStartContainer(ctx, request)

	return err
}

func (l *local) UpdateContainer(ctx context.Context, pod PodSandbox, ctr Container, req *nri.LinuxResources) (*nri.LinuxResources, error) {
	if !l.IsEnabled() {
		return nil, nil
	}

	l.Lock("UpdateContainer", ctr.GetID())
	defer l.Unlock("UpdateContainer", ctr.GetID())

	request := &nri.UpdateContainerRequest{
		Pod:            podSandboxToNRI(pod),
		Container:      containerToNRI(ctr),
		LinuxResources: req,
	}

	response, err := l.nri.UpdateContainer(ctx, request)
	if err != nil {
		return nil, err
	}

	go func() {
		_, err = l.evictContainers(ctx, response.Evict)
		if err != nil {
			logrus.Warnf("pre-create eviction failed: %v", err)
		}
	}()

	cnt := len(response.Update)
	if cnt == 0 {
		return nil, nil
	}

	if cnt > 1 {
		l.asyncUpdate(response.Update[0 : cnt-1])
	}

	return response.Update[cnt-1].GetLinux().GetResources(), nil
}

func (l *local) PostUpdateContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("PostUpdateContainer", ctr.GetID())
	defer l.Unlock("PostUpdateContainer", ctr.GetID())

	request := &nri.PostUpdateContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	err := l.nri.PostUpdateContainer(ctx, request)

	return err
}

func (l *local) StopContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("StopContainer", ctr.GetID())
	defer l.Unlock("StopContainer", ctr.GetID())

	return l.stopContainer(ctx, pod, ctr)
}

func (l *local) stopContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.needsStopping(ctr.GetID()) {
		return nil
	}

	request := &nri.StopContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	response, err := l.nri.StopContainer(ctx, request)
	l.setState(request.Container.Id, Stopped)
	if err != nil {
		return err
	}

	l.asyncUpdate(response.Update)

	return err
}

func (l *local) NotifyContainerExit(ctx context.Context, pod PodSandbox, ctr Container) {
	l.asyncExit(pod, ctr)
}

func (l *local) RemoveContainer(ctx context.Context, pod PodSandbox, ctr Container) error {
	if !l.IsEnabled() {
		return nil
	}

	l.Lock("RemoveContainer", ctr.GetID())
	defer l.Unlock("RemoveContainer", ctr.GetID())

	if !l.needsRemoval(ctr.GetID()) {
		return nil
	}

	if err := l.stopContainer(ctx, pod, ctr); err != nil {
		log.Warnf(ctx, "Container NRI stop request failed: %v", err)
	}

	request := &nri.RemoveContainerRequest{
		Pod:       podSandboxToNRI(pod),
		Container: containerToNRI(ctr),
	}

	err := l.nri.RemoveContainer(ctx, request)
	l.setState(request.Container.Id, Removed)

	return err
}

func (l *local) IsEnabled() bool {
	return l != nil && l.cfg.Enabled
}

func (l *local) syncPlugin(ctx context.Context, syncFn nri.SyncCB) error {
	l.Lock("syncPlugin")
	defer l.Unlock("syncPlugin")

	log.Infof(ctx, "Synchronizing NRI (plugin) with current runtime state")

	pods := podSandboxesToNRI(domains.listPodSandboxes())
	containers := containersToNRI(domains.listContainers())

	for _, ctr := range containers {
		switch ctr.GetState() {
		case nri.ContainerState_CONTAINER_CREATED:
			l.setState(ctr.GetId(), Created)
		case nri.ContainerState_CONTAINER_RUNNING, nri.ContainerState_CONTAINER_PAUSED:
			l.setState(ctr.GetId(), Running)
		case nri.ContainerState_CONTAINER_STOPPED:
			l.setState(ctr.GetId(), Stopped)
		default:
			l.setState(ctr.GetId(), Removed)
		}
	}

	updates, err := syncFn(ctx, pods, containers)
	if err != nil {
		return err
	}

	_, err = l.applyUpdates(ctx, updates)
	if err != nil {
		return err
	}

	return nil
}

func (l *local) updateFromPlugin(ctx context.Context, req []*nri.ContainerUpdate) ([]*nri.ContainerUpdate, error) {
	l.Lock("updateFromPlugin")
	defer l.Unlock("updateFromPlugin")

	log.Infof(ctx, "Unsolicited container update from NRI")

	failed, err := l.applyUpdates(ctx, req)
	return failed, err
}

func (l *local) applyUpdates(ctx context.Context, updates []*nri.ContainerUpdate) ([]*nri.ContainerUpdate, error) {
	failed, err := domains.updateContainers(ctx, updates)
	return failed, err
}

func (l *local) evictContainers(ctx context.Context, evict []*nri.ContainerEviction) ([]*nri.ContainerEviction, error) {
	failed, err := domains.evictContainers(ctx, evict)
	return failed, err
}

func (l *local) setState(id string, state State) {
	if state != Removed {
		l.state[id] = state
		return
	}

	delete(l.state, id)
}

func (l *local) needsStopping(id string) bool {
	s := l.getState(id)
	if s == Created || s == Running {
		return true
	}
	return false
}

func (l *local) needsRemoval(id string) bool {
	s := l.getState(id)
	if s == Created || s == Running || s == Stopped {
		return true
	}
	return false
}

func (l *local) getState(id string) State {
	if state, ok := l.state[id]; ok {
		return state
	}

	return Removed
}
