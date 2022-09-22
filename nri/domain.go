package nri

import (
	"context"
	"fmt"
	"sync"

	"github.com/cri-o/cri-o/internal/log"
	"github.com/sirupsen/logrus"

	nri "github.com/containerd/nri/v2alpha1/pkg/adaptation"
)

// Domain implements the functions the generic NRI interface needs to
// deal with pods and containers from a particular containerd namespace.
type Domain interface {
	// GetName() returns the containerd namespace for this domain.
	GetName() string

	// ListPodSandboxes list all pods in this namespace.
	ListPodSandboxes() []PodSandbox

	// ListContainer list all containers in this namespace.
	ListContainers() []Container

	// GetPodSandbox returns the pod for the given ID.
	GetPodSandbox(string) (PodSandbox, bool)

	// GetContainer returns the container for the given ID.
	GetContainer(string) (Container, bool)

	// UpdateContainer applies an NRI container update request in the namespace.
	UpdateContainer(context.Context, *nri.ContainerUpdate) error

	// EvictContainer evicts the requested container in the namespace.
	EvictContainer(context.Context, *nri.ContainerEviction) error
}

// SetDomain registers an NRI domain for a containerd namespace.
func SetDomain(d Domain) {
	err := domains.set(d)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to register namespace %q with NRI", d.GetName())
	}

	logrus.Infof("Registered domain %q with NRI", d.GetName())
}

type domainTable struct {
	sync.Mutex
	domain Domain
}

func (t *domainTable) set(d Domain) error {
	t.Lock()
	defer t.Unlock()

	t.domain = d
	return nil
}

func (t *domainTable) listPodSandboxes() []PodSandbox {
	t.Lock()
	defer t.Unlock()

	return t.domain.ListPodSandboxes()
}

func (t *domainTable) listContainers() []Container {
	t.Lock()
	defer t.Unlock()

	return t.domain.ListContainers()
}

func (t *domainTable) updateContainers(ctx context.Context, updates []*nri.ContainerUpdate) ([]*nri.ContainerUpdate, error) {
	var failed []*nri.ContainerUpdate

	for _, u := range updates {
		err := t.domain.UpdateContainer(ctx, u)
		if err != nil {
			log.Errorf(ctx, "NRI update of container %s failed: %w", u.ContainerId, err)
			if !u.IgnoreFailure {
				failed = append(failed, u)
			}
		}
	}

	if len(failed) != 0 {
		return failed, fmt.Errorf("NRI update of containers failed")
	}

	return nil, nil
}

func (t *domainTable) evictContainers(ctx context.Context, evict []*nri.ContainerEviction) ([]*nri.ContainerEviction, error) {
	var failed []*nri.ContainerEviction

	for _, e := range evict {
		err := t.domain.EvictContainer(ctx, e)
		if err != nil {
			log.Errorf(ctx, "NRI eviction of container %s failed: %w", e.ContainerId, err)
			failed = append(failed, e)
		}
	}

	if len(failed) != 0 {
		return failed, fmt.Errorf("NRI eviction of containers failed")
	}

	return nil, nil
}

var domains = &domainTable{}
