package nri

import (
	nri "github.com/containerd/nri/v2alpha1/pkg/adaptation"
)

// PodSandbox interface for interacting with NRI.
type PodSandbox interface {
	GetDomain() string

	GetID() string
	GetName() string
	GetUID() string
	GetNamespace() string
	GetLabels() map[string]string
	GetAnnotations() map[string]string
	GetCgroupParent() string
	GetRuntimeHandler() string
}

func podSandboxToNRI(pod PodSandbox) *nri.PodSandbox {
	return &nri.PodSandbox{
		Id:             pod.GetID(),
		Name:           pod.GetName(),
		Uid:            pod.GetUID(),
		Namespace:      pod.GetNamespace(),
		Labels:         pod.GetLabels(),
		Annotations:    pod.GetAnnotations(),
		CgroupParent:   pod.GetCgroupParent(),
		RuntimeHandler: pod.GetRuntimeHandler(),
	}
}

func podSandboxesToNRI(podList []PodSandbox) []*nri.PodSandbox {
	pods := []*nri.PodSandbox{}
	for _, pod := range podList {
		pods = append(pods, podSandboxToNRI(pod))
	}
	return pods
}
