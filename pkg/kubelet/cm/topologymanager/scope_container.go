/*
Copyright 2020 The Kubernetes Authors.

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

package topologymanager

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

type containerScope struct {
	scope
}

// Ensure containerScope implements Scope interface
var _ Scope = &containerScope{}

// NewContainerScope returns a container scope.
func NewContainerScope(policy Policy) Scope {
	return &containerScope{
		scope{
			name:              containerTopologyScope,
			podTopologyHints:  podTopologyHints{},
			policy:            policy,
			podMap:            containermap.NewContainerMap(),
			podFarMemAffinity: map[string]map[string]TopologyHint{},
		},
	}
}

func (s *containerScope) Admit(pod *v1.Pod) lifecycle.PodAdmitResult {
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		bestHint, admit := s.calculateAffinity(pod, &container)
		klog.InfoS("Best TopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name)

		if !admit {
			if IsAlignmentGuaranteed(s.policy) {
				metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Inc()
			}
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			return admission.GetPodAdmitResult(&TopologyAffinityError{})
		}
		klog.InfoS("Topology Affinity", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name)

		farMemMgrHints := s.farMemMgr.GetTopologyHints(pod, &container)
		if len(farMemMgrHints) > 0 {
			fmh := getFarMemHint(farMemMgrHints)

			// If no hint that satisfies the container's far memory request could be found the pod
			// can't be admitted (we hackishly use the Preferred field to encode that).
			if !fmh.Preferred {
				if IsAlignmentGuaranteed(s.policy) {
					metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Inc()
				}
				metrics.TopologyManagerAdmissionErrorsTotal.Inc()
				klog.InfoS("far memory request could not be satisfied",
					"pod", klog.KObj(pod),
					"container", container.Name)
				return admission.GetPodAdmitResult(&TopologyAffinityError{})
			}

			s.setFarMemTopoHint(string(pod.UID), container.Name, fmh)
		}

		s.setTopologyHints(string(pod.UID), container.Name, bestHint)

		err := s.allocateAlignedResources(pod, &container)
		if err != nil {
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			return admission.GetPodAdmitResult(err)
		}

		// Now, allocate far memory.
		err = s.farMemMgr.Allocate(pod, &container)
		if err != nil {
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			return admission.GetPodAdmitResult(err)
		}

		if IsAlignmentGuaranteed(s.policy) {
			klog.V(4).InfoS("Resource alignment at container scope guaranteed", "pod", klog.KObj(pod))
			metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Inc()
		}
	}
	return admission.GetPodAdmitResult(nil)
}

func getFarMemHint(farMemMgrHints map[string][]TopologyHint) TopologyHint {
	if len(farMemMgrHints) != 1 {
		panic(fmt.Sprintf("far memory manager returned hints for resource types other than %s", string(v1.ResourceMemory)))
	}

	farMemHints, ok := farMemMgrHints[string(v1.ResourceMemory)]

	if !ok || len(farMemHints) == 0 {
		panic(fmt.Sprintf("far memory manager returned no hint for resource type %s", string(v1.ResourceMemory)))
	}

	if len(farMemHints) > 1 {
		panic(fmt.Sprintf("far memory manager returned more than one hint for resource type %s", string(v1.ResourceMemory)))
	}

	return farMemHints[0]
}

func (s *containerScope) accumulateProvidersHints(pod *v1.Pod, container *v1.Container) []map[string][]TopologyHint {
	var providersHints []map[string][]TopologyHint

	for _, provider := range s.hintProviders {
		// Get the TopologyHints for a Container from a provider.
		hints := provider.GetTopologyHints(pod, container)
		providersHints = append(providersHints, hints)
		klog.InfoS("TopologyHints", "hints", hints, "pod", klog.KObj(pod), "containerName", container.Name)
	}
	return providersHints
}

func (s *containerScope) calculateAffinity(pod *v1.Pod, container *v1.Container) (TopologyHint, bool) {
	providersHints := s.accumulateProvidersHints(pod, container)
	bestHint, admit := s.policy.Merge(providersHints)
	klog.InfoS("ContainerTopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name)
	return bestHint, admit
}
