//go:build linux
// +build linux

/*
Copyright 2021 The Kubernetes Authors.

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

package cm

import (
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/farmemtopologymanager"
)

func (i *internalContainerLifecycleImpl) PreCreateContainer(pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	// if fmm, ok := i.topologyManager.(*farmemtopologymanager.Manager); ok {
	// 	preCreateContainerWUnifiedMgr(fmm, pod, container, containerConfig)
	// } else {
	// 	preCreateContainerWCPUandMemMgrs(i, pod, container, containerConfig)
	// }
	return preCreateContainerWCPUandMemMgrs(i, pod, container, containerConfig)
}

func preCreateContainerWUnifiedMgr(fmm *farmemtopologymanager.Manager, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) {
	a := fmm.GetAlloc(pod, container)
	if a == nil {
		return
	}

	if !a.CPUs.IsEmpty() {
		containerConfig.Linux.Resources.CpusetCpus = a.CPUs.String()
	}

	if len(a.PerNUMANodeMemBytes) == 0 {
		return
	}

	var cpusetMems []string
	containerConfig.Linux.Resources.MaxPerMemInBytes = make(map[uint64]uint64, len(a.PerNUMANodeMemBytes))
	for node, maxMemBytes := range a.PerNUMANodeMemBytes {
		containerConfig.Linux.Resources.MaxPerMemInBytes[uint64(node)] = maxMemBytes
		cpusetMems = append(cpusetMems, strconv.Itoa(node))
	}
	containerConfig.Linux.Resources.CpusetMems = strings.Join(cpusetMems, ",")
}

func preCreateContainerWCPUandMemMgrs(i *internalContainerLifecycleImpl, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	if i.cpuManager != nil {
		allocatedCPUs := i.cpuManager.GetCPUAffinity(string(pod.UID), container.Name)
		if !allocatedCPUs.IsEmpty() {
			containerConfig.Linux.Resources.CpusetCpus = allocatedCPUs.String()
		}
	}

	if i.memoryManager != nil {
		numaNodes := i.memoryManager.GetMemoryNUMANodes(pod, container)
		if numaNodes.Len() > 0 {
			var affinity []string
			for _, numaNode := range sets.List(numaNodes) {
				affinity = append(affinity, strconv.Itoa(numaNode))
			}
			containerConfig.Linux.Resources.CpusetMems = strings.Join(affinity, ",")
		}
	}

	return nil
}
