package farmemtopologymanager

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/utils/cpuset"

	cadvisor "github.com/google/cadvisor/info/v1"
)

type ActivePodsFunc func() []*v1.Pod

// Allocation describes the Allocation for a container: i.e. from  which NUMA nodes the container's
// CPUs and memory will come from, and how much the container is allowed to use.
type Allocation struct {
	CPUs                cpuset.CPUSet
	PerNUMANodeMemBytes map[int]uint64
}

type reconciledContainer struct {
	podName       string
	containerName string
	containerID   string
}

type runtimeService interface {
	UpdateContainerResources(ctx context.Context, id string, resources *runtimeapi.ContainerResources) error
}

// Constants corresponding to the different ways to distribute CPUs across NUMA nodes.
const (
	Pack = "pack"
	Even = "even"
)

var supportedCPUsDistributionsOverNUMAs map[string]struct{} = map[string]struct{}{
	Pack: {},
	Even: {},
}

// Constants corresponding to the different ways to distribute CPUs across caches.
const (
	LLCShare = "llc-share"
	L1Share  = "l1-share"
	L1Spread = "l1-spread"
)

var supportedCPUsDistributionsOverCaches map[string]struct{} = map[string]struct{}{
	LLCShare: {},
	L1Share:  {},
	L1Spread: {},
}

type resourceRequest struct {
	localMem, farMem           uint64
	minNNUMAs, maxNNUMAs       int
	cpus                       int
	cpusDistributionOverNUMAs  string
	cpusDistributionOverCaches string
}

type Manager struct {
	// Mapping of (PodUID, ContainerName) to ContainerID for Adding/Removing Pods from PodTopologyHints mapping
	podMap containermap.ContainerMap

	// allocs maps podUIDs to container names to the allocation for the container with that name.
	allocs map[string]map[string]Allocation

	topo *topology

	// Set to false at the beginning of each reconciliation period, and to true any time a container
	// with exclusively reserved CPUs is admitted or removed.
	defaultCPUSetChanged bool
	reconcilePeriod      time.Duration
	activePods           ActivePodsFunc
	podStatusProvider    status.PodStatusProvider

	// containerRuntime is the container runtime service interface needed
	// to make UpdateContainerResources() calls against the containers.
	containerRuntime runtimeService

	mutex sync.RWMutex
}

var _ topologymanager.Manager = &Manager{}

// AddHintProvider implements topologymanager.Manager.
func (m *Manager) AddHintProvider(topologymanager.HintProvider) {
}

// GetAffinity implements topologymanager.Manager.
func (m *Manager) GetAffinity(podUID string, containerName string) topologymanager.TopologyHint {
	panic("unimplemented")
}

// GetPolicy implements topologymanager.Manager.
func (m *Manager) GetPolicy() topologymanager.Policy {
	panic("unimplemented")
}

func New(mi *cadvisor.MachineInfo,
	specificSystemReservedCPUs cpuset.CPUSet,
	specificSystemReservedMem []kubeletconfig.MemoryReservation,
	systemReservedQuantities v1.ResourceList,
	reconcilePeriod time.Duration) (*Manager, error) {

	topo := initTopology(mi)

	if err := claimSystemReservedCPUs(topo, specificSystemReservedCPUs, systemReservedQuantities); err != nil {
		return nil, fmt.Errorf("failed to build far memory manager: failed to reserve system CPUs: %v", err)
	}

	if err := claimSystemReservedMem(topo, specificSystemReservedMem, systemReservedQuantities); err != nil {
		return nil, fmt.Errorf("failed to build far memory manager: failed to reserve system memory: %v", err)
	}

	//! matte21 debug
	jsonData, err := json.Marshal(*topo)
	if err != nil {
		panic(err)
	}
	fmt.Println("matte21 dump", string(jsonData))
	//! matte21 debug

	return &Manager{
		podMap:          containermap.NewContainerMap(),
		allocs:          make(map[string]map[string]Allocation),
		topo:            topo,
		reconcilePeriod: reconcilePeriod,
	}, nil
}

func (m *Manager) Start(activePods ActivePodsFunc,
	podStatusProvider status.PodStatusProvider,
	containerRuntime runtimeService) {

	m.activePods = activePods
	m.podStatusProvider = podStatusProvider
	m.containerRuntime = containerRuntime
	go wait.Until(func() { m.reconcileDefaultCPUSet() }, m.reconcilePeriod, wait.NeverStop)
}

func (m *Manager) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	t0 := time.Now()
	defer func() {
		klog.InfoS("admit time", "time", time.Since(t0))
	}()

	p := attrs.Pod
	if v1qos.GetPodQOS(p) != v1.PodQOSGuaranteed {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	// TODO: if admission fails, remove all containers (for now, with a single container we don't care).

	// TODO: think about more fine-grained locking.
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, c := range p.Spec.Containers {
		req, err := m.parseContainerReqs(p, &c)
		if err != nil {
			return lifecycle.PodAdmitResult{Message: err.Error(), Reason: "InvalidResourceRequests"}
		}

		minNumNNUMAs := m.minNumNNUMAsGivenFreeResources(req.cpus, req.localMem)
		if minNumNNUMAs > req.maxNNUMAs {
			return lifecycle.PodAdmitResult{Admit: false, Reason: "NeedMoreNUMANodesThanMaxRequested"}
		}

		if minNumNNUMAs > req.minNNUMAs {
			req.minNNUMAs = minNumNNUMAs
		}

		// For now, we do a first fit.
		// TODO: do something more effective than first fit.
		var nNUMAsCombo []int
		var zNUMAsCombo []int

		// Remove any reference to topology.
		allNNUMAs := make([]int, len(m.topo.NNUMAsSortedByNeighborFarMemory))
		copy(allNNUMAs, m.topo.NNUMAsSortedByNeighborFarMemory)
		slices.Sort(allNNUMAs)

		for i := req.minNNUMAs; i <= req.maxNNUMAs; i++ {
			iterateCombinations(allNNUMAs, i, func(nNUMAsGrp []int) LoopControl {
				// if !m.groupIsConnected(nNUMAsGrp) {
				// 	klog.InfoS("discarding nNUMAs group", "group", nNUMAsGrp, "reason", "disconnected")
				// 	return Continue
				// }

				freeCPUs := 0
				freeMemBytes := uint64(0)
				for _, nID := range nNUMAsGrp {
					freeCPUs += m.topo.NNUMANodes[nID].FreeCPUs.Size()
					freeMemBytes += m.topo.NNUMANodes[nID].FreeBytes
				}

				if freeCPUs < req.cpus {
					klog.InfoS("discarding nNUMAs group: not enough free CPUs",
						"group", nNUMAsGrp,
						"num missing CPUs", req.cpus-freeCPUs)
					return Continue
				}

				if freeMemBytes < req.localMem {
					klog.InfoS("discarding nNUMAs group: not enough free memory",
						"group", nNUMAsGrp,
						"missing free bytes", req.localMem-freeMemBytes)
					return Continue
				}

				if m.candidateBetterThanCurrent(nNUMAsGrp, nNUMAsCombo, true) {
					nNUMAsCombo = nNUMAsGrp
				}
				return Continue

				// if req.farMem == 0 {
				// 	if m.candidateBetterThanCurrent(nNUMAsGrp, nNUMAsCombo, false) {
				// 		nNUMAsCombo = nNUMAsGrp
				// 	}
				// 	return Continue
				// }

				// We need a list of zNUMAs, but we intermediately store them in a map to avoid
				// duplicates.
				zNUMAsSet := make(map[int]struct{})
				zNUMAs := make([]int, 0, 1)
				for _, nID := range nNUMAsGrp {
					if m.topo.NNUMANodes[nID].FreeCPUs.Size() > 0 {
						for zN := range m.topo.NNUMANodes[nID].NeighborZNUMAs {
							if _, alreadySeen := zNUMAsSet[zN]; !alreadySeen {
								zNUMAsSet[zN] = struct{}{}
								zNUMAs = append(zNUMAs, zN)
							}
						}
					}
				}
				// TODO: sort in a better way.
				slices.Sort(zNUMAs)

				for j := 1; j <= len(zNUMAs); j++ {
					// We still do a first fit, and we should do better.
					iterateCombinations(zNUMAs, j, func(zNUMAsGrp []int) LoopControl {
						freeFarMemBytes := uint64(0)
						for _, znID := range zNUMAsGrp {
							freeFarMemBytes += m.topo.ZNUMANodes[znID].FreeBytes
						}
						if freeFarMemBytes >= req.farMem {
							if m.candidateBetterThanCurrent(nNUMAsGrp, nNUMAsCombo, true) {
								nNUMAsCombo = nNUMAsGrp
								zNUMAsCombo = zNUMAsGrp
							}
							// TODO: continue instead. But in our testbeds it's not needed.
							return Break
						}
						klog.InfoS("discarding zNUMAs group", "zNUMAs", zNUMAsGrp, "nNUMAs", nNUMAsGrp)
						return Continue
					})
				}

				return Continue
			})

			if len(nNUMAsCombo) > 0 {
				break
			}
		}

		if len(nNUMAsCombo) == 0 {
			return lifecycle.PodAdmitResult{
				Admit:   false,
				Reason:  "NoNUMAComboFound",
				Message: "No NUMA Combo found",
			}
		}

		// Now, get the zNUMAs.
		// Remove any reference to topology.
		allZNUMAs := make([]int, 0, len(m.topo.ZNUMANodes))
		for zN := range m.topo.ZNUMANodes {
			allNNUMAs = append(allNNUMAs, zN)
		}
		slices.Sort(allZNUMAs)

		for j := 1; j <= len(allZNUMAs); j++ {
			// We still do a first fit, and we should do better.
			iterateCombinations(allZNUMAs, j, func(zNUMAsGrp []int) LoopControl {
				freeFarMemBytes := uint64(0)
				for _, znID := range zNUMAsGrp {
					freeFarMemBytes += m.topo.ZNUMANodes[znID].FreeBytes
				}
				if freeFarMemBytes >= req.farMem {
					if m.candidateBetterThanCurrentZNUMAs(zNUMAsGrp, zNUMAsCombo) {
						nNUMAsCombo = zNUMAsGrp
					}
					// TODO: continue instead. But in our testbeds it's not needed.
					return Break
				}
				klog.InfoS("discarding zNUMAs group", "zNUMAs", zNUMAsGrp, "nNUMAs", zNUMAsCombo)
				return Continue
			})
		}

		if len(zNUMAsCombo) == 0 {
			return lifecycle.PodAdmitResult{
				Admit:   false,
				Reason:  "NoZNUMAComboFound",
				Message: "No zNUMA Combo found",
			}
		}

		klog.InfoS("can admit container", "pod", klog.KObj(p), "container", c.Name,
			"nNUMAs", nNUMAsCombo, "zNUMAs", zNUMAsCombo)

		// Now, do the allocation.
		alloc := Allocation{
			PerNUMANodeMemBytes: make(map[int]uint64, len(zNUMAsCombo)+len(nNUMAsCombo)),
		}

		// Allocate CPUs from the selected nNUMAs combo.
		allocatedCPUs, cpuGivers := m.allocateCPUs(nNUMAsCombo, req.cpus, req.cpusDistributionOverCaches, req.farMem > 0)
		alloc.CPUs = allocatedCPUs

		// Allocate local memory.
		localMemReq := req.localMem
		for nID := range cpuGivers {
			n := m.topo.NNUMANodes[nID]
			if n.FreeBytes == 0 {
				continue
			}
			bytesOnThisNode := uint64(float64(localMemReq) * float64(cpuGivers[nID]) / float64(req.cpus))
			if n.FreeBytes >= bytesOnThisNode {
				req.localMem -= bytesOnThisNode
				alloc.PerNUMANodeMemBytes[nID] += bytesOnThisNode
				n.FreeBytes -= bytesOnThisNode
			} else {
				req.localMem -= n.FreeBytes
				alloc.PerNUMANodeMemBytes[nID] += n.FreeBytes
				n.FreeBytes = 0
			}
		}
		if req.localMem > 0 {
			for nID := range cpuGivers {
				n := m.topo.NNUMANodes[nID]
				if n.FreeBytes >= req.localMem {
					n.FreeBytes -= req.localMem
					alloc.PerNUMANodeMemBytes[nID] += req.localMem
					req.localMem = 0
					break
				}
				req.localMem -= n.FreeBytes
				alloc.PerNUMANodeMemBytes[nID] += n.FreeBytes
				n.FreeBytes = 0
			}
		}
		if req.localMem > 0 {
			for _, nID := range nNUMAsCombo {
				n := m.topo.NNUMANodes[nID]
				if n.FreeBytes >= req.localMem {
					n.FreeBytes -= req.localMem
					alloc.PerNUMANodeMemBytes[nID] += req.localMem
					req.localMem = 0
					break
				}
				req.localMem -= n.FreeBytes
				alloc.PerNUMANodeMemBytes[nID] += n.FreeBytes
				n.FreeBytes = 0
			}
		}
		heap.Init(m.topo.nNUMAsByFreeMem)

		// Allocate far memory.
		for _, nID := range zNUMAsCombo {
			n := m.topo.ZNUMANodes[nID]
			if n.FreeBytes >= req.farMem {
				n.FreeBytes -= req.farMem
				alloc.PerNUMANodeMemBytes[nID] += req.farMem
				req.farMem = 0
				break
			}
			req.farMem -= n.FreeBytes
			alloc.PerNUMANodeMemBytes[nID] += n.FreeBytes
			n.FreeBytes = 0
		}

		if _, ok := m.allocs[string(p.UID)]; !ok {
			m.allocs[string(p.UID)] = make(map[string]Allocation, len(p.Spec.Containers))
		}
		m.allocs[string(p.UID)][c.Name] = alloc
	}

	return lifecycle.PodAdmitResult{
		Admit: true,
	}
}

// nNUMAs is the set of nNUMA nodes to allocate `numToAlloc` CPUs from.
// TODO: implement dist CPU selection strategy.
func (m *Manager) allocateCPUs(nNUMAs []int, numToAlloc int, cacheDist string, wantFarMem bool) (cpuset.CPUSet, map[int]int) {
	allocatedCPUs := cpuset.New()

	slices.SortFunc(nNUMAs, func(n1ID, n2ID int) int {
		farMemDiff := m.topo.farMemBytesInNeighbors(n1ID) - m.topo.farMemBytesInNeighbors(n2ID)

		if farMemDiff == 0 {
			return m.topo.NNUMANodes[n1ID].FreeCPUs.Size() - m.topo.NNUMANodes[n2ID].FreeCPUs.Size()
		}

		if wantFarMem {
			return -farMemDiff
		}
		return farMemDiff
	})

	// Set of nNUMAs that actually contribute CPUs to the allocation.
	cpuGivers := make(map[int]int, len(nNUMAs))

	for _, nID := range nNUMAs {
		if allocatedCPUs.Size() == numToAlloc {
			break
		}

		n := m.topo.NNUMANodes[nID]

		if n.FreeCPUs.IsEmpty() {
			continue
		}

		var cs cpuset.CPUSet
		if allocatedCPUs.Size()+n.FreeCPUs.Size() > numToAlloc {
			// If we're here, n has more free CPUs than those needed to completely satisfy the
			// allocation.
			cs = cpuset.New(m.getCPUs(n, numToAlloc-allocatedCPUs.Size(), cacheDist)...)
		} else {
			// If we're here, n has less or just enough free CPUs to completely satisfy the
			// allocation.
			cs = n.FreeCPUs
			updateCoresAndLLCsMaps(n, cs.List())
		}

		cpuGivers[nID] = cs.Size()

		allocatedCPUs = allocatedCPUs.Union(cs)
		n.ReservedCPUs = n.ReservedCPUs.Union(cs)
		n.FreeCPUs = n.FreeCPUs.Difference(cs)
		m.defaultCPUSetChanged = true
	}

	heap.Init(m.topo.nNUMAsByFreeCPUs)

	return allocatedCPUs, cpuGivers
}

func (m *Manager) getCPUs(n *nNUMANode, numToAlloc int, cacheDist string) []int {
	switch cacheDist {
	case LLCShare:
		return m.getCPUsLLCShare(n, numToAlloc)
	case L1Share:
		return m.getCPUsL1Share(n, numToAlloc)
	case L1Spread:
		return m.getCPUsL1Spread(n, numToAlloc)
	}
	return nil
}

func (m *Manager) getCPUsLLCShare(n *nNUMANode, remainingToAlloc int) []int {
	allocated := make([]int, 0, remainingToAlloc)
	cpusPerLLC := int(m.topo.CPUsPerLLC)

	defer func() {
		updateCoresAndLLCsMaps(n, allocated)
	}()

	for _, cpus := range n.IdleLLCsToCPUs {
		if remainingToAlloc == 0 {
			return allocated
		}

		if remainingToAlloc >= cpusPerLLC {
			allocated = append(allocated, cpus.List()...)
			remainingToAlloc -= cpusPerLLC
			continue
		}

		for _, cpu := range cpus.List() {
			allocated = append(allocated, cpu)
			remainingToAlloc--
			if remainingToAlloc == 0 {
				return allocated
			}
		}
	}

	if remainingToAlloc == 0 {
		return allocated
	}

	busyLLCs := make([]int, 0, len(n.BusyLLCsToFreeCPUs))
	for llc := range n.BusyLLCsToFreeCPUs {
		busyLLCs = append(busyLLCs, llc)
	}
	slices.SortFunc(busyLLCs, func(x, y int) int {
		return n.BusyLLCsToFreeCPUs[y].Size() - n.BusyLLCsToFreeCPUs[x].Size()
	})

	for _, llc := range busyLLCs {
		if remainingToAlloc == 0 {
			return allocated
		}

		cpus := n.BusyLLCsToFreeCPUs[llc]
		if remainingToAlloc >= cpus.Size() {
			allocated = append(allocated, cpus.List()...)
			remainingToAlloc -= cpus.Size()
			continue
		}

		for _, cpu := range cpus.List() {
			allocated = append(allocated, cpu)
			remainingToAlloc--
			if remainingToAlloc == 0 {
				return allocated
			}
		}
	}

	return allocated
}

func (m *Manager) getCPUsL1Share(n *nNUMANode, remainingToAlloc int) []int {
	allocated := make([]int, 0, remainingToAlloc)
	cpusPerCore := int(m.topo.CPUsPerCore)

	defer func() {
		updateCoresAndLLCsMaps(n, allocated)
	}()

	for _, cpus := range n.IdleCoresToCPUs {
		if remainingToAlloc == 0 {
			return allocated
		}

		if remainingToAlloc >= cpusPerCore {
			allocated = append(allocated, cpus.List()...)
			remainingToAlloc -= cpusPerCore
			continue
		}

		for _, cpu := range cpus.List() {
			allocated = append(allocated, cpu)
			remainingToAlloc--
			if remainingToAlloc == 0 {
				return allocated
			}
		}
	}

	if remainingToAlloc == 0 {
		return allocated
	}

	busyCores := make([]int, 0, len(n.BusyCoresToFreeCPUs))
	for core := range n.BusyCoresToFreeCPUs {
		busyCores = append(busyCores, core)
	}
	slices.SortFunc(busyCores, func(x, y int) int {
		return n.BusyCoresToFreeCPUs[y].Size() - n.BusyCoresToFreeCPUs[x].Size()
	})

	for _, core := range busyCores {
		if remainingToAlloc == 0 {
			return allocated
		}

		cpus := n.BusyCoresToFreeCPUs[core]
		if remainingToAlloc >= cpus.Size() {
			allocated = append(allocated, cpus.List()...)
			remainingToAlloc -= cpus.Size()
			continue
		}

		for _, cpu := range cpus.List() {
			allocated = append(allocated, cpu)
			remainingToAlloc--
			if remainingToAlloc == 0 {
				return allocated
			}
		}
	}

	return allocated
}

func (m *Manager) getCPUsL1Spread(n *nNUMANode, remainingToAlloc int) []int {
	// TODO: make this function's implementation portable. Currently it relies on the fact that on
	// both of my testbeds cpu i and i+1 do NOT share L1 cache. But that's not true across all
	// platforms.
	allocated := make([]int, 0, remainingToAlloc)

	defer func() {
		updateCoresAndLLCsMaps(n, allocated)
	}()

	for _, cpu := range n.FreeCPUs.List() {
		allocated = append(allocated, cpu)
		remainingToAlloc--
		if remainingToAlloc == 0 {
			break
		}
	}

	return allocated
}

func (m *Manager) minNumNNUMAsGivenFreeResources(reqCPUs int, reqMem uint64) int {
	maxFreeCPUs := m.topo.maxFreeCPUsInSingleNNUMA()
	minCPUWise := (reqCPUs + maxFreeCPUs - 1) / maxFreeCPUs

	maxFreeMem := m.topo.maxFreeMemInSingleNNUMA()
	minMemWise := int((reqMem + maxFreeMem - 1) / maxFreeMem)

	if minMemWise > minCPUWise {
		return minMemWise
	}
	return minCPUWise
}

// LoopControl controls the behavior of the cpu accumulator loop logic
type LoopControl int

// Possible loop control outcomes
const (
	Continue LoopControl = iota
	Break
)

func iterateCombinations(n []int, k int, f func([]int) LoopControl) {
	if k < 1 {
		return
	}

	var helper func(n []int, k int, start int, accum []int, f func([]int) LoopControl) LoopControl
	helper = func(n []int, k int, start int, accum []int, f func([]int) LoopControl) LoopControl {
		if k == 0 {
			return f(accum)
		}
		for i := start; i <= len(n)-k; i++ {
			control := helper(n, k-1, i+1, append(accum, n[i]), f)
			if control == Break {
				return Break
			}
		}
		return Continue
	}

	helper(n, k, 0, []int{}, f)
}

// TODO: collect stats about the group (i.e. how much free mem) as you explore it.
func (m *Manager) groupIsConnected(nNUMAsList []int) bool {
	nNUMAsGrp := make(map[int]struct{}, len(nNUMAsList))
	for _, n := range nNUMAsList {
		nNUMAsGrp[n] = struct{}{}
	}

	// todo: dedup visited and nNUMAsGrp.
	visited := make(map[int]bool)
	queue := []int{nNUMAsList[0]}

	visited[nNUMAsList[0]] = true

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		// TODO: first, consider all nNUMAs in the same socket.
		neighbors := make([]int, 0, len(m.topo.NNUMANodes[node].NeighborNNUMAsBySocket))
		for _, neighborsInOneSocket := range m.topo.NNUMANodes[node].NeighborNNUMAsBySocket {
			for neighbor := range neighborsInOneSocket {
				if _, neighborInGrp := nNUMAsGrp[neighbor]; neighborInGrp {
					neighbors = append(neighbors, neighbor)
				}
			}
		}

		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}

	for n := range nNUMAsGrp {
		if !visited[n] {
			return false
		}
	}

	return true
}

func (m *Manager) parseContainerReqs(p *v1.Pod, c *v1.Container) (resourceRequest, error) {
	var rr resourceRequest
	var err error

	rr.localMem, err = getRequestedLocalMemBytes(c)
	if err != nil {
		return rr, fmt.Errorf("failed to parse local memory: %v", err)
	}

	rr.farMem, err = getRequestedFarMemBytes(p, c)
	if err != nil {
		return rr, fmt.Errorf("failed to parse far memory: %v", err)
	}

	// TODO: handle the case where the CPU request is 0 (is it even possible for Guaranteed pods?)
	rr.cpus = getRequestedCPUs(p, c)

	rr.cpusDistributionOverNUMAs, rr.cpusDistributionOverCaches, err = getRequestCPUsDistPolicy(p, c)
	if err != nil {
		return rr, fmt.Errorf("failed to parse CPUs distribution policy: %v", err)
	}

	rr.minNNUMAs, rr.maxNNUMAs, err = m.getRequestedNumNNUMAs(p, c)
	if err != nil {
		return rr, fmt.Errorf("failed to parse number of nNUMA nodes: %v", err)
	}

	return rr, nil
}

func getRequestCPUsDistPolicy(p *v1.Pod, c *v1.Container) (distOverNUMAs, distOverCaches string, err error) {
	distOverNUMAsKey, distOverCachesKey := cpusDistPolicyAnnotationKeys(c.Name)

	distOverNUMAs, ok := p.Annotations[distOverNUMAsKey]
	if !ok {
		distOverNUMAs = Pack
	}
	if _, isSupported := supportedCPUsDistributionsOverNUMAs[distOverNUMAs]; !isSupported {
		err = fmt.Errorf("requested CPUs distribution policy over NUMAs %s is not supported. "+
			"Supported ones are %v", distOverNUMAs, supportedCPUsDistributionsOverNUMAs)
		return
	}

	distOverCaches, ok = p.Annotations[distOverCachesKey]
	if !ok {
		distOverCaches = L1Share
	}
	if _, isSupported := supportedCPUsDistributionsOverCaches[distOverCaches]; !isSupported {
		err = fmt.Errorf("requested CPUs distribution policy over Caches %s is not supported. "+
			"Supported ones are %v", distOverCaches, supportedCPUsDistributionsOverCaches)
	}

	return
}

func cpusDistPolicyAnnotationKeys(containerName string) (distOverNUMAsKey, distOverCachesKey string) {
	distOverNUMAsKey = containerName + "/cpus-distribution-over-numas"
	distOverCachesKey = containerName + "/cpus-distribution-over-caches"
	return
}

// If getRequestedFarMemBytes returns an error, it also returns a 0 quantity, because the caller is a
// function that can't return errors. So the caller logs the error, but then continues execution
// normally => having a 0 quantity makes its code simpler.
func getRequestedFarMemBytes(pod *v1.Pod, container *v1.Container) (uint64, error) {
	farMemAnnotationKey := farMemAnnotationKey(container.Name)

	farMemAnnotationVal, ok := pod.Annotations[farMemAnnotationKey]
	if !ok {
		return 0, nil
	}

	farMemQty, err := resource.ParseQuantity(farMemAnnotationVal)
	if err != nil {
		return 0, fmt.Errorf("failed to parse far memory annotation %s: %s", farMemAnnotationVal, err)
	}

	requestedSize, succeed := farMemQty.AsInt64()
	if !succeed {
		return 0, fmt.Errorf("[farmemorymanager] failed to represent quantity as int64")
	}

	return uint64(requestedSize), nil
}

func farMemAnnotationKey(containerName string) string {
	return containerName + "/far-mem"
}

func (m *Manager) getRequestedNumNNUMAs(pod *v1.Pod, container *v1.Container) (min, max int, err error) {
	// Get the user requested minimum, if specified.
	min = 1
	minKey := minNNUMAsAnnotationKey(container.Name)
	minStr, ok := pod.Annotations[minKey]
	if ok {
		min, err = strconv.Atoi(minStr)
		if err != nil {
			err = fmt.Errorf("failed to parse annotation \"%s: %s\" with minimum number of nNUMA "+
				"nodes: %v", minKey, minStr, err)
			return
		}
		if min <= 0 {
			err = fmt.Errorf("illegal value for min number of nNNUMA nodes in annotation %s: "+
				"found value %d; must be > 0", minKey, min)
			return
		}
	}

	// Get the user requested maximum, if specified.
	max = len(m.topo.NNUMANodes)
	maxKey := maxNNUMAsAnnotationKey(container.Name)
	maxStr, ok := pod.Annotations[maxKey]
	if ok {
		max, err = strconv.Atoi(maxStr)
		if err != nil {
			err = fmt.Errorf("failed to parse annotation \"%s: %s\" with max number of nNUMA "+
				"nodes: %v", maxKey, maxStr, err)
			return
		}
		// No need to check max <= 0, the check on max >= min will catch that.
	}

	if max < min {
		err = fmt.Errorf("illegal values of min and max number of NUMA nodes: max value %d is "+
			"less than min value %d; max must be >= min", max, min)
	}

	return
}

func minNNUMAsAnnotationKey(containerName string) string {
	return containerName + "/min-nnumas"
}

func maxNNUMAsAnnotationKey(containerName string) string {
	return containerName + "/max-nnumas"
}

func getRequestedLocalMemBytes(container *v1.Container) (uint64, error) {
	for resourceName, quantity := range container.Resources.Requests {
		if resourceName != v1.ResourceMemory {
			continue
		}
		requestedSize, succeed := quantity.AsInt64()
		if !succeed {
			return 0, fmt.Errorf("[farmemorymanager] failed to represent quantity as int64")
		}
		return uint64(requestedSize), nil
	}
	return 0, fmt.Errorf("[farmemorymanager] failed to get local mem")
}

func getRequestedCPUs(pod *v1.Pod, container *v1.Container) int {
	cpuQuantity := container.Resources.Requests[v1.ResourceCPU]

	cpuValue := cpuQuantity.Value()
	if cpuValue*1000 != cpuQuantity.MilliValue() {
		klog.V(5).InfoS("Exclusive CPU allocation skipped, pod requested non-integral CPUs", "pod", klog.KObj(pod), "containerName", container.Name, "cpu", cpuValue)
		return 0
	}
	// Safe downcast to do for all systems with < 2.1 billion CPUs.
	// Per the language spec, `int` is guaranteed to be at least 32 bits wide.
	// https://golang.org/ref/spec#Numeric_types
	return int(cpuQuantity.Value())
}

func (m *Manager) AddContainer(p *v1.Pod, c *v1.Container, containerID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.podMap.Add(string(p.UID), c.Name, containerID)
}

func (m *Manager) RemoveContainer(containerID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Get the podUID and containerName associated with the containerID to be removed and remove it
	podUIDString, containerName, err := m.podMap.GetContainerRef(containerID)
	if err != nil {
		return nil
	}
	m.podMap.RemoveByContainerID(containerID)

	klog.InfoS("RemoveContainer", "containerID", containerID, "podUID", podUIDString, "containerName", containerName)

	// Now, remove the allocation.

	// In cases where a container has been restarted, it's possible that the same podUID and
	// containerName are already associated with a *different* containerID now. Only remove
	// the allocations associated with that podUID and containerName if this is not true
	if _, err := m.podMap.GetContainerID(podUIDString, containerName); err != nil {
		cntAlloc, ok := m.allocs[podUIDString][containerName]

		delete(m.allocs[podUIDString], containerName)
		if len(m.allocs[podUIDString]) == 0 {
			delete(m.allocs, podUIDString)
		}

		if !ok {
			return nil
		}

		// Free memory.
		for numaNodeID, usedMemBytesOnNode := range cntAlloc.PerNUMANodeMemBytes {
			numaNode, ok := m.topo.numaNodeMem(numaNodeID)
			if !ok {
				klog.InfoS("deleting container with allocation on non-existing NUMA node",
					"container_name", containerName,
					"pod_uid", podUIDString,
					"numa_node", numaNodeID)
				continue
			}
			numaNode.FreeBytes += usedMemBytesOnNode
			// We do this truncation in case the NUMA node has been resized before we could process
			// the deletion (could happen because of CXL hot-UNplugging on zNUMAs for example).
			if numaNode.FreeBytes > numaNode.AllocatableBytes {
				numaNode.FreeBytes = numaNode.AllocatableBytes
			}
		}

		// Free CPUs. This code is perversely inefficient, but I don't have time to fix it now.
		m.defaultCPUSetChanged = true
		for _, n := range m.topo.NNUMANodes {
			cpusToFree := n.ReservedCPUs.Intersection(cntAlloc.CPUs)
			n.ReservedCPUs = n.ReservedCPUs.Difference(cpusToFree)
			n.FreeCPUs = n.FreeCPUs.Union(cpusToFree)

			for _, cpu := range cpusToFree.List() {
				cset := cpuset.New(cpu)

				core := n.cpuToCoreAndLLC[cpu].coreID
				n.BusyCoresToFreeCPUs[core] = n.BusyCoresToFreeCPUs[core].Union(cset)
				if len(n.BusyCoresToFreeCPUs[core].List()) == int(m.topo.CPUsPerCore) {
					n.IdleCoresToCPUs[core] = n.BusyCoresToFreeCPUs[core]
					delete(n.BusyCoresToFreeCPUs, core)
				}

				llc := n.cpuToCoreAndLLC[cpu].llcID
				n.BusyLLCsToFreeCPUs[llc] = n.BusyLLCsToFreeCPUs[llc].Union(cset)
				if len(n.BusyLLCsToFreeCPUs[llc].List()) == int(m.topo.CPUsPerLLC) {
					n.IdleLLCsToCPUs[llc] = n.BusyLLCsToFreeCPUs[llc]
					delete(n.BusyLLCsToFreeCPUs, llc)
				}
			}
		}

		heap.Init(m.topo.nNUMAsByFreeMem)
		heap.Init(m.topo.nNUMAsByFreeCPUs)
	}

	return nil
}

func (m *Manager) GetAllCPUs() cpuset.CPUSet {
	return m.topo.AllCPUs.Clone()
}

func (m *Manager) GetAllocatableCPUs() cpuset.CPUSet {
	return m.topo.AllCPUs.Difference(m.topo.SystemReservedCPUs)
}

func (m *Manager) GetCPUAffinity(podUID string, containerName string) cpuset.CPUSet {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if podAllocs, ok := m.allocs[podUID]; ok {
		if cntAllocs, ok := podAllocs[containerName]; ok {
			return cntAllocs.CPUs.Clone()
		}
	}

	defaultAffinity := m.topo.SystemReservedCPUs.Clone()
	for _, n := range m.topo.NNUMANodes {
		defaultAffinity = defaultAffinity.Union(n.FreeCPUs)
	}
	return defaultAffinity
}

func (m *Manager) GetExclusiveCPUs(podUID string, containerName string) cpuset.CPUSet {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if podAllocs, ok := m.allocs[podUID]; ok {
		if containerAllocs, ok := podAllocs[containerName]; ok {
			return containerAllocs.CPUs
		}
	}
	return cpuset.CPUSet{}
}

// GetAllocatableMemory implements memorymanager.Manager.
func (m *Manager) GetAllocatableMemory() []state.Block {
	allocMem := make([]state.Block, 0, len(m.topo.NNUMANodes)+len(m.topo.ZNUMANodes))
	for _, n := range m.topo.NNUMANodes {
		if n.AllocatableBytes > 0 {
			allocMem = append(allocMem, state.Block{
				Type:         v1.ResourceMemory,
				Size:         n.AllocatableBytes,
				NUMAAffinity: []int{n.ID},
			})
		}
	}
	for _, n := range m.topo.ZNUMANodes {
		if n.AllocatableBytes > 0 {
			allocMem = append(allocMem, state.Block{
				Type:         v1.ResourceMemory,
				Size:         n.AllocatableBytes,
				NUMAAffinity: []int{n.ID},
			})
		}
	}
	return allocMem
}

// GetMemory implements memorymanager.Manager.
func (m *Manager) GetMemory(podUID string, containerName string) []state.Block {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if podAllocs, ok := m.allocs[podUID]; ok {
		if cntAlloc, ok := podAllocs[containerName]; ok {
			var totMemBytes uint64
			numaNodes := make([]int, 0, len(cntAlloc.PerNUMANodeMemBytes))
			for n, memBytes := range cntAlloc.PerNUMANodeMemBytes {
				totMemBytes += memBytes
				numaNodes = append(numaNodes, n)
			}
			return []state.Block{{
				NUMAAffinity: numaNodes,
				Type:         v1.ResourceMemory,
				Size:         totMemBytes,
			}}
		}
	}

	return nil
}

// GetMemoryNUMANodes implements memorymanager.Manager.
func (m *Manager) GetMemoryNUMANodes(pod *v1.Pod, container *v1.Container) sets.Set[int] {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	nodes := sets.New[int]()
	if podAllocs, ok := m.allocs[string(pod.UID)]; ok {
		if cntAlloc, ok := podAllocs[container.Name]; ok {
			for numaNode := range cntAlloc.PerNUMANodeMemBytes {
				nodes.Insert(numaNode)
			}
		}
	}
	if nodes.Len() == 0 {
		klog.V(5).InfoS("NUMA nodes not available for allocation",
			"pod", klog.KObj(pod),
			"containerName", container.Name)
		return nil
	}
	klog.InfoS("Memory affinity", "pod", klog.KObj(pod), "containerName", container.Name, "numaNodes", nodes)
	return nodes
}

func (m *Manager) GetAlloc(pod *v1.Pod, container *v1.Container) *Allocation {
	if podAllocs, ok := m.allocs[string(pod.UID)]; ok {
		if alloc, ok := podAllocs[container.Name]; ok {
			return &alloc
		}
	}
	return nil
}

func (m *Manager) reconcileDefaultCPUSet() (success []reconciledContainer, failure []reconciledContainer) {
	m.mutex.Lock()
	if !m.defaultCPUSetChanged {
		m.mutex.Unlock()
		return
	}
	// TODO: fix the bug that occurs if we fail to update the default CPUSet for a container:
	// the fact that we set this to false here means that in such a scenario there's no guarantee
	// that there will be a retry: the container will be stuck with the old default CPUSet (which
	// might hurt experiments performance).
	m.defaultCPUSetChanged = false
	m.mutex.Unlock()

	success = []reconciledContainer{}
	failure = []reconciledContainer{}

	ctx := context.Background()
	for _, pod := range m.activePods() {
		pstatus, ok := m.podStatusProvider.GetPodStatus(pod.UID)
		if !ok {
			klog.V(5).InfoS("reconcileDefaultCPUSet: skipping pod; status not found", "pod", klog.KObj(pod))
			failure = append(failure, reconciledContainer{pod.Name, "", ""})
			continue
		}

		// Currently we don't support init containers, so we don't iterate over them.
		for _, container := range pod.Spec.Containers {
			containerID, err := findContainerIDByName(&pstatus, container.Name)
			if err != nil {
				klog.V(5).InfoS("reconcileDefaultCPUSet: skipping container; ID not found in pod status", "pod", klog.KObj(pod), "containerName", container.Name, "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			cstatus, err := findContainerStatusByName(&pstatus, container.Name)
			if err != nil {
				klog.V(5).InfoS("reconcileDefaultCPUSet: skipping container; container status not found in pod status", "pod", klog.KObj(pod), "containerName", container.Name, "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			if cstatus.State.Waiting != nil ||
				(cstatus.State.Waiting == nil && cstatus.State.Running == nil && cstatus.State.Terminated == nil) {
				klog.V(4).InfoS("reconcileDefaultCPUSet: skipping container; container still in the waiting state", "pod", klog.KObj(pod), "containerName", container.Name, "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			m.mutex.Lock()
			if cstatus.State.Terminated != nil {
				// The container is terminated but we can't call m.RemoveContainer()
				// here because it could remove the allocated cpuset for the container
				// which may be in the process of being restarted.  That would result
				// in the container losing any exclusively-allocated CPUs that it
				// was allocated.
				_, _, err := m.podMap.GetContainerRef(containerID)
				if err == nil {
					klog.V(4).InfoS("ReconcileState: ignoring terminated container", "pod", klog.KObj(pod), "containerID", containerID)
				}
				m.mutex.Unlock()
				continue
			}

			// Once we make it here we know we have a running container.
			// Idempotently add it to the containerMap incase it is missing.
			// This can happen after a kubelet restart, for example.
			m.podMap.Add(string(pod.UID), container.Name, containerID)

			// Compute default CPUSet.
			defaultCPUset := m.topo.SystemReservedCPUs.Clone()
			for _, n := range m.topo.NNUMANodes {
				defaultCPUset = defaultCPUset.Union(n.FreeCPUs)
			}
			m.mutex.Unlock()

			if m.GetExclusiveCPUs(string(pod.UID), container.Name).IsEmpty() {
				klog.V(5).InfoS("ReconcileState: updating container", "pod", klog.KObj(pod), "containerName", container.Name, "containerID", containerID, "cpuSet", defaultCPUset)
				err = m.updateContainerCPUSet(ctx, containerID, defaultCPUset)
				if err != nil {
					klog.ErrorS(err, "ReconcileState: failed to update container", "pod", klog.KObj(pod), "containerName", container.Name, "containerID", containerID, "cpuSet", defaultCPUset)
					failure = append(failure, reconciledContainer{pod.Name, container.Name, containerID})
					continue
				}
			}
			success = append(success, reconciledContainer{pod.Name, container.Name, containerID})
		}
	}

	return success, failure
}

func findContainerIDByName(status *v1.PodStatus, name string) (string, error) {
	for _, container := range status.ContainerStatuses {
		if container.Name == name && container.ContainerID != "" {
			cid := &kubecontainer.ContainerID{}
			err := cid.ParseString(container.ContainerID)
			if err != nil {
				return "", err
			}
			return cid.ID, nil
		}
	}
	return "", fmt.Errorf("unable to find ID for container with name %v in pod status (it may not be running)", name)
}

func findContainerStatusByName(status *v1.PodStatus, name string) (*v1.ContainerStatus, error) {
	for _, containerStatus := range status.ContainerStatuses {
		if containerStatus.Name == name {
			return &containerStatus, nil
		}
	}
	return nil, fmt.Errorf("unable to find status for container with name %v in pod status (it may not be running)", name)
}

// claimSystemReservedCPUs mutates `t` by recording which CPUs are "system reserved", i.e.
// non-exclusively allocatable to Guaranteed Pods so that the OS and the kubelet always have
// enough CPUs to run on.
func claimSystemReservedCPUs(t *topology, specificSysReservedCPUs cpuset.CPUSet, sysReservedQuantities v1.ResourceList) error {
	numSysReservedCPUs, err := getNumSysReservedCPUs(sysReservedQuantities)
	if err != nil {
		return err
	}

	if specificSysReservedCPUs.Size() <= 0 {
		// Find a set of CPUs to reserve exclusively.
		specificSysReservedCPUs = getSysReservedCPUs(t, numSysReservedCPUs)
	}

	if specificSysReservedCPUs.Size() != numSysReservedCPUs {
		return fmt.Errorf("[farmemmanager] unable to reserve the required amount of CPUs (size of %d did not equal %d)",
			specificSysReservedCPUs.Size(), numSysReservedCPUs)
	}

	if !specificSysReservedCPUs.IsSubsetOf(t.AllCPUs) {
		return fmt.Errorf("[farmemmanager] the following reserved CPUs do not exist: %v",
			specificSysReservedCPUs.Difference(t.AllCPUs))
	}

	setSysReservedCPUs(t, specificSysReservedCPUs)

	return nil
}

// claimSystemReservedMem mutates `t` by recording how much memory on each NUMA node is "system
// reserved", i.e. non-exclusively allocatable to Guaranteed Pods so that the OS and the kubelet
// always have enough memory to run on.
func claimSystemReservedMem(t *topology, specificSysReservedMem []kubeletconfig.MemoryReservation, sysReservedQuantities v1.ResourceList) error {
	sysResMemBytes := uint64(0)
	for resName, qty := range sysReservedQuantities {
		if resName == v1.ResourceMemory {
			qtyAsInt64, success := qty.AsInt64()
			if !success {
				return fmt.Errorf("could not covert host-wide system %s reservation of type Quantity to int64", v1.ResourceMemory)
			}
			sysResMemBytes = uint64(qtyAsInt64)
		}
	}
	if sysResMemBytes == 0 {
		return fmt.Errorf("far mem manager: kubelet config must specify non-zero system reserved memory and it doesn't")
	}

	totSpecificSysResMemBytes := uint64(0)
	for _, res := range specificSysReservedMem {
		numaNode, ok := t.numaNodeMem(int(res.NumaNode))
		if !ok {
			return fmt.Errorf("the reserved memory configuration references a NUMA node %d that does not exist on this machine", res.NumaNode)
		}

		for resName, qty := range res.Limits {
			if resName != v1.ResourceMemory {
				return fmt.Errorf("far mem manager supports memory reservation only for %s, but the current kubelet config has reservation for other types as well: %s",
					v1.ResourceMemory, resName)
			}
			resValBytesInt, success := qty.AsInt64()
			if !success {
				return fmt.Errorf("could not covert system memory reservation for NUMA node %d of type Quantity to int64", res.NumaNode)
			}
			resValBytes := uint64(resValBytesInt)
			if numaNode.TotBytes < resValBytes {
				return fmt.Errorf("kubelet config reserves %d bytes of memory on NUMA node %d, which only has %d",
					resValBytes, res.NumaNode, numaNode.TotBytes)
			}
			totSpecificSysResMemBytes += resValBytes
			numaNode.SystemReservedBytes = resValBytes
			numaNode.AllocatableBytes = numaNode.TotBytes - numaNode.SystemReservedBytes
			numaNode.FreeBytes = numaNode.AllocatableBytes
		}
	}

	if totSpecificSysResMemBytes != sysResMemBytes {
		return fmt.Errorf("the sum of the per-NUMA node system reserved %s (%d bytes) is not equal to the value %d bytes determined by Node Allocatable feature",
			v1.ResourceMemory, totSpecificSysResMemBytes, sysResMemBytes)
	}

	return nil
}

func getNumSysReservedCPUs(sysReservedQuantities v1.ResourceList) (int, error) {
	numSysReservedCPUsUnrounded, ok := sysReservedQuantities[v1.ResourceCPU]
	if !ok {
		return -1, fmt.Errorf("[farmemmanager] unable to determine reserved CPU resources")
	}
	if numSysReservedCPUsUnrounded.IsZero() {
		// The far mem manager requires this to be nonzero. Zero CPU reservation
		// would allow the shared pool to be completely exhausted. At that point
		// either we would violate our guarantee of exclusivity or need to evict
		// any pod that has at least one container that requires zero CPUs.
		// See the comments in CPU manager's policy_static.go for more details.
		return -1, fmt.Errorf("[farmemmanager] systemreserved.cpu + kubereserved.cpu must be greater than zero")
	}
	// Take the ceiling of the reservation, since fractional CPUs cannot be
	// exclusively allocated.
	reservedCPUsFloat := float64(numSysReservedCPUsUnrounded.MilliValue()) / 1000
	return int(math.Ceil(reservedCPUsFloat)), nil
}

func getSysReservedCPUs(t *topology, numSysReservedCPUs int) cpuset.CPUSet {
	j := 0
	sysReservedCPUs := make([]int, numSysReservedCPUs)
	for _, n := range t.NNUMAsSortedByNeighborFarMemory {
		sortedCPUsInN := t.NNUMANodes[n].FreeCPUs.List()
		for i := 0; i < len(sortedCPUsInN) && j < numSysReservedCPUs; i++ {
			sysReservedCPUs[j] = sortedCPUsInN[i]
			j++
		}
		if j == numSysReservedCPUs {
			break
		}
	}
	return cpuset.New(sysReservedCPUs...)
}

func setSysReservedCPUs(topo *topology, sysResCPUs cpuset.CPUSet) {
	topo.SystemReservedCPUs = sysResCPUs
	for _, n := range topo.NNUMANodes {
		sysResCPUsInN := n.FreeCPUs.Intersection(sysResCPUs)
		n.FreeCPUs = n.FreeCPUs.Difference(sysResCPUsInN)
		updateCoresAndLLCsMaps(n, sysResCPUsInN.List())
	}
}

func (m *Manager) updateContainerCPUSet(ctx context.Context, containerID string, cpus cpuset.CPUSet) error {
	// TODO: Consider adding a `ResourceConfigForContainer` helper in
	// helpers_linux.go similar to what exists for pods.
	// It would be better to pass the full container resources here instead of
	// this patch-like partial resources.

	return m.containerRuntime.UpdateContainerResources(
		ctx,
		containerID,
		&runtimeapi.ContainerResources{
			Linux: &runtimeapi.LinuxContainerResources{
				CpusetCpus: cpus.String(),
			},
		})
}

func updateCoresAndLLCsMaps(n *nNUMANode, cpus []int) {
	for _, cpu := range cpus {
		cset := cpuset.New(cpu)

		core := n.cpuToCoreAndLLC[cpu].coreID
		freeCoreCPUs, ok := n.IdleCoresToCPUs[core]
		if ok {
			delete(n.IdleCoresToCPUs, core)
		} else {
			freeCoreCPUs = n.BusyCoresToFreeCPUs[core]
		}
		n.BusyCoresToFreeCPUs[core] = freeCoreCPUs.Difference(cset)

		llc := n.cpuToCoreAndLLC[cpu].llcID
		freeLLCCPUs, ok := n.IdleLLCsToCPUs[llc]
		if ok {
			delete(n.IdleLLCsToCPUs, llc)
		} else {
			freeLLCCPUs = n.BusyLLCsToFreeCPUs[llc]
		}
		n.BusyLLCsToFreeCPUs[llc] = freeLLCCPUs.Difference(cset)
	}
}

func (m *Manager) candidateBetterThanCurrent(candidate, current []int, farMemRequested bool) bool {
	if len(current) == 0 {
		return true
	}

	if !farMemRequested {
		candidateFarMem := uint64(0)
		zNUMAsSet := make(map[int]struct{})
		for _, nID := range candidate {
			for zN := range m.topo.NNUMANodes[nID].NeighborZNUMAs {
				if _, alreadySeen := zNUMAsSet[zN]; !alreadySeen {
					zNUMAsSet[zN] = struct{}{}
					candidateFarMem += m.topo.ZNUMANodes[zN].AllocatableBytes
				}
			}
		}

		currentFarMem := uint64(0)
		zNUMAsSet = make(map[int]struct{})
		for _, nID := range current {
			for zN := range m.topo.NNUMANodes[nID].NeighborZNUMAs {
				if _, alreadySeen := zNUMAsSet[zN]; !alreadySeen {
					zNUMAsSet[zN] = struct{}{}
					currentFarMem += m.topo.ZNUMANodes[zN].AllocatableBytes
				}
			}
		}

		if candidateFarMem != currentFarMem {
			return candidateFarMem < currentFarMem
		}
	}

	if len(candidate) < len(current) {
		return true
	}

	candidateDistance := uint64(0)
	for _, i := range candidate {
		for _, j := range candidate {
			candidateDistance += m.topo.NUMADistanceMatrix[i][j]
		}
	}
	candidateDistanceFlt := float64(candidateDistance) / float64(len(candidate))

	currentDistance := uint64(0)
	for _, i := range current {
		for _, j := range current {
			currentDistance += m.topo.NUMADistanceMatrix[i][j]
		}
	}
	currentDistanceFlt := float64(currentDistance) / float64(len(current))

	if currentDistanceFlt == candidateDistanceFlt {
		curMask, _ := bitmask.NewBitMask(current...)
		candMask, _ := bitmask.NewBitMask(candidate...)
		return candMask.IsLessThan(curMask)
	}

	return candidateDistanceFlt < currentDistanceFlt
}

func (m *Manager) candidateBetterThanCurrentZNUMAs(candidate, current []int) bool {
	if len(current) == 0 {
		return true
	}

	if len(candidate) < len(current) {
		return true
	}

	if len(candidate) > len(current) {
		return false
	}

	curMask, _ := bitmask.NewBitMask(current...)
	candMask, _ := bitmask.NewBitMask(candidate...)
	return candMask.IsLessThan(curMask)
}
