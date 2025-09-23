package farmemtopologymanager

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"

	_ "k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/utils/cpuset"

	cadvisor "github.com/google/cadvisor/info/v1"
)

// Throughout this file, SNC stands for "sub-NUMA clustering". When we talk about SNC, we also mean
// systems where a single socket holds multiple physical NUMA nodes (do they exist?), besides
// real SNC systems.

// Assumption: # NUMA nodes >= # sockets.
// This means that we support sub-NUMA clustering or systems with N NUMA nodes per CPU socket.
// However, currently we do not support systems with multiple sockets per NUMA.
// Those are relatively rare, but do exist.
type topology struct {
	// Maps socket ID to the IDs of the nNUMA nodes inside the socket.
	SocketToNNUMANodesIDs map[int]map[int]struct{}

	NNUMANodes map[int]*nNUMANode

	// We consider zNUMAs as external to sockets (even though in practice they might be considered
	// as belonging to one, maybe more, sockets, since they'll be connected to the host via a CXL
	// port that will be on a CPU socket).
	ZNUMANodes map[int]*zNUMANode

	// AllCPUs stores the IDs of all CPUs known to the manager. It can be constructed out of the
	// information scattered across the elements of NNUMANodes, but we keep it to avoid
	// reconstructing it every time this information is needed.
	AllCPUs cpuset.CPUSet

	CPUsPerCore uint16
	CPUsPerLLC  uint16

	// A subset of the shared pool which is not exclusively allocatable. The membership of this pool
	// is static for the lifetime of the Kubelet. The size of the reserved pool is
	// ceil(systemreserved.cpu + kubereserved.cpu). Reserved CPUs are either taken topologically
	// starting with lowest-indexed physical core, as reported by cAdvisor, or explicitly identified
	// via the kubelet invocation CLI or config file. These CPUs are spared for the OS, the
	// kubelet or other critical infrastructure components. Pods in BestEffort and Burstable QoS
	// classes can still run on them though.
	SystemReservedCPUs cpuset.CPUSet

	// When doing first fit, we want to allocate to containers that don't request far memory
	// starting from the nNUMAs that have least far memory neighboring them. So we keep a list
	// of nNUMAs IDs sorted in ascending order of far memory in neighboring zNUMAs.
	// TODO: consider making this a min heap.
	NNUMAsSortedByNeighborFarMemory []int

	// To speed up the search, we maintain max heaps of the nNUMA node IDs, where the sorting is done
	// based on the amount of free CPUs and memory. By knowing the maximum amount of free
	// memory/CPUs that a single nNUMA holds, we can determine the minimum number of nNUMA nodes
	// needed to satisfy an allocation, and we can start searching form combos of NUMAs of that size.
	nNUMAsByFreeCPUs *FreeResourcesMaxHeap
	nNUMAsByFreeMem  *FreeResourcesMaxHeap

	NUMADistanceMatrix map[int][]uint64
}

func (t *topology) numaNodeMem(id int) (*Mem, bool) {
	if n, ok := t.NNUMANodes[id]; ok {
		return &n.Mem, true
	}
	if n, ok := t.ZNUMANodes[id]; ok {
		return &n.Mem, true
	}
	return nil, false
}

type cpuParents struct {
	coreID, llcID int
}

type nNUMANode struct {
	ID       int
	SocketID int
	Mem
	FreeCPUs     cpuset.CPUSet
	ReservedCPUs cpuset.CPUSet

	IdleCoresToCPUs     map[int]cpuset.CPUSet
	BusyCoresToFreeCPUs map[int]cpuset.CPUSet

	IdleLLCsToCPUs     map[int]cpuset.CPUSet
	BusyLLCsToFreeCPUs map[int]cpuset.CPUSet

	cpuToCoreAndLLC map[int]cpuParents

	// If SNC is off, there's a 1:1 mapping between sockets and nNUMAs. So NeighborNNUMAsBySocket
	// maps each socket directly connected to this nNUMA's socket to the nNUMA contained by that
	// socket. If SNC is on, each socket contains more than one nNUMA. In that case,
	// NeighborNNUMAsBySocket has one entry corresponding to this nNUMA's own socket, and whose
	// values are the IDs of all the nNUMAs in that same socket (i.e. the other nNUMAs in the same
	// sub NUMA cluster). Plus, there's an entry for each socket directly connected to this nNUMA's
	// socket, whose values are all or some of the IDs of the nNUMAs in that socket. More precisely,
	// if all nNUMAs in that socket are equally distant from this nNUMA's socket, then they are all
	// in the list. If instead some of those nNUMAs are closer than others to this nNUMA's socket
	// (for example because the intel UPI port connecting the two sockets is in only one of the sub
	// nNUMAs), only the closest nNUMAs are in the list. Note that this means that some neighbors
	// are closer to this nNUMA than other neighbors. For example, on a two socket system with SNC
	// on, the nNUMAs in the same socket as this nNUMA are closer to it than the nNUMAs in a socket
	// directly connected to this nNUMA's socket.
	NeighborNNUMAsBySocket map[int]map[int]struct{}

	// The zNUMAs for which this nNUMA is (one of) the closest nNUMAs.
	NeighborZNUMAs map[int]struct{}
}

type zNUMANode struct {
	ID int
	Mem
	NeighborNNUMAsBySocket map[int]map[int]struct{}
}

type Mem struct {
	TotBytes            uint64
	AllocatableBytes    uint64
	SystemReservedBytes uint64
	FreeBytes           uint64
}

// TODO: state which symmetry assumptions we make (both in terms of distances and number of NUMAs
// per socket).
func initTopology(mi *cadvisor.MachineInfo) *topology {
	t := &topology{
		SocketToNNUMANodesIDs:           make(map[int]map[int]struct{}),
		NNUMANodes:                      make(map[int]*nNUMANode),
		NNUMAsSortedByNeighborFarMemory: make([]int, 0, len(mi.Topology)),
		ZNUMANodes:                      make(map[int]*zNUMANode),
		AllCPUs:                         cpuset.New(),
		SystemReservedCPUs:              cpuset.New(),
	}

	// This holds the ACPI SLIT table.
	distanceMatrix := make(map[int][]uint64, len(mi.Topology))

	// Populate all n and z NUMAs in the system using cadvisor's topology as source of truth.
	// Do not set neihghboring relationships yet.
	for _, numaNode := range mi.Topology {
		distanceMatrix[numaNode.Id] = numaNode.Distances

		// TODO: check if the following length is 0 even when we use simulation. Otherwise, we have
		// to check how many threads there are as well.
		if len(numaNode.Cores) == 0 {
			// If we're here, this NUMA node is a zNUMA.
			t.addZNUMA(numaNode)
		} else {
			// If we're here, this NUMA node is a nNUMA.
			t.addNNUMA(numaNode)
		}
	}

	for _, n := range t.NNUMANodes {
		for _, cCPUs := range n.IdleCoresToCPUs {
			t.CPUsPerCore = uint16(cCPUs.Size())
			break
		}
		for _, llcCPUs := range n.IdleLLCsToCPUs {
			t.CPUsPerLLC = uint16(llcCPUs.Size())
			break
		}
		break
	}

	// Now initialize the neighboring relationships between nodes.
	// First, initialize those between n and z NUMAs.
	// To do that, use Linux sysfs files described here: https://docs.kernel.org/admin-guide/mm/numaperf.html.
	// TODO: I strongly suspect that this breaks with our simulation approach. Double-check that.
	for _, zN := range t.ZNUMANodes {
		// zNUMAs might dynamically grow and shrink in terms of memory. In one of the systems I have
		// access to, if a zNUMA has 0 bytes, there are no access0 and access1 sysfs folders, hence
		// we can't use those folders for discovering neighboring relationships. But it doesn't
		// matter: their size is 0, so they'll never be part of an allocation (until they grow
		// in capactiy, but in that case we have to update the topology).
		if zN.Mem.TotBytes <= 0 {
			continue
		}

		// TODO: add comment on why we use both index 0 and 1 (in theory we shouldn't, but practice
		// mandates that).
		for i := 0; i <= 1; i++ {
			dir := "/sys/devices/system/node/node" + strconv.Itoa(zN.ID) + "/access" + strconv.Itoa(i) + "/initiators/"
			files, err := os.ReadDir(dir)
			if err != nil {
				panic(fmt.Errorf("failed to list files in %s while findind neighbors for zNUMA node %d: %v", dir, zN.ID, err))
			}

			for _, f := range files {
				neighborIDStr, ok := strings.CutPrefix(f.Name(), "node")
				if !ok {
					continue
				}

				neighborID, err := strconv.Atoi(neighborIDStr)
				if err != nil {
					continue
				}

				neighbor, ok := t.NNUMANodes[neighborID]
				if !ok {
					continue
				}

				if _, ok := zN.NeighborNNUMAsBySocket[neighbor.SocketID]; !ok {
					zN.NeighborNNUMAsBySocket[neighbor.SocketID] = make(map[int]struct{}, 1)
				}
				zN.NeighborNNUMAsBySocket[neighbor.SocketID][neighborID] = struct{}{}

				neighbor.NeighborZNUMAs[zN.ID] = struct{}{}
			}
		}
	}

	// Finally, initialize neighboring relationships between nNUMAs only.

	// First, handle SNC: if it's enabled, all nNUMAs in the same socket are neighbors.
	// We don't consider an nNUMA to be neighbor with itself.
	for s, allNNUMAsInSocket := range t.SocketToNNUMANodesIDs {
		if len(allNNUMAsInSocket) == 1 {
			continue
		}
		for n1ID := range allNNUMAsInSocket {
			n1 := t.NNUMANodes[n1ID]
			for n2ID := range allNNUMAsInSocket {
				if n2ID != n1ID {
					if _, ok := n1.NeighborNNUMAsBySocket[s]; !ok {
						n1.NeighborNNUMAsBySocket[s] = make(map[int]struct{})
					}
					n1.NeighborNNUMAsBySocket[s][n2ID] = struct{}{}
				}
			}
		}
	}

	// Now, handle neighbors outside of the same socket (SNC and non-SNC case are unified).
	for _, n1 := range t.NNUMANodes {
		distances := distanceMatrix[n1.ID]

		// Find the minimum distance between this nNUMA and other nNUMAs in different sockets.
		minDistance := uint64(math.MaxUint64)
		for s, nIDs := range t.SocketToNNUMANodesIDs {
			if s == n1.SocketID {
				continue
			}
			for n2ID := range nIDs {
				if distances[n2ID] < minDistance {
					minDistance = distances[n2ID]
				}
			}
		}

		// Now, record as neighbors all the nNUMAs with the min distance we have found.
		for s, nIDs := range t.SocketToNNUMANodesIDs {
			if s == n1.SocketID {
				continue
			}
			for n2ID := range nIDs {
				if distances[n2ID] == minDistance {
					if _, ok := n1.NeighborNNUMAsBySocket[s]; !ok {
						n1.NeighborNNUMAsBySocket[s] = make(map[int]struct{})
					}
					n1.NeighborNNUMAsBySocket[s][n2ID] = struct{}{}
				}
			}
		}
	}

	// Sort nNUMAs in ascending order of far memory in neighboring zNUMAs.
	// slices.SortFunc(t.NNUMAsSortedByNeighborFarMemory, func(n1, n2 int) int {
	// 	return t.farMemBytesInNeighbors(n1) - t.farMemBytesInNeighbors(n2)
	// })

	t.NUMADistanceMatrix = distanceMatrix

	// Make nNUMAs forget about zNUMAs
	for _, nN := range t.NNUMANodes {
		nN.NeighborZNUMAs = make(map[int]struct{})
	}

	// Convert zNUMAs to nNUMAs.
	for zNID, zN := range t.ZNUMANodes {
		newNN := &nNUMANode{
			ID:                  zNID,
			Mem:                 zN.Mem,
			FreeCPUs:            cpuset.New(),
			ReservedCPUs:        cpuset.New(),
			IdleCoresToCPUs:     make(map[int]cpuset.CPUSet),
			BusyCoresToFreeCPUs: make(map[int]cpuset.CPUSet),
			IdleLLCsToCPUs:      make(map[int]cpuset.CPUSet),
			BusyLLCsToFreeCPUs:  make(map[int]cpuset.CPUSet),
			cpuToCoreAndLLC:     make(map[int]cpuParents),
			NeighborZNUMAs:      make(map[int]struct{}),
		}

		var neighborNNUMA int
		for sock, neighborsInSock := range zN.NeighborNNUMAsBySocket {
			newNN.SocketID = sock
			t.SocketToNNUMANodesIDs[sock][zNID] = struct{}{}

			newNN.NeighborNNUMAsBySocket = make(map[int]map[int]struct{})
			newNN.NeighborNNUMAsBySocket[sock] = make(map[int]struct{})

			for nNID := range neighborsInSock {
				neighborNNUMA = nNID
				nN := t.NNUMANodes[nNID]
				if _, ok := nN.NeighborNNUMAsBySocket[sock]; !ok {
					nN.NeighborNNUMAsBySocket[sock] = make(map[int]struct{})
				}
				nN.NeighborNNUMAsBySocket[sock][zNID] = struct{}{}
				newNN.NeighborNNUMAsBySocket[sock][nNID] = struct{}{}
			}
		}

		t.NNUMANodes[zNID] = newNN
		t.NNUMAsSortedByNeighborFarMemory = append(t.NNUMAsSortedByNeighborFarMemory, newNN.ID)

		if _, ok := t.NUMADistanceMatrix[zNID]; !ok {
			t.NUMADistanceMatrix[zNID] = make([]uint64, len(machineInfo.Topology))
			for i := 0; i < len(machineInfo.Topology); i++ {
				if _, ok := t.NUMADistanceMatrix[i]; ok {
					t.NUMADistanceMatrix[zNID][i] = t.NUMADistanceMatrix[i][zNID]
				} else if i == zNID {
					// 10 is the convention for self references.
					t.NUMADistanceMatrix[zNID][zNID] = 10
				} else {
					t.NUMADistanceMatrix[zNID][i] = t.NUMADistanceMatrix[neighborNNUMA][i]
				}
			}
		}
	}
	t.ZNUMANodes = make(map[int]*zNUMANode)

	t.nNUMAsByFreeCPUs = newMaxHeap(t, true)
	t.nNUMAsByFreeMem = newMaxHeap(t, false)

	slices.Sort(t.NNUMAsSortedByNeighborFarMemory)

	return t
}

func (t *topology) addZNUMA(zn cadvisor.Node) {
	t.ZNUMANodes[zn.Id] = &zNUMANode{
		ID: zn.Id,
		Mem: Mem{
			TotBytes:         zn.Memory,
			AllocatableBytes: zn.Memory,
			FreeBytes:        zn.Memory,
		},
		NeighborNNUMAsBySocket: make(map[int]map[int]struct{}),
	}
}

func (t *topology) addNNUMA(nn cadvisor.Node) {
	// Glossary: with hyperthreading, a cpu is a hardware thread, while without
	// hyperthreading a CPU is a physical core (as far as this code is concerned).
	numCPUs := len(nn.Cores) * len(nn.Cores[0].Threads)
	cpusIDs := make([]int, 0, numCPUs)
	cpuToCoreAndLLC := make(map[int]cpuParents, numCPUs)
	idleCoresToCPUs := make(map[int]cpuset.CPUSet, len(nn.Cores))
	idleLLCsToCPUs := make(map[int]cpuset.CPUSet)

	for _, c := range nn.Cores {
		coreID, err := getUniqueCoreID(c.Threads)
		if err != nil {
			panic(fmt.Errorf("failed to get unique core ID: %v. This should never happen and is unrecoverable", err))
		}
		llcID := getUncoreCacheID(c)
		for _, cpuID := range c.Threads {
			cpuToCoreAndLLC[cpuID] = cpuParents{
				coreID: coreID,
				llcID:  llcID,
			}
		}
		cpusIDs = append(cpusIDs, c.Threads...)
		idleCoresToCPUs[coreID] = cpuset.New(c.Threads...)
		llcCPUs, ok := idleLLCsToCPUs[llcID]
		if !ok {
			llcCPUs = cpuset.New()
		}
		idleLLCsToCPUs[llcID] = llcCPUs.Union(cpuset.New(c.Threads...))
	}

	t.AllCPUs = t.AllCPUs.Union(cpuset.New(cpusIDs...))

	sockID := nn.Cores[0].SocketID

	t.NNUMANodes[nn.Id] = &nNUMANode{
		ID:       nn.Id,
		SocketID: sockID,
		Mem: Mem{
			TotBytes:         nn.Memory,
			AllocatableBytes: nn.Memory,
			FreeBytes:        nn.Memory,
		},
		IdleCoresToCPUs:        idleCoresToCPUs,
		BusyCoresToFreeCPUs:    make(map[int]cpuset.CPUSet),
		cpuToCoreAndLLC:        cpuToCoreAndLLC,
		FreeCPUs:               cpuset.New(cpusIDs...),
		ReservedCPUs:           cpuset.New(),
		NeighborZNUMAs:         make(map[int]struct{}, 0),
		NeighborNNUMAsBySocket: make(map[int]map[int]struct{}),
		IdleLLCsToCPUs:         idleLLCsToCPUs,
		BusyLLCsToFreeCPUs:     make(map[int]cpuset.CPUSet),
	}
	t.NNUMAsSortedByNeighborFarMemory = append(t.NNUMAsSortedByNeighborFarMemory, nn.Id)

	if _, ok := t.SocketToNNUMANodesIDs[sockID]; !ok {
		t.SocketToNNUMANodesIDs[sockID] = make(map[int]struct{}, 1)
	}
	t.SocketToNNUMANodesIDs[sockID][nn.Id] = struct{}{}
}

func (t *topology) maxFreeCPUsInSingleNNUMA() int {
	emptiestNNUMAID := t.nNUMAsByFreeCPUs.nNUMAs[0]
	nN := t.NNUMANodes[emptiestNNUMAID]
	return nN.FreeCPUs.Size()
}

func (t *topology) maxFreeMemInSingleNNUMA() uint64 {
	emptiestNNUMAID := t.nNUMAsByFreeMem.nNUMAs[0]
	nN := t.NNUMANodes[emptiestNNUMAID]
	return nN.FreeBytes
}

func newMaxHeap(t *topology, isAboutCPUs bool) *FreeResourcesMaxHeap {
	h := &FreeResourcesMaxHeap{
		t:           t,
		nNUMAs:      make([]int, len(t.NNUMAsSortedByNeighborFarMemory)),
		isAboutCPUs: isAboutCPUs,
	}

	copy(h.nNUMAs, t.NNUMAsSortedByNeighborFarMemory)
	heap.Init(h)

	return h
}

type FreeResourcesMaxHeap struct {
	t           *topology
	nNUMAs      []int
	isAboutCPUs bool
}

func (h *FreeResourcesMaxHeap) Len() int {
	return len(h.nNUMAs)
}

func (h *FreeResourcesMaxHeap) Less(i, j int) bool {
	n1ID := h.nNUMAs[i]
	n2ID := h.nNUMAs[j]

	n1, ok1 := h.t.NNUMANodes[n1ID]
	if !ok1 {
		panic(fmt.Errorf("free CPUs heap's Less() invoked with non-existent NUMA node ID %d", n1ID))
	}

	n2, ok2 := h.t.NNUMANodes[n2ID]
	if !ok2 {
		panic(fmt.Errorf("free CPUs heap's Less() invoked with non-existent NUMA node ID %d", n2ID))
	}

	// Note that n1 is "Less than" n2 if it has *more* free resources, because we need a max
	// heap, but Go stdlib's container/heap functions implement a min heap. So we have to flip the
	// meaning of Less().

	if h.isAboutCPUs {
		return n1.FreeCPUs.Size() > n2.FreeCPUs.Size()
	}

	return n1.FreeBytes > n2.FreeBytes
}

func (h *FreeResourcesMaxHeap) Swap(i, j int) {
	h.nNUMAs[i], h.nNUMAs[j] = h.nNUMAs[j], h.nNUMAs[i]
}

// The population of nNUMAs is assumed to be fixed, so we don't need Push and Pop, but we implement
// them to satisfy the "container/heap" interface of Go's stdlib.

func (*FreeResourcesMaxHeap) Push(x any) {
	panic("Push is unimplemented and you should never call it.")
}

func (*FreeResourcesMaxHeap) Pop() any {
	panic("Pop is unimplemented and you should never call it.")
}

// copied verbatim from cpu manager package.
func getUniqueCoreID(threads []int) (coreID int, err error) {
	if len(threads) == 0 {
		return 0, fmt.Errorf("no cpus provided")
	}

	if len(threads) != cpuset.New(threads...).Size() {
		return 0, fmt.Errorf("cpus provided are not unique")
	}

	min := threads[0]
	for _, thread := range threads[1:] {
		if thread < min {
			min = thread
		}
	}

	return min, nil
}

// copied verbatim from CPU manager package.
func getUncoreCacheID(core cadvisor.Core) int {
	if len(core.UncoreCaches) < 1 {
		// In case cAdvisor is nil, failback to socket alignment since uncorecache is not shared
		return core.SocketID
	}
	// Even though cadvisor API returns a slice, we only expect either 0 or a 1 uncore caches,
	// so everything past the first entry should be discarded or ignored
	return core.UncoreCaches[0].Id
}

func (t *topology) farMemBytesInNeighbors(id int) int {
	nNUMA := t.NNUMANodes[id]

	farMemBytes := uint64(0)
	for neighborZNUMA := range nNUMA.NeighborZNUMAs {
		farMemBytes += t.ZNUMANodes[neighborZNUMA].Mem.AllocatableBytes
	}

	if farMemBytes > uint64(math.MaxInt) {
		panic(fmt.Errorf("amount of far memory neighboring nNUMA %d overflows", id))
	}

	return int(farMemBytes)
}
