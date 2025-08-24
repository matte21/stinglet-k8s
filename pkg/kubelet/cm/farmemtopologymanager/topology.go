package farmemtopologymanager

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	_ "k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/utils/cpuset"

	cadvisor "github.com/google/cadvisor/info/v1"
)

// TODO: consider just using hwloc (and its hardware model, which is different, and probably better,
// than ours). So far I haven't done so under the (perhaps incorrect) assumption that it'd have been
// faster to implement this code by myself without having to figure out how to integrate hwloc
// (written in C) with this Go code.

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

	// A subset of the shared pool (AllCPUs - cpus reserved for guaranteed pods) which is not
	// exclusively allocatable. The membership of this pool is static for the lifetime of the
	// Kubelet. The size of the reserved pool is ceil(systemreserved.cpu + kubereserved.cpu).
	// Reserved CPUs are either taken topologically starting with lowest-indexed physical core, as
	// reported by cAdvisor, or explicitly identified via the kubelet invocation CLI or config file.
	// These CPUs are spared for the OS, the kubelet or other critical infrastructure components.
	// Pods in BestEffort and Burstable QoS classes can still run on them though.
	SystemReservedCPUs cpuset.CPUSet
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

type nNUMANode struct {
	ID       int
	SocketID int
	Mem
	FreeCPUs     cpuset.CPUSet
	ReservedCPUs cpuset.CPUSet

	// If SNC is off, there's a 1:1 mapping between sockets and nNUMAs. So SocketToNeighborNNUMAtoLatencyNs
	// maps each socket directly connected to this nNUMA's socket to the nNUMA contained by that
	// socket. If SNC is on, each socket contains more than one nNUMA. In that case,
	// SocketToNeighborNNUMAtoLatencyNs has one entry corresponding to this nNUMA's own socket, and whose
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
	SocketToNeighborNNUMAtoLatencyNs map[int]map[int]uint32

	// The zNUMAs for which this nNUMA is (one of) the closest nNUMAs.
	NeighborZNUMAToLatencyNs map[int]uint32
}

type zNUMANode struct {
	ID int
	Mem
}

type Mem struct {
	TotBytes            uint64
	AllocatableBytes    uint64
	SystemReservedBytes uint64
	FreeBytes           uint64
}

// TODO: state which symmetry assumptions we make (both in terms of distances and number of NUMAs
// per socket).
func initTopology(machineInfo *cadvisor.MachineInfo) *topology {
	t := &topology{
		SocketToNNUMANodesIDs: make(map[int]map[int]struct{}),
		NNUMANodes:            make(map[int]*nNUMANode),
		ZNUMANodes:            make(map[int]*zNUMANode),
		AllCPUs:               cpuset.New(),
		SystemReservedCPUs:    cpuset.New(),
	}

	// This holds the ACPI SLIT table. We'll use it to compute which NUMA node is neighbor to which
	// other NUMA node.
	distanceMatrix := make(map[int][]uint64, len(machineInfo.Topology))

	// Populate all n and z NUMAs in the system using cadvisor's topology as source of truth.
	// Do not set neighboring relationships yet.
	for _, numaNode := range machineInfo.Topology {
		distanceMatrix[numaNode.Id] = numaNode.Distances

		// TODO: check if the following length is 0 even when we use simulation. Otherwise, we have
		// to check how many threads there are as well.
		if len(numaNode.Cores) == 0 {
			// If we're here, this NUMA node is a zNUMA.
			t.addZNUMANode(numaNode)
		} else {
			// If we're here, this NUMA node is a nNUMA.
			t.addNNUMANode(numaNode)
		}
	}

	// Now initialize the neighboring relationships between nodes.
	// First, initialize those between n and z NUMAs.
	// To do that, use Linux sysfs files described here: https://docs.kernel.org/admin-guide/mm/numaperf.html.
	// TODO: I strongly suspect that this breaks with our simulation approach. Double-check that.
	t.addNtoZNUMAsNeighborRelationships()

	// Finally, initialize neighboring relationships between nNUMAs only.
	t.addNtoNNUMAsNeighborRelationships(distanceMatrix)

	return t
}

// mutates t.
func (t *topology) addNtoNNUMAsNeighborRelationships(distanceMatrix map[int][]uint64) {
	// First, handle Sub NUMA Clustering (SNC): if it's enabled, all nNUMAs in the same socket are
	// neighbors. We don't consider an nNUMA to be neighbor with itself.
	for s, allNNUMAsInSocket := range t.SocketToNNUMANodesIDs {
		if len(allNNUMAsInSocket) == 1 {
			continue
		}
		for n1ID := range allNNUMAsInSocket {
			n1 := t.NNUMANodes[n1ID]
			for n2ID := range allNNUMAsInSocket {
				if n2ID != n1ID {
					if _, ok := n1.SocketToNeighborNNUMAtoLatencyNs[s]; !ok {
						n1.SocketToNeighborNNUMAtoLatencyNs[s] = make(map[int]uint32)
					}
					n1.SocketToNeighborNNUMAtoLatencyNs[s][n2ID] = 0
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
					if _, ok := n1.SocketToNeighborNNUMAtoLatencyNs[s]; !ok {
						n1.SocketToNeighborNNUMAtoLatencyNs[s] = make(map[int]uint32)
					}
					n1.SocketToNeighborNNUMAtoLatencyNs[s][n2ID] = 0
				}
			}
		}
	}
}

// mutates t.
func (t *topology) addNtoZNUMAsNeighborRelationships() {
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

				neighbor.NeighborZNUMAToLatencyNs[zN.ID] = 0
			}
		}
	}
}

// mutates t.
func (t *topology) addNNUMANode(nNode cadvisor.Node) {
	// Glossary: with hyperthreading, a cpu is a hardware thread, while without
	// hyperthreading a CPU is a physical core (as far as this code is concerned).
	// We assume all cores have the same number of threads.
	cpusIDs := make([]int, 0, len(nNode.Cores)*len(nNode.Cores[0].Threads))

	// The following code assumes that core ID = thread ID when hyper-threading is off.
	// I didn't check the assumption myself, but the vanilla K8s CPU manager code makes the same
	// assumption, so I guess it's a safe assumption to make (or if it's not, we're not introducing
	// a new bug, we're only keeping a pre-existing one).
	for _, c := range nNode.Cores {
		cpusIDs = append(cpusIDs, c.Threads...)
	}

	t.AllCPUs = t.AllCPUs.Union(cpuset.New(cpusIDs...))

	sockID := nNode.Cores[0].SocketID

	t.NNUMANodes[nNode.Id] = &nNUMANode{
		ID:       nNode.Id,
		SocketID: sockID,
		Mem: Mem{
			TotBytes:         nNode.Memory,
			AllocatableBytes: nNode.Memory,
			FreeBytes:        nNode.Memory,
		},
		// Note: always assuming all CPUs are free means that this code breaks (badly) if the
		// kubelet crashes and restarts, because we forget which CPUs were reserved before the
		// crash. Fixing this isn't that hard conceptually, but it's more code and I don't have time
		// given that it's not critical for a research project. But at some point we might want to
		// fix this.
		FreeCPUs:     cpuset.New(cpusIDs...),
		ReservedCPUs: cpuset.New(),
		// Neighbor relationships are initialized later, separately, so for now we set them to empty
		// values.
		NeighborZNUMAToLatencyNs:         make(map[int]uint32, 0),
		SocketToNeighborNNUMAtoLatencyNs: make(map[int]map[int]uint32),
	}

	if _, ok := t.SocketToNNUMANodesIDs[sockID]; !ok {
		t.SocketToNNUMANodesIDs[sockID] = make(map[int]struct{}, 1)
	}
	t.SocketToNNUMANodesIDs[sockID][nNode.Id] = struct{}{}
}

// mutates t.
func (t *topology) addZNUMANode(zNode cadvisor.Node) {
	t.ZNUMANodes[zNode.Id] = &zNUMANode{
		ID: zNode.Id,
		Mem: Mem{
			TotBytes:         zNode.Memory,
			AllocatableBytes: zNode.Memory,
			FreeBytes:        zNode.Memory,
		},
	}
}
