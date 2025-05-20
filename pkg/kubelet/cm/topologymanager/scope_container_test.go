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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

func TestContainerCalculateAffinity(t *testing.T) {
	tcases := []struct {
		name     string
		hp       []HintProvider
		expected []map[string][]TopologyHint
	}{
		{
			name:     "No hint providers",
			hp:       []HintProvider{},
			expected: ([]map[string][]TopologyHint)(nil),
		},
		{
			name: "HintProvider returns empty non-nil map[string][]TopologyHint",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{},
				},
			},
			expected: []map[string][]TopologyHint{
				{},
			},
		},
		{
			name: "HintProvider returns -nil map[string][]TopologyHint from provider",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": nil,
					},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource": nil,
				},
			},
		},
		{
			name: "Assorted HintProviders",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource-1/A": {
							{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
							{NUMANodeAffinity: NewTestBitMask(0, 1), Preferred: false},
						},
						"resource-1/B": {
							{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
							{NUMANodeAffinity: NewTestBitMask(1, 2), Preferred: false},
						},
					},
				},
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource-2/A": {
							{NUMANodeAffinity: NewTestBitMask(2), Preferred: true},
							{NUMANodeAffinity: NewTestBitMask(3, 4), Preferred: false},
						},
						"resource-2/B": {
							{NUMANodeAffinity: NewTestBitMask(2), Preferred: true},
							{NUMANodeAffinity: NewTestBitMask(3, 4), Preferred: false},
						},
					},
				},
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource-3": nil,
					},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource-1/A": {
						{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
						{NUMANodeAffinity: NewTestBitMask(0, 1), Preferred: false},
					},
					"resource-1/B": {
						{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
						{NUMANodeAffinity: NewTestBitMask(1, 2), Preferred: false},
					},
				},
				{
					"resource-2/A": {
						{NUMANodeAffinity: NewTestBitMask(2), Preferred: true},
						{NUMANodeAffinity: NewTestBitMask(3, 4), Preferred: false},
					},
					"resource-2/B": {
						{NUMANodeAffinity: NewTestBitMask(2), Preferred: true},
						{NUMANodeAffinity: NewTestBitMask(3, 4), Preferred: false},
					},
				},
				{
					"resource-3": nil,
				},
			},
		},
	}

	for _, tc := range tcases {
		ctnScope := &containerScope{
			scope{
				hintProviders: tc.hp,
				policy:        &mockPolicy{},
				name:          podTopologyScope,
			},
		}

		ctnScope.calculateAffinity(&v1.Pod{}, &v1.Container{})
		actual := ctnScope.policy.(*mockPolicy).ph
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("Test Case: %s", tc.name)
			t.Errorf("Expected result to be %v, got %v", tc.expected, actual)
		}
	}
}

func TestContainerAccumulateProvidersHints(t *testing.T) {
	tcases := []struct {
		name     string
		hp       []HintProvider
		expected []map[string][]TopologyHint
	}{
		{
			name:     "TopologyHint not set",
			hp:       []HintProvider{},
			expected: nil,
		},
		{
			name: "HintProvider returns empty non-nil map[string][]TopologyHint",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{},
				},
			},
			expected: []map[string][]TopologyHint{
				{},
			},
		},
		{
			name: "HintProvider returns - nil map[string][]TopologyHint from provider",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": nil,
					},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource": nil,
				},
			},
		},
		{
			name: "2 HintProviders with 1 resource returns hints",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource1": {TopologyHint{}},
					},
				},
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource2": {TopologyHint{}},
					},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource1": {TopologyHint{}},
				},
				{
					"resource2": {TopologyHint{}},
				},
			},
		},
		{
			name: "2 HintProviders 1 with 1 resource 1 with nil hints",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource1": {TopologyHint{}},
					},
				},
				&mockHintProvider{nil},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource1": {TopologyHint{}},
				},
				nil,
			},
		},
		{
			name: "2 HintProviders 1 with 1 resource 1 empty hints",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource1": {TopologyHint{}},
					},
				},
				&mockHintProvider{
					map[string][]TopologyHint{},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource1": {TopologyHint{}},
				},
				{},
			},
		},
		{
			name: "HintProvider with 2 resources returns hints",
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource1": {TopologyHint{}},
						"resource2": {TopologyHint{}},
					},
				},
			},
			expected: []map[string][]TopologyHint{
				{
					"resource1": {TopologyHint{}},
					"resource2": {TopologyHint{}},
				},
			},
		},
	}

	for _, tc := range tcases {
		ctnScope := containerScope{
			scope{
				hintProviders: tc.hp,
			},
		}
		actual := ctnScope.accumulateProvidersHints(&v1.Pod{}, &v1.Container{})
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("Test Case %s: Expected NUMANodeAffinity in result to be %v, got %v", tc.name, tc.expected, actual)
		}
	}
}

func TestAdmitWFarMem(t *testing.T) {
	testCases := []struct {
		name                string
		hintProviders       []HintProvider
		farMemHint          TopologyHint
		expectedResult      lifecycle.PodAdmitResult
		expectedAlignedHint TopologyHint
		expectedFarMemHint  TopologyHint
	}{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctnScope := cntScope(tc.hintProviders, tc.farMemHint)

			podUID := "pod1"
			cntName := "container1"
			pod := guaranteedPod(podUID, cntName)

			admitRes := ctnScope.Admit(pod)

			if admitRes != tc.expectedResult {
				t.Fatalf("Got admit result %#v, expected %#v", admitRes, tc.expectedResult)
			}

			alignedHint := ctnScope.GetAffinity(podUID, cntName)
			if !reflect.DeepEqual(alignedHint, tc.expectedAlignedHint) {
				t.Fatalf("Got aligned hint %#v, expected %#v", alignedHint, tc.expectedAlignedHint)
			}

			farMemHint := ctnScope.GetFarMemAffinity(podUID, cntName)
			if !reflect.DeepEqual(farMemHint, tc.expectedFarMemHint) {
				t.Fatalf("Got far mem hint %#v, expected %#v", farMemHint, tc.expectedFarMemHint)
			}
		})
	}
}

func guaranteedPod(uid, containerName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      containerName,
					Resources: v1.ResourceRequirements{},
				},
			},
		},
		Status: v1.PodStatus{
			QOSClass: v1.PodQOSGuaranteed,
		},
	}
}

func cntScope(hps []HintProvider, farMemHint TopologyHint) containerScope {
	return containerScope{
		scope{
			podTopologyHints: podTopologyHints{},
			policy: NewBestEffortPolicy(
				commonNUMAInfoEightNodes(),
				PolicyOptions{false, defaultMaxAllowableNUMANodes},
			),
			podMap:            containermap.NewContainerMap(),
			podFarMemAffinity: map[string]map[string]TopologyHint{},
			hintProviders:     hps,
			farMemMgr: &mockHintProvider{
				th: map[string][]TopologyHint{
					string(v1.ResourceMemory): []TopologyHint{farMemHint},
				},
			},
		},
	}
}
