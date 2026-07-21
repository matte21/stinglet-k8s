package farmemtopologymanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// dcmfmClient asks Pangaea's DCMFM Agent to online more far-memory capacity on
// demand, instead of only ever working with whatever zNUMA capacity existed
// at kubelet startup. It is optional: if DCMFM_AGENT_ADDR is unset, there is
// no client, and callers must fall back to today's fixed-topology behavior
// exactly.
//
// DCMFM_AGENT_ADDR should look like "http://127.0.0.1:4000" (no trailing
// slash), matching the DCMFM Agent's own address/port config.
type dcmfmClient struct {
	baseURL    string
	httpClient *http.Client
}

func newDCMFMClientFromEnv() *dcmfmClient {
	addr := os.Getenv("DCMFM_AGENT_ADDR")
	if addr == "" {
		return nil
	}
	return &dcmfmClient{
		baseURL:    addr,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// podRequest mirrors Pangaea's DCMFM_Agent/app/model.PodRequest -- the JSON
// body DCMFM Agent's memblock allocation endpoint expects.
type podRequest struct {
	NodeName      string `json:"node_name"`
	PodName       string `json:"pod_name"`
	PodId         string `json:"pod_id"`
	PodNamespace  string `json:"pod_namespace"`
	ClaimCapacity string `json:"claim_capacity"`
}

// requestMoreFarMem asks DCMFM Agent to online at least sizeMB more MB of
// far-memory. On success, the caller should re-read the zNUMA node's real
// capacity from sysfs rather than trust a computed delta -- sysfs is ground
// truth for how much actually got onlined. Returns an error describing why no
// additional capacity is available (including the now-fixed clean
// "insufficient far-memory capacity" response DCMFM Agent sends when it has
// nothing to give).
func (c *dcmfmClient) requestMoreFarMem(sizeMB int, podName, podNamespace, podUID string) error {
	body, err := json.Marshal(podRequest{
		NodeName:      nodeNameFromEnv(),
		PodName:       podName,
		PodId:         podUID,
		PodNamespace:  podNamespace,
		ClaimCapacity: fmt.Sprintf("%d", sizeMB),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal DCMFM request: %v", err)
	}

	url := fmt.Sprintf("%s/api/v1/memblocks?size=%d", c.baseURL, sizeMB)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build DCMFM request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DCMFM Agent unreachable at %s: %v", c.baseURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DCMFM Agent declined request (status %d): no additional far-memory capacity available", resp.StatusCode)
	}

	return nil
}

func nodeNameFromEnv() string {
	if n := os.Getenv("NODE_NAME"); n != "" {
		return n
	}
	hostname, _ := os.Hostname()
	return hostname
}
