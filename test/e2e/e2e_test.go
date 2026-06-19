package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	e2eNamespace = "storage"

	e2eSourceNode = "worker-a"
	e2eTargetNode = "worker-b"

	e2eLabelPrefix           = "zfsreplication.example.com"
	e2eLeaseStateAnnotation  = e2eLabelPrefix + "/state"
	e2eSimEnvAnnotation      = e2eLabelPrefix + "/sim-env-"
	e2eBootstrapFailIfNoBase = "FailIfNoBase"
	e2eBootstrapDestroyFull  = "DestroyTargetAndReceiveFull"
)

func TestE2EFullAndIncrementalReplication(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	name := "e2e-flow-" + suffix
	first := replicationCase{
		Name:           name,
		RunID:          "r-" + suffix + "-1",
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/src-" + suffix,
		TargetDataset:  "tank/dst-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapDestroyFull,
	}
	k.cleanupReplicationOnExit(name)
	k.cleanupReplication(name)

	k.applyReplication(first)
	firstStatus := k.waitForSuccess(first, 4*time.Minute)
	firstEvents := k.zfsSimEvents(first.Name, first.RunID)
	assertRunTopology(t, first, firstEvents)
	firstGUID := assertFullBootstrapZFSEvents(t, first, firstEvents)
	assertSucceededStatus(t, first, firstStatus, firstGUID)
	k.assertSecretDeleted(firstStatus.TokenSecretName)
	k.assertLeaseState(first.Name, "succeeded")

	second := first
	second.RunID = "r-" + suffix + "-2"
	second.BootstrapMode = e2eBootstrapFailIfNoBase
	k.applyReplication(second)
	secondStatus := k.waitForSuccess(second, 4*time.Minute)
	secondEvents := k.zfsSimEvents(second.Name, second.RunID)
	assertRunTopology(t, second, secondEvents)
	secondGUID := assertIncrementalZFSEvents(t, second, first.snapshotName(), secondEvents)
	assertSucceededStatus(t, second, secondStatus, secondGUID)
	k.assertSecretDeleted(secondStatus.TokenSecretName)
	k.assertLeaseState(second.Name, "succeeded")
}

func TestE2EFailIfNoBase(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	sc := replicationCase{
		Name:           "e2e-no-base-" + suffix,
		RunID:          "r-" + suffix,
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/src-no-base-" + suffix,
		TargetDataset:  "tank/dst-no-base-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapFailIfNoBase,
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedStatus(t, sc, status, "no base snapshot")
	k.assertLeaseState(sc.Name, "failed")

	events := k.zfsSimEvents(sc.Name, sc.RunID)
	assertRunTopology(t, sc, events)
	assertFailIfNoBaseEvents(t, sc, events)
}

func TestE2ESameDatasetRejectedBeforeJobs(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	sc := replicationCase{
		Name:           "e2e-same-ds-" + suffix,
		RunID:          "r-" + suffix,
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/same-" + suffix,
		TargetDataset:  "tank/same-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapDestroyFull,
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 2*time.Minute)
	assertFailedStatus(t, sc, status, "source and target datasets must differ")
	k.assertNoJobsOrSecrets(sc.Name)
	k.assertNoLease(sc.Name)
}

func TestE2ESendFailure(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	sc := replicationCase{
		Name:           "e2e-send-fail-" + suffix,
		RunID:          "r-" + suffix,
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/src-send-fail-" + suffix,
		TargetDataset:  "tank/dst-send-fail-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapDestroyFull,
		Annotations: map[string]string{
			e2eSimEnvAnnotation + "ZFS_SIM_FAIL_SEND": "1",
		},
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedStatus(t, sc, status, "forced send failure")
	k.assertLeaseState(sc.Name, "failed")

	events := k.zfsSimEvents(sc.Name, sc.RunID)
	assertRunTopology(t, sc, events)
	assertSendFailureEvents(t, sc, events)
}

func TestE2EReceiveFailure(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	sc := replicationCase{
		Name:           "e2e-recv-fail-" + suffix,
		RunID:          "r-" + suffix,
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/src-recv-fail-" + suffix,
		TargetDataset:  "tank/dst-recv-fail-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapDestroyFull,
		Annotations: map[string]string{
			e2eSimEnvAnnotation + "ZFS_SIM_FAIL_RECEIVE": "1",
		},
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedStatus(t, sc, status, "forced receive failure")
	k.assertLeaseState(sc.Name, "failed")

	events := k.zfsSimEvents(sc.Name, sc.RunID)
	assertRunTopology(t, sc, events)
	assertReceiveFailureEvents(t, sc, events)
}

func TestE2EMountedTargetRejectedBeforeDestroy(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	sc := replicationCase{
		Name:           "e2e-mounted-" + suffix,
		RunID:          "r-" + suffix,
		SourceNode:     e2eSourceNode,
		TargetNode:     e2eTargetNode,
		SourceDataset:  "tank/src-mounted-" + suffix,
		TargetDataset:  "tank/dst-mounted-" + suffix,
		SnapshotPrefix: "zsync",
		BootstrapMode:  e2eBootstrapDestroyFull,
		Annotations: map[string]string{
			e2eSimEnvAnnotation + "ZFS_SIM_MOUNTED": "yes",
		},
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedStatus(t, sc, status, "target dataset mounted")
	k.assertLeaseState(sc.Name, "failed")

	events := k.zfsSimEvents(sc.Name, sc.RunID)
	assertRunTopology(t, sc, events)
	assertMountedTargetEvents(t, sc, events)
}

type replicationCase struct {
	Name           string
	RunID          string
	SourceNode     string
	TargetNode     string
	SourceDataset  string
	TargetDataset  string
	SnapshotPrefix string
	BootstrapMode  string
	Annotations    map[string]string
}

func (c replicationCase) snapshotName() string {
	prefix := c.SnapshotPrefix
	if prefix == "" {
		prefix = "zsync"
	}
	return prefix + "-" + c.RunID
}

func (c replicationCase) sourceSnapshot() string {
	return c.SourceDataset + "@" + c.snapshotName()
}

func (c replicationCase) targetSnapshot() string {
	return c.TargetDataset + "@" + c.snapshotName()
}

func (c replicationCase) manifestJSON(t *testing.T) []byte {
	t.Helper()
	metadata := map[string]any{
		"name":      c.Name,
		"namespace": e2eNamespace,
	}
	if len(c.Annotations) > 0 {
		metadata["annotations"] = c.Annotations
	}
	doc := map[string]any{
		"apiVersion": "zfsreplication.example.com/v1alpha1",
		"kind":       "ZFSReplication",
		"metadata":   metadata,
		"spec": map[string]any{
			"runID": c.RunID,
			"source": map[string]any{
				"nodeName": c.SourceNode,
				"dataset":  c.SourceDataset,
			},
			"target": map[string]any{
				"nodeName": c.TargetNode,
				"dataset":  c.TargetDataset,
			},
			"snapshotPrefix": c.SnapshotPrefix,
			"bootstrap": map[string]any{
				"mode": c.BootstrapMode,
			},
			"receive": map[string]any{
				"receiveUnmounted": true,
				"resumable":        true,
			},
		},
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type kubectlRunner struct {
	t          *testing.T
	path       string
	kubeconfig string
}

func newKubectlRunner(t *testing.T) kubectlRunner {
	t.Helper()
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("KUBECONFIG is required for VM e2e tests")
	}
	if _, err := os.Stat(kubeconfig); err != nil {
		t.Skipf("KUBECONFIG is not usable: %v", err)
	}
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skipf("kubectl is required for VM e2e tests: %v", err)
	}
	k := kubectlRunner{t: t, path: kubectl, kubeconfig: kubeconfig}
	k.ensureNamespace(e2eNamespace)
	return k
}

func (k kubectlRunner) ensureNamespace(namespace string) {
	if _, err := k.runOutput(20*time.Second, "get", "namespace", namespace); err == nil {
		return
	}
	if _, err := k.runOutput(20*time.Second, "create", "namespace", namespace); err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		k.t.Fatal(err)
	}
}

func (k kubectlRunner) cleanupReplication(name string) {
	k.run(75*time.Second, "delete", "zfsreplication", name, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "jobs,pods,secrets", "-n", e2eNamespace, "-l", e2eLabelPrefix+"/name="+name, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "lease", "zfsrep-"+name, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
}

func (k kubectlRunner) cleanupReplicationOnExit(name string) {
	k.t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		k.t.Logf("leaving e2e resources for %s because E2E_KEEP_RESOURCES=1", name)
		return
	}
	k.t.Cleanup(func() { k.cleanupReplication(name) })
}

func (k kubectlRunner) applyReplication(sc replicationCase) {
	k.t.Helper()
	manifest := sc.manifestJSON(k.t)
	if _, err := k.runInput(30*time.Second, manifest, "apply", "-f", "-"); err != nil {
		k.t.Fatalf("apply replication manifest:\n%s\n%v", manifest, err)
	}
}

func (k kubectlRunner) waitForSuccess(sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForStatus(sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Succeeded" && st.LastSuccessfulRunID == sc.RunID
	}, "Succeeded with lastSuccessfulRunID="+sc.RunID)
}

func (k kubectlRunner) waitForFailed(sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForStatus(sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Failed" && st.LastAttemptedRunID == sc.RunID
	}, "Failed with lastAttemptedRunID="+sc.RunID)
}

func (k kubectlRunner) waitForStatus(sc replicationCase, timeout time.Duration, ready func(replicationStatus) bool, want string) replicationStatus {
	k.t.Helper()
	deadline := time.Now().Add(timeout)
	var last replicationStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := k.getStatus(sc.Name)
		if err == nil {
			last = status
			if ready(status) {
				return status
			}
			if status.Phase == "Failed" && !strings.HasPrefix(want, "Failed") {
				k.collectDiagnostics(sc.Name)
				k.t.Fatalf("%s reached Failed while waiting for %s: %#v", sc.Name, want, status)
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	k.collectDiagnostics(sc.Name)
	k.t.Fatalf("timed out waiting for %s to reach %s; last status=%#v last error=%v", sc.Name, want, last, lastErr)
	return replicationStatus{}
}

func (k kubectlRunner) getStatus(name string) (replicationStatus, error) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "zfsreplication", name, "-n", e2eNamespace, "-o", "json")
	if err != nil {
		return replicationStatus{}, err
	}
	var obj replicationObject
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return replicationStatus{}, fmt.Errorf("parse zfsreplication status: %w\n%s", err, out)
	}
	return obj.Status, nil
}

func (k kubectlRunner) collectDiagnostics(name string) {
	for _, args := range [][]string{
		{"get", "zfsreplication", name, "-n", e2eNamespace, "-o", "yaml"},
		{"get", "pods,jobs,secrets,leases", "-n", e2eNamespace, "-o", "wide"},
		{"get", "events", "-n", e2eNamespace, "--sort-by=.lastTimestamp"},
		{"logs", "-n", "zfsreplication-system", "deployment/zfsreplication-controller"},
	} {
		if out, err := k.runOutput(25*time.Second, args...); err == nil {
			k.t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
		} else {
			k.t.Logf("kubectl %s failed: %v", strings.Join(args, " "), err)
		}
	}

	pods, err := k.podsForReplication(name, "")
	if err != nil {
		k.t.Logf("list datamover pods for diagnostics failed: %v", err)
		return
	}
	for _, pod := range pods.Items {
		args := []string{"logs", "-n", e2eNamespace, "pod/" + pod.Metadata.Name, "--all-containers=true"}
		if out, err := k.runOutput(20*time.Second, args...); err == nil {
			k.t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
		} else {
			k.t.Logf("kubectl %s failed: %v", strings.Join(args, " "), err)
		}
	}
}

func (k kubectlRunner) zfsSimEvents(name, runID string) []zfsSimEvent {
	k.t.Helper()
	pods, err := k.podsForReplication(name, runID)
	if err != nil {
		k.t.Fatal(err)
	}
	if len(pods.Items) == 0 {
		k.t.Fatalf("no datamover pods found for %s/%s", name, runID)
	}

	var events []zfsSimEvent
	for _, pod := range pods.Items {
		logs, err := k.runOutput(20*time.Second, "logs", "-n", e2eNamespace, "pod/"+pod.Metadata.Name, "--all-containers=true")
		if err != nil {
			k.t.Fatal(err)
		}
		events = append(events, parseZFSSimEvents(k.t, pod.Metadata.Name, logs)...)
	}
	if len(events) == 0 {
		k.t.Fatalf("no zfs simulator events found in datamover pod logs for %s/%s", name, runID)
	}
	return events
}

func (k kubectlRunner) podsForReplication(name, runID string) (podList, error) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/name=" + name
	if runID != "" {
		selector += "," + e2eLabelPrefix + "/run-id=" + runID
	}
	out, err := k.runOutput(20*time.Second, "get", "pods", "-n", e2eNamespace, "-l", selector, "-o", "json")
	if err != nil {
		return podList{}, err
	}
	var pods podList
	if err := json.Unmarshal([]byte(out), &pods); err != nil {
		return podList{}, fmt.Errorf("parse pod list: %w\n%s", err, out)
	}
	return pods, nil
}

func (k kubectlRunner) assertSecretDeleted(name string) {
	k.t.Helper()
	if name == "" {
		k.t.Fatal("status tokenSecretName is empty")
	}
	if out, err := k.runOutput(15*time.Second, "get", "secret", name, "-n", e2eNamespace); err == nil {
		k.t.Fatalf("token secret %s still exists:\n%s", name, out)
	} else if !strings.Contains(err.Error(), "NotFound") {
		k.t.Fatalf("check token secret %s deletion: %v", name, err)
	}
}

func (k kubectlRunner) assertNoJobsOrSecrets(name string) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "jobs,secrets", "-n", e2eNamespace, "-l", e2eLabelPrefix+"/name="+name, "-o", "name")
	if err != nil && !strings.Contains(err.Error(), "No resources found") {
		k.t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		k.t.Fatalf("jobs/secrets exist for %s:\n%s", name, out)
	}
}

func (k kubectlRunner) assertLeaseState(name, want string) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "lease", "zfsrep-"+name, "-n", e2eNamespace, "-o", "json")
	if err != nil {
		k.t.Fatal(err)
	}
	var lease leaseObject
	if err := json.Unmarshal([]byte(out), &lease); err != nil {
		k.t.Fatalf("parse lease: %v\n%s", err, out)
	}
	if got := lease.Metadata.Annotations[e2eLeaseStateAnnotation]; got != want {
		k.t.Fatalf("lease state = %q, want %q\n%s", got, want, out)
	}
}

func (k kubectlRunner) assertNoLease(name string) {
	k.t.Helper()
	if out, err := k.runOutput(15*time.Second, "get", "lease", "zfsrep-"+name, "-n", e2eNamespace); err == nil {
		k.t.Fatalf("lease for %s exists:\n%s", name, out)
	} else if !strings.Contains(err.Error(), "NotFound") {
		k.t.Fatalf("check lease for %s absence: %v", name, err)
	}
}

func (k kubectlRunner) run(timeout time.Duration, args ...string) {
	if _, err := k.runOutput(timeout, args...); err != nil {
		k.t.Fatal(err)
	}
}

func (k kubectlRunner) runOutput(timeout time.Duration, args ...string) (string, error) {
	return k.runCommand(timeout, nil, args...)
}

func (k kubectlRunner) runInput(timeout time.Duration, input []byte, args ...string) (string, error) {
	return k.runCommand(timeout, input, args...)
}

func (k kubectlRunner) runCommand(timeout time.Duration, input []byte, args ...string) (string, error) {
	k.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fullArgs := append([]string{"--kubeconfig", k.kubeconfig, "--request-timeout=" + timeout.String()}, args...)
	cmd := exec.CommandContext(ctx, k.path, fullArgs...)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	out := output.String()
	if ctx.Err() != nil {
		return out, errors.Join(ctx.Err(), commandError(args, out))
	}
	if err != nil {
		return out, commandError(args, out)
	}
	return out, nil
}

func commandError(args []string, output string) error {
	return errors.New("kubectl " + strings.Join(args, " ") + " failed:\n" + output)
}

type replicationObject struct {
	Status replicationStatus `json:"status"`
}

type replicationStatus struct {
	Phase                      string `json:"phase"`
	ObservedRunID              string `json:"observedRunID"`
	LastAttemptedRunID         string `json:"lastAttemptedRunID"`
	LastSuccessfulRunID        string `json:"lastSuccessfulRunID"`
	LastSuccessfulSnapshot     string `json:"lastSuccessfulSnapshot"`
	LastSuccessfulSnapshotGUID string `json:"lastSuccessfulSnapshotGUID"`
	SenderJobName              string `json:"senderJobName"`
	ReceiverJobName            string `json:"receiverJobName"`
	ReceiverPodName            string `json:"receiverPodName"`
	ReceiverPodIP              string `json:"receiverPodIP"`
	TokenSecretName            string `json:"tokenSecretName"`
	StartedAt                  string `json:"startedAt"`
	CompletedAt                string `json:"completedAt"`
	LastError                  string `json:"lastError"`
}

type leaseObject struct {
	Metadata struct {
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	} `json:"items"`
}

type zfsSimEvent struct {
	Time    string `json:"time"`
	Node    string `json:"node"`
	Pod     string `json:"pod"`
	Role    string `json:"role"`
	RunID   string `json:"runID"`
	Action  string `json:"action"`
	Command string `json:"command"`
	Target  string `json:"target"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
	Detail  string `json:"detail"`
}

func parseZFSSimEvents(t *testing.T, podName, logs string) []zfsSimEvent {
	t.Helper()

	var events []zfsSimEvent
	for _, line := range strings.Split(logs, "\n") {
		raw, ok := strings.CutPrefix(strings.TrimSpace(line), "zfs-sim-event ")
		if !ok {
			continue
		}
		var event zfsSimEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			t.Fatalf("parse zfs simulator event from %s: %v\nline: %s", podName, err, line)
		}
		events = append(events, event)
	}
	return events
}

func assertSucceededStatus(t *testing.T, sc replicationCase, st replicationStatus, wantGUID string) {
	t.Helper()
	if st.Phase != "Succeeded" || st.ObservedRunID != sc.RunID || st.LastAttemptedRunID != sc.RunID || st.LastSuccessfulRunID != sc.RunID {
		t.Fatalf("unexpected success status for %s/%s: %#v", sc.Name, sc.RunID, st)
	}
	if st.LastSuccessfulSnapshot != sc.snapshotName() {
		t.Fatalf("lastSuccessfulSnapshot = %q, want %q", st.LastSuccessfulSnapshot, sc.snapshotName())
	}
	if st.LastSuccessfulSnapshotGUID != wantGUID {
		t.Fatalf("lastSuccessfulSnapshotGUID = %q, want %q", st.LastSuccessfulSnapshotGUID, wantGUID)
	}
	if st.LastError != "" {
		t.Fatalf("lastError = %q, want empty", st.LastError)
	}
	if st.SenderJobName == "" || st.ReceiverJobName == "" || st.TokenSecretName == "" {
		t.Fatalf("status object names missing: %#v", st)
	}
	if st.ReceiverPodName == "" || st.ReceiverPodIP == "" {
		t.Fatalf("receiver pod status missing: %#v", st)
	}
	if st.StartedAt == "" || st.CompletedAt == "" {
		t.Fatalf("status timestamps missing: %#v", st)
	}
}

func assertFailedStatus(t *testing.T, sc replicationCase, st replicationStatus, wantError string) {
	t.Helper()
	if st.Phase != "Failed" || st.ObservedRunID != sc.RunID || st.LastAttemptedRunID != sc.RunID {
		t.Fatalf("unexpected failed status for %s/%s: %#v", sc.Name, sc.RunID, st)
	}
	if !strings.Contains(st.LastError, wantError) {
		t.Fatalf("lastError = %q, want to contain %q", st.LastError, wantError)
	}
	if st.LastSuccessfulRunID != "" || st.LastSuccessfulSnapshot != "" || st.LastSuccessfulSnapshotGUID != "" {
		t.Fatalf("failure should not update successful status fields: %#v", st)
	}
	if st.StartedAt == "" || st.CompletedAt == "" {
		t.Fatalf("failure timestamps missing: %#v", st)
	}
}

func assertRunTopology(t *testing.T, sc replicationCase, events []zfsSimEvent) {
	t.Helper()
	for _, event := range events {
		if event.RunID != sc.RunID {
			t.Fatalf("event runID = %q, want %q: %#v", event.RunID, sc.RunID, event)
		}
		if event.Action == "unsupported" {
			t.Fatalf("unsupported zfs command reached simulator: %#v\nall events:\n%s", event, formatZFSEvents(events))
		}
		switch event.Role {
		case "sender":
			if event.Node != sc.SourceNode {
				t.Fatalf("sender event ran on node %q, want %q: %#v", event.Node, sc.SourceNode, event)
			}
		case "receiver":
			if event.Node != sc.TargetNode {
				t.Fatalf("receiver event ran on node %q, want %q: %#v", event.Node, sc.TargetNode, event)
			}
		default:
			t.Fatalf("event role = %q, want sender or receiver: %#v", event.Role, event)
		}
	}
}

func assertFullBootstrapZFSEvents(t *testing.T, sc replicationCase, events []zfsSimEvent) string {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	targetSnap := sc.targetSnapshot()

	sourceList := requireSingleEvent(t, events, "sender", "list-snapshot", sourceSnap)
	if sourceList.Detail != "missing" {
		t.Fatalf("initial source snapshot lookup detail = %q, want missing: %#v", sourceList.Detail, sourceList)
	}
	requireSingleEvent(t, events, "sender", "snapshot", sourceSnap)
	send := requireSingleEvent(t, events, "sender", "send", sourceSnap)
	if send.Command != "zfs send "+sourceSnap {
		t.Fatalf("send command = %q, want %q", send.Command, "zfs send "+sourceSnap)
	}
	if send.Detail != "base=" {
		t.Fatalf("send detail = %q, want base=: %#v", send.Detail, send)
	}
	assertPayloadRecorded(t, send)
	guid := requireSingleEvent(t, events, "sender", "get-guid", sourceSnap)
	if guid.SHA256 == "" {
		t.Fatalf("sender get-guid did not record a GUID: %#v", guid)
	}

	requireSingleEvent(t, events, "receiver", "get-mounted", sc.TargetDataset)
	requireSingleEvent(t, events, "receiver", "destroy", sc.TargetDataset)
	receive := requireSingleEvent(t, events, "receiver", "receive", targetSnap)
	if receive.Command != "zfs receive -u -s "+sc.TargetDataset {
		t.Fatalf("receive command = %q, want %q", receive.Command, "zfs receive -u -s "+sc.TargetDataset)
	}
	if receive.Detail != "args=-u -s" {
		t.Fatalf("receive detail = %q, want args=-u -s: %#v", receive.Detail, receive)
	}
	targetList := requireSingleEvent(t, events, "receiver", "list-snapshot", targetSnap)
	if targetList.Detail != "exists" {
		t.Fatalf("target snapshot lookup detail = %q, want exists: %#v", targetList.Detail, targetList)
	}
	assertMatchingPayload(t, send, receive)
	assertEventOrder(t, events, []eventMatch{
		{role: "sender", action: "list-snapshot", target: sourceSnap},
		{role: "sender", action: "snapshot", target: sourceSnap},
		{role: "sender", action: "send", target: sourceSnap},
		{role: "sender", action: "get-guid", target: sourceSnap},
	})
	assertEventOrder(t, events, []eventMatch{
		{role: "receiver", action: "get-mounted", target: sc.TargetDataset},
		{role: "receiver", action: "destroy", target: sc.TargetDataset},
		{role: "receiver", action: "receive", target: targetSnap},
		{role: "receiver", action: "list-snapshot", target: targetSnap},
	})
	return guid.SHA256
}

func assertIncrementalZFSEvents(t *testing.T, sc replicationCase, baseSnapshotName string, events []zfsSimEvent) string {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	baseSourceSnap := sc.SourceDataset + "@" + baseSnapshotName
	targetSnap := sc.targetSnapshot()

	sourceList := requireSingleEvent(t, events, "sender", "list-snapshot", sourceSnap)
	if sourceList.Detail != "missing" {
		t.Fatalf("current source snapshot lookup detail = %q, want missing: %#v", sourceList.Detail, sourceList)
	}
	requireSingleEvent(t, events, "sender", "snapshot", sourceSnap)
	baseList := requireSingleEvent(t, events, "sender", "list-snapshot", baseSourceSnap)
	if baseList.Detail != "exists" {
		t.Fatalf("base source snapshot lookup detail = %q, want exists: %#v", baseList.Detail, baseList)
	}
	send := requireSingleEvent(t, events, "sender", "send", sourceSnap)
	wantSend := "zfs send -i " + baseSourceSnap + " " + sourceSnap
	if send.Command != wantSend {
		t.Fatalf("incremental send command = %q, want %q", send.Command, wantSend)
	}
	if send.Detail != "base="+baseSourceSnap {
		t.Fatalf("incremental send detail = %q, want base=%s: %#v", send.Detail, baseSourceSnap, send)
	}
	assertPayloadRecorded(t, send)
	guid := requireSingleEvent(t, events, "sender", "get-guid", sourceSnap)
	if guid.SHA256 == "" {
		t.Fatalf("sender get-guid did not record a GUID: %#v", guid)
	}

	requireSingleEvent(t, events, "receiver", "get-mounted", sc.TargetDataset)
	requireNoEvent(t, events, "receiver", "destroy", sc.TargetDataset)
	receive := requireSingleEvent(t, events, "receiver", "receive", targetSnap)
	if receive.Command != "zfs receive -u -s "+sc.TargetDataset {
		t.Fatalf("receive command = %q, want %q", receive.Command, "zfs receive -u -s "+sc.TargetDataset)
	}
	if receive.Detail != "args=-u -s" {
		t.Fatalf("receive detail = %q, want args=-u -s: %#v", receive.Detail, receive)
	}
	targetList := requireSingleEvent(t, events, "receiver", "list-snapshot", targetSnap)
	if targetList.Detail != "exists" {
		t.Fatalf("target snapshot lookup detail = %q, want exists: %#v", targetList.Detail, targetList)
	}
	assertMatchingPayload(t, send, receive)
	assertEventOrder(t, events, []eventMatch{
		{role: "sender", action: "list-snapshot", target: sourceSnap},
		{role: "sender", action: "snapshot", target: sourceSnap},
		{role: "sender", action: "list-snapshot", target: baseSourceSnap},
		{role: "sender", action: "send", target: sourceSnap},
		{role: "sender", action: "get-guid", target: sourceSnap},
	})
	assertEventOrder(t, events, []eventMatch{
		{role: "receiver", action: "get-mounted", target: sc.TargetDataset},
		{role: "receiver", action: "receive", target: targetSnap},
		{role: "receiver", action: "list-snapshot", target: targetSnap},
	})
	return guid.SHA256
}

func assertFailIfNoBaseEvents(t *testing.T, sc replicationCase, events []zfsSimEvent) {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	sourceList := requireSingleEvent(t, events, "sender", "list-snapshot", sourceSnap)
	if sourceList.Detail != "missing" {
		t.Fatalf("source snapshot lookup detail = %q, want missing: %#v", sourceList.Detail, sourceList)
	}
	requireSingleEvent(t, events, "sender", "snapshot", sourceSnap)
	requireNoEvent(t, events, "sender", "send", "")
	requireNoEvent(t, events, "sender", "get-guid", "")
	requireNoEvent(t, events, "receiver", "destroy", "")
	requireNoEvent(t, events, "receiver", "receive", "")
}

func assertSendFailureEvents(t *testing.T, sc replicationCase, events []zfsSimEvent) {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	requireSingleEvent(t, events, "sender", "snapshot", sourceSnap)
	send := requireSingleEvent(t, events, "sender", "send", sourceSnap)
	if send.Detail != "forced failure" {
		t.Fatalf("send detail = %q, want forced failure: %#v", send.Detail, send)
	}
	requireNoEvent(t, events, "sender", "get-guid", "")
	for _, receive := range matchingEvents(events, "receiver", "receive", "") {
		if receive.Detail != "invalid stream" {
			t.Fatalf("receiver receive detail = %q, want invalid stream after forced send failure: %#v\nall events:\n%s", receive.Detail, receive, formatZFSEvents(events))
		}
	}
}

func assertReceiveFailureEvents(t *testing.T, sc replicationCase, events []zfsSimEvent) {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	targetSnap := sc.targetSnapshot()
	send := requireSingleEvent(t, events, "sender", "send", sourceSnap)
	assertPayloadRecorded(t, send)
	receive := requireSingleEvent(t, events, "receiver", "receive", targetSnap)
	if receive.Detail != "forced failure" {
		t.Fatalf("receive detail = %q, want forced failure: %#v", receive.Detail, receive)
	}
	requireNoEvent(t, events, "sender", "get-guid", "")
	requireNoEvent(t, events, "receiver", "list-snapshot", targetSnap)
}

func assertMountedTargetEvents(t *testing.T, sc replicationCase, events []zfsSimEvent) {
	t.Helper()
	sourceSnap := sc.sourceSnapshot()
	send := requireSingleEvent(t, events, "sender", "send", sourceSnap)
	assertPayloadRecorded(t, send)
	requireSingleEvent(t, events, "receiver", "get-mounted", sc.TargetDataset)
	requireNoEvent(t, events, "receiver", "destroy", "")
	requireNoEvent(t, events, "receiver", "receive", "")
	requireNoEvent(t, events, "sender", "get-guid", "")
}

type eventMatch struct {
	role   string
	action string
	target string
}

func requireSingleEvent(t *testing.T, events []zfsSimEvent, role, action, target string) zfsSimEvent {
	t.Helper()
	matches := matchingEvents(events, role, action, target)
	if len(matches) != 1 {
		t.Fatalf("found %d events for role=%s action=%s target=%s, want exactly 1\nall events:\n%s", len(matches), role, action, target, formatZFSEvents(events))
	}
	return matches[0]
}

func requireNoEvent(t *testing.T, events []zfsSimEvent, role, action, target string) {
	t.Helper()
	if matches := matchingEvents(events, role, action, target); len(matches) != 0 {
		t.Fatalf("found %d unexpected events for role=%s action=%s target=%s\nmatches:\n%s\nall events:\n%s", len(matches), role, action, target, formatZFSEvents(matches), formatZFSEvents(events))
	}
}

func matchingEvents(events []zfsSimEvent, role, action, target string) []zfsSimEvent {
	var matches []zfsSimEvent
	for _, event := range events {
		if event.Role != role || event.Action != action {
			continue
		}
		if target != "" && event.Target != target {
			continue
		}
		matches = append(matches, event)
	}
	return matches
}

func assertPayloadRecorded(t *testing.T, event zfsSimEvent) {
	t.Helper()
	if event.Bytes <= 0 || event.SHA256 == "" {
		t.Fatalf("%s did not record payload bytes and hash: %#v", event.Action, event)
	}
}

func assertMatchingPayload(t *testing.T, send, receive zfsSimEvent) {
	t.Helper()
	if receive.Bytes != send.Bytes || receive.SHA256 != send.SHA256 {
		t.Fatalf("receive payload = %d/%s, want send payload %d/%s", receive.Bytes, receive.SHA256, send.Bytes, send.SHA256)
	}
}

func assertEventOrder(t *testing.T, events []zfsSimEvent, want []eventMatch) {
	t.Helper()

	next := 0
	for _, event := range events {
		if next >= len(want) {
			return
		}
		match := want[next]
		if event.Role == match.role && event.Action == match.action && event.Target == match.target {
			next++
		}
	}
	if next != len(want) {
		t.Fatalf("events did not appear in expected order, matched %d of %d\nwant: %#v\nall events:\n%s", next, len(want), want, formatZFSEvents(events))
	}
}

func formatZFSEvents(events []zfsSimEvent) string {
	var b strings.Builder
	for _, event := range events {
		_, _ = fmt.Fprintf(&b, "%s %s %s %s run=%s %q bytes=%d sha=%s detail=%q\n", event.Role, event.Node, event.Action, event.Target, event.RunID, event.Command, event.Bytes, event.SHA256, event.Detail)
	}
	return b.String()
}

func uniqueSuffix() string {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	if len(suffix) > 8 {
		return suffix[len(suffix)-8:]
	}
	return suffix
}
