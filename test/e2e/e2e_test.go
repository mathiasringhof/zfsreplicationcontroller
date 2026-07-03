package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
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

	e2eLabelPrefix = "zfsreplication.example.com"
)

func TestE2EFullAndIncrementalReplication(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	name := "e2e-repl-" + suffix
	first := replicationCase{
		Name:          name + "-1",
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-" + suffix,
		TargetDataset: pool + "/dst-" + suffix,
		ForceDelete:   true,
	}
	k.cleanupReplicationOnExit(first.Name)
	k.cleanupReplication(first.Name)
	k.cleanupRealZFSDatasetOnExit(first.SourceNode, "zfs-cln-src-"+suffix, first.SourceDataset)
	k.cleanupRealZFSDatasetOnExit(first.TargetNode, "zfs-cln-dst-"+suffix, first.TargetDataset)

	k.runRealZFS(first.SourceNode, "zfs-src-"+suffix, realZFSSetupSourceScript(pool, first.SourceDataset, "first-"+suffix))
	k.runRealZFS(first.TargetNode, "zfs-dst-"+suffix, realZFSSetupTargetScript(pool, first.TargetDataset))

	k.applyReplication(first)
	assertSucceededStatus(t, first, k.waitForSuccess(first, 4*time.Minute))
	k.assertRealZFSMarker(first.TargetNode, "zfs-dst-p1-"+suffix, pool, first.TargetDataset, "first-"+suffix)
	k.assertRunEphemeralCleanup(first.Name)

	second := first
	second.Name = name + "-2"
	second.ForceDelete = false
	k.cleanupReplicationOnExit(second.Name)
	k.cleanupReplication(second.Name)
	k.runRealZFS(second.SourceNode, "zfs-mut-"+suffix, realZFSMutateSourceScript(pool, second.SourceDataset, "second-"+suffix))

	k.applyReplication(second)
	assertSucceededStatus(t, second, k.waitForSuccess(second, 4*time.Minute))
	k.assertRealZFSMarker(second.TargetNode, "zfs-dst-p2-"+suffix, pool, second.TargetDataset, "second-"+suffix)
	k.assertRunEphemeralCleanup(second.Name)
}

func TestE2EExternalSnapshotsWithoutCommonBaseFails(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	sc := replicationCase{
		Name:          "e2e-no-base-" + suffix,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-no-base-" + suffix,
		TargetDataset: pool + "/dst-no-base-" + suffix,
		SnapshotName:  "snap-" + suffix,
		NoSyncSnap:    true,
		IncludeSnaps:  []string{"^snap-" + suffix + "$"},
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)
	k.cleanupRealZFSDatasetOnExit(sc.SourceNode, "zfs-cln-no-base-src-"+suffix, sc.SourceDataset)
	k.cleanupRealZFSDatasetOnExit(sc.TargetNode, "zfs-cln-no-base-dst-"+suffix, sc.TargetDataset)

	k.runRealZFS(sc.SourceNode, "zfs-src-no-base-"+suffix, realZFSSetupSourceScript(pool, sc.SourceDataset, "no-base-"+suffix))
	k.runRealZFS(sc.SourceNode, "zfs-snap-no-base-"+suffix, "zfs snapshot "+sc.sourceSnapshot())
	k.runRealZFS(sc.TargetNode, "zfs-dst-no-base-"+suffix, realZFSSetupTargetScript(pool, sc.TargetDataset))

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedAfterDataMoverSetupStatus(t, sc, status, "")
	k.assertRealZFSSnapshotExists(sc.SourceNode, "zfs-src-snap-no-base-"+suffix, sc.sourceSnapshot())
	k.assertRunEphemeralCleanup(sc.Name)
}

func TestE2ESameNodeSameDatasetRejectedBeforeJobs(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	sc := replicationCase{
		Name:          "e2e-same-ds-" + suffix,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eSourceNode,
		SourceDataset: pool + "/same-" + suffix,
		TargetDataset: pool + "/same-" + suffix,
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 2*time.Minute)
	assertFailedStatus(t, sc, status, "source and target must not reference the same dataset on the same node")
	k.assertNoJobsOrSecrets(sc.Name)
}

func TestE2ESyncoidFailure(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	sc := replicationCase{
		Name:          "e2e-sync-fail-" + suffix,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-sync-fail-" + suffix,
		TargetDataset: "missingpool/dst-sync-fail-" + suffix,
		ForceDelete:   true,
	}
	k.cleanupReplicationOnExit(sc.Name)
	k.cleanupReplication(sc.Name)
	k.cleanupRealZFSDatasetOnExit(sc.SourceNode, "zfs-cln-sync-fail-src-"+suffix, sc.SourceDataset)

	k.runRealZFS(sc.SourceNode, "zfs-src-sync-fail-"+suffix, realZFSSetupSourceScript(pool, sc.SourceDataset, "sync-fail-"+suffix))

	k.applyReplication(sc)
	status := k.waitForFailed(sc, 4*time.Minute)
	assertFailedAfterDataMoverSetupStatus(t, sc, status, "CRITICAL ERROR")
	if !strings.Contains(status.LastError, sc.TargetDataset) {
		t.Fatalf("lastError = %q, want to mention target dataset %q", status.LastError, sc.TargetDataset)
	}
	k.assertRunEphemeralCleanup(sc.Name)
}

func TestE2EOrphanReceiverPodTerminalCleanup(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	name := "e2e-orphan-receiver-" + suffix
	podName := name + "-receiver"
	pool := realZFSPool()
	sc := replicationCase{
		Name:          name,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/orphan-src-" + suffix,
		TargetDataset: pool + "/orphan-dst-" + suffix,
	}
	k.cleanupReplicationOnExit(name)
	k.cleanupReplication(name)

	k.scaleController(0)
	t.Cleanup(func() { k.scaleController(1) })

	k.applyReplication(sc)
	k.run(30*time.Second, "patch", "zfsreplicationrun", name, "-n", e2eNamespace, "--subresource=status", "--type=merge", "-p", `{"status":{"phase":"Succeeded"}}`)
	if _, err := k.runInput(30*time.Second, orphanReceiverPodManifest(t, name, podName), "apply", "-f", "-"); err != nil {
		t.Fatal(err)
	}
	k.run(60*time.Second, "wait", "pod/"+podName, "-n", e2eNamespace, "--for=condition=Ready", "--timeout=60s")
	k.logSelectedRunObjects(name, "before orphan receiver cleanup")

	k.scaleController(1)
	k.run(30*time.Second, "annotate", "zfsreplicationrun", name, "-n", e2eNamespace, "zfsreplication.example.com/e2e-trigger="+suffix, "--overwrite")
	k.assertRunEphemeralCleanup(name)
	k.logSelectedRunObjects(name, "after orphan receiver cleanup")
}

type replicationCase struct {
	Name          string
	SourceNode    string
	TargetNode    string
	SourceDataset string
	TargetDataset string
	SnapshotName  string
	NoSyncSnap    bool
	ForceDelete   bool
	IncludeSnaps  []string
	ExcludeSnaps  []string
	Annotations   map[string]string
}

func (c replicationCase) snapshotName() string {
	return c.SnapshotName
}

func (c replicationCase) sourceSnapshot() string {
	return c.SourceDataset + "@" + c.snapshotName()
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
		"kind":       "ZFSReplicationRun",
		"metadata":   metadata,
		"spec": map[string]any{
			"source": map[string]any{
				"nodeName": c.SourceNode,
				"dataset":  c.SourceDataset,
			},
			"target": map[string]any{
				"nodeName": c.TargetNode,
				"dataset":  c.TargetDataset,
			},
			"syncoid": map[string]any{
				"noSyncSnap":       c.NoSyncSnap,
				"noRollback":       true,
				"forceDelete":      c.ForceDelete,
				"compress":         "none",
				"receiveUnmounted": true,
				"receiveResumable": true,
				"includeSnaps":     c.IncludeSnaps,
				"excludeSnaps":     c.ExcludeSnaps,
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
		runFlag := flag.Lookup("test.run")
		if runFlag == nil || runFlag.Value.String() == "" {
			t.Skip("KUBECONFIG is required for VM e2e tests; run test/e2e/run.sh, then go test ./test/e2e -run TestE2E")
		}
		kubeconfig = ".artifacts/kubeconfig"
	}
	if _, err := os.Stat(kubeconfig); err != nil {
		t.Skipf("KUBECONFIG is not usable: set KUBECONFIG or run test/e2e/run.sh to create .artifacts/kubeconfig: %v", err)
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
	k.run(75*time.Second, "delete", "zfsreplicationrun", name, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "jobs,pods,secrets", "-n", e2eNamespace, "-l", e2eLabelPrefix+"/run="+name, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
}

func (k kubectlRunner) cleanupReplicationOnExit(name string) {
	k.t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		k.t.Logf("leaving e2e resources for %s because E2E_KEEP_RESOURCES=1", name)
		return
	}
	k.t.Cleanup(func() { k.cleanupReplication(name) })
}

func (k kubectlRunner) scaleController(replicas int) {
	k.t.Helper()
	k.run(30*time.Second, "scale", "deployment/zfsreplication-controller", "-n", "zfsreplication-system", "--replicas="+strconv.Itoa(replicas))
	k.run(180*time.Second, "rollout", "status", "deployment/zfsreplication-controller", "-n", "zfsreplication-system", "--timeout=180s")
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
		return st.Phase == "Succeeded"
	}, "Succeeded")
}

func (k kubectlRunner) waitForFailed(sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForStatus(sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Failed"
	}, "Failed")
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
	out, err := k.runOutput(20*time.Second, "get", "zfsreplicationrun", name, "-n", e2eNamespace, "-o", "json")
	if err != nil {
		return replicationStatus{}, err
	}
	var obj replicationObject
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return replicationStatus{}, fmt.Errorf("parse zfsreplicationrun status: %w\n%s", err, out)
	}
	return obj.Status, nil
}

func (k kubectlRunner) collectDiagnostics(name string) {
	for _, args := range [][]string{
		{"get", "zfsreplicationrun", name, "-n", e2eNamespace, "-o", "yaml"},
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

func (k kubectlRunner) podsForReplication(name, runID string) (podList, error) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name
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

func (k kubectlRunner) assertRunEphemeralCleanup(name string) {
	k.t.Helper()
	k.assertNoLabelledResourcesEventually(name, "receiver jobs", "jobs", e2eLabelPrefix+"/role=receiver")
	k.assertNoLabelledResourcesEventually(name, "receiver pods", "pods", e2eLabelPrefix+"/role=receiver")
	k.assertNoLabelledResourcesEventually(name, "run secrets", "secrets", "")
}

func (k kubectlRunner) logSelectedRunObjects(name, stage string) {
	k.t.Helper()
	for _, selector := range []string{
		e2eLabelPrefix + "/run=" + name,
		e2eLabelPrefix + "/run=" + name + "," + e2eLabelPrefix + "/role=receiver",
	} {
		out, err := k.runOutput(20*time.Second, "get", "pods,jobs,secrets", "-n", e2eNamespace, "-l", selector, "-o", "wide", "--show-labels")
		if err != nil {
			k.t.Logf("%s selector=%q failed: %v", stage, selector, err)
			continue
		}
		k.t.Logf("%s selector=%q\n%s", stage, selector, out)
	}
}

func (k kubectlRunner) assertNoLabelledResourcesEventually(name, description, resource, extraSelector string) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name
	if extraSelector != "" {
		selector += "," + extraSelector
	}
	deadline := time.Now().Add(60 * time.Second)
	var lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := k.runOutput(20*time.Second, "get", resource, "-n", e2eNamespace, "-l", selector, "-o", "name")
		if err != nil {
			if strings.Contains(err.Error(), "No resources found") {
				return
			}
			lastErr = err
		} else {
			lastOut = out
			if strings.TrimSpace(out) == "" {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	k.collectDiagnostics(name)
	k.t.Fatalf("%s still exist for %s after terminal cleanup; selector=%q last output=%q last error=%v", description, name, selector, lastOut, lastErr)
}

func (k kubectlRunner) assertNoJobsOrSecrets(name string) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "jobs,secrets", "-n", e2eNamespace, "-l", e2eLabelPrefix+"/run="+name, "-o", "name")
	if err != nil && !strings.Contains(err.Error(), "No resources found") {
		k.t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		k.t.Fatalf("jobs/secrets exist for %s:\n%s", name, out)
	}
}

func (k kubectlRunner) cleanupRealZFSDatasetOnExit(node, jobName, dataset string) {
	k.t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		k.t.Logf("leaving real ZFS dataset %s on %s because E2E_KEEP_RESOURCES=1", dataset, node)
		return
	}
	k.t.Cleanup(func() {
		if logs, err := k.tryRealZFS(node, jobName, "zfs destroy -r "+dataset+" >/dev/null 2>&1 || true"); err != nil {
			k.t.Logf("cleanup real ZFS dataset %s on %s failed: %v\n%s", dataset, node, err, logs)
		}
	})
}

func (k kubectlRunner) assertRealZFSMarker(node, jobName, pool, dataset, want string) {
	k.t.Helper()
	logs := k.runRealZFS(node, jobName, realZFSReadMarkerScript(pool, dataset))
	got := strings.TrimSpace(logs)
	if got != want {
		k.t.Fatalf("%s marker on %s = %q, want %q", dataset, node, got, want)
	}
}

func (k kubectlRunner) assertRealZFSSnapshotExists(node, jobName, snapshot string) {
	k.t.Helper()
	k.runRealZFS(node, jobName, "zfs list -H -t snapshot "+snapshot+" >/dev/null")
}

func (k kubectlRunner) runRealZFS(node, name, script string) string {
	k.t.Helper()
	logs, err := k.tryRealZFS(node, name, script)
	if err != nil {
		k.t.Fatalf("real ZFS job %s on %s failed: %v\n%s", name, node, err, logs)
	}
	return logs
}

func (k kubectlRunner) tryRealZFS(node, name, script string) (string, error) {
	k.t.Helper()
	k.deleteJob(name)
	defer k.deleteJob(name)

	manifest := realZFSJobManifest(k.t, node, name, script)
	if _, err := k.runInput(30*time.Second, manifest, "apply", "-f", "-"); err != nil {
		return "", err
	}
	if err := k.waitForJobSuccess(name, 2*time.Minute); err != nil {
		logs, logErr := k.runOutput(20*time.Second, "logs", "-n", e2eNamespace, "job/"+name, "--all-containers=true")
		if logErr != nil {
			return "", errors.Join(err, fmt.Errorf("collect job logs: %w", logErr))
		}
		return logs, err
	}
	logs, err := k.runOutput(20*time.Second, "logs", "-n", e2eNamespace, "job/"+name, "--all-containers=true")
	if err != nil {
		return "", err
	}
	return logs, nil
}

func (k kubectlRunner) deleteJob(name string) {
	k.t.Helper()
	if _, err := k.runOutput(45*time.Second, "delete", "job", name, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=30s"); err != nil {
		k.t.Logf("delete job %s: %v", name, err)
	}
}

func (k kubectlRunner) waitForJobSuccess(name string, timeout time.Duration) error {
	k.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := k.getJob(name)
		if err == nil {
			if job.Status.Succeeded > 0 {
				return nil
			}
			if job.Status.Failed > 0 {
				return fmt.Errorf("job failed: %s", jobFailureMessage(job))
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timed out waiting for job %s to complete", name)
}

func (k kubectlRunner) getJob(name string) (jobObject, error) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "job", name, "-n", e2eNamespace, "-o", "json")
	if err != nil {
		return jobObject{}, err
	}
	var job jobObject
	if err := json.Unmarshal([]byte(out), &job); err != nil {
		return jobObject{}, fmt.Errorf("parse job: %w\n%s", err, out)
	}
	return job, nil
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
	Phase           string `json:"phase"`
	SenderJobName   string `json:"senderJobName"`
	ReceiverJobName string `json:"receiverJobName"`
	ReceiverPodName string `json:"receiverPodName"`
	ReceiverPodIP   string `json:"receiverPodIP"`
	SSHSecretName   string `json:"sshSecretName"`
	StartedAt       string `json:"startedAt"`
	CompletedAt     string `json:"completedAt"`
	LastError       string `json:"lastError"`
}

type jobObject struct {
	Status struct {
		Succeeded  int            `json:"succeeded"`
		Failed     int            `json:"failed"`
		Conditions []jobCondition `json:"conditions"`
	} `json:"status"`
}

type jobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	} `json:"items"`
}

func assertSucceededStatus(t *testing.T, sc replicationCase, st replicationStatus) {
	t.Helper()
	if st.Phase != "Succeeded" {
		t.Fatalf("unexpected success status for %s: %#v", sc.Name, st)
	}
	if st.LastError != "" {
		t.Fatalf("lastError = %q, want empty", st.LastError)
	}
	if st.SenderJobName == "" {
		t.Fatalf("status object names missing: %#v", st)
	}
	if st.ReceiverJobName == "" || st.ReceiverPodName == "" || st.ReceiverPodIP == "" || st.SSHSecretName == "" {
		t.Fatalf("receiver/ssh status names missing: %#v", st)
	}
	if st.StartedAt == "" || st.CompletedAt == "" {
		t.Fatalf("status timestamps missing: %#v", st)
	}
}

func assertFailedStatus(t *testing.T, sc replicationCase, st replicationStatus, wantError string) {
	t.Helper()
	if st.Phase != "Failed" {
		t.Fatalf("unexpected failed status for %s: %#v", sc.Name, st)
	}
	if wantError != "" && !strings.Contains(st.LastError, wantError) {
		t.Fatalf("lastError = %q, want to contain %q", st.LastError, wantError)
	}
	if st.LastError == "" {
		t.Fatalf("lastError is empty")
	}
	if st.StartedAt == "" || st.CompletedAt == "" {
		t.Fatalf("failure timestamps missing: %#v", st)
	}
}

func assertFailedAfterDataMoverSetupStatus(t *testing.T, sc replicationCase, st replicationStatus, wantError string) {
	t.Helper()
	assertFailedStatus(t, sc, st, wantError)
	if st.ReceiverJobName == "" || st.ReceiverPodName == "" || st.ReceiverPodIP == "" || st.SSHSecretName == "" {
		t.Fatalf("receiver/ssh status names missing after datamover setup for %s: %#v", sc.Name, st)
	}
}

func realZFSJobManifest(t *testing.T, node, name, script string) []byte {
	t.Helper()
	doc := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      name,
			"namespace": e2eNamespace,
		},
		"spec": map[string]any{
			"backoffLimit":            0,
			"ttlSecondsAfterFinished": 300,
			"template": map[string]any{
				"spec": map[string]any{
					"restartPolicy":                "Never",
					"automountServiceAccountToken": false,
					"nodeName":                     node,
					"containers": []map[string]any{
						{
							"name":            "zfs",
							"image":           e2eImageTag(),
							"imagePullPolicy": "IfNotPresent",
							"command":         []string{"/bin/sh", "-ec", script},
							"securityContext": map[string]any{"privileged": true},
							"volumeMounts": []map[string]any{
								{"name": "dev-zfs", "mountPath": "/dev/zfs"},
							},
						},
					},
					"volumes": []map[string]any{
						{"name": "dev-zfs", "hostPath": map[string]any{"path": "/dev/zfs"}},
					},
				},
			},
		},
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func orphanReceiverPodManifest(t *testing.T, runName, podName string) []byte {
	t.Helper()
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": e2eNamespace,
			"labels": map[string]string{
				e2eLabelPrefix + "/run":  runName,
				e2eLabelPrefix + "/role": "receiver",
			},
		},
		"spec": map[string]any{
			"restartPolicy":                "Never",
			"automountServiceAccountToken": false,
			"nodeName":                     e2eTargetNode,
			"containers": []map[string]any{
				{
					"name":            "receiver",
					"image":           e2eImageTag(),
					"imagePullPolicy": "IfNotPresent",
					"command":         []string{"/bin/sh", "-c", "sleep 600"},
					"securityContext": map[string]any{"privileged": true},
				},
			},
		},
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func realZFSSetupSourceScript(pool, dataset, marker string) string {
	mountpoint := realZFSMountpoint(dataset)
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"zfs destroy -r " + shellQuote(dataset) + " >/dev/null 2>&1 || true",
		"rm -rf " + shellQuote(mountpoint),
		"mkdir -p " + shellQuote(mountpoint),
		"zfs create -o mountpoint=" + shellQuote(mountpoint) + " " + shellQuote(dataset),
		"printf '%s\\n' " + shellQuote(marker) + " > " + shellQuote(realZFSMarkerPath(dataset)),
		"sync",
	}, "\n")
}

func realZFSSetupTargetScript(pool, dataset string) string {
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"zfs destroy -r " + shellQuote(dataset) + " >/dev/null 2>&1 || true",
		"zfs create -o mountpoint=none " + shellQuote(dataset),
	}, "\n")
}

func realZFSMutateSourceScript(pool, dataset, marker string) string {
	mountpoint := realZFSMountpoint(dataset)
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"mkdir -p " + shellQuote(mountpoint),
		"zfs set mountpoint=" + shellQuote(mountpoint) + " " + shellQuote(dataset),
		"zfs mount " + shellQuote(dataset) + " >/dev/null 2>&1 || true",
		"printf '%s\\n' " + shellQuote(marker) + " > " + shellQuote(realZFSMarkerPath(dataset)),
		"sync",
	}, "\n")
}

func realZFSReadMarkerScript(pool, dataset string) string {
	clone := dataset + "-read-" + uniqueSuffix()
	mountpoint := realZFSMountpoint(clone)
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"snapshot=$(zfs list -H -t snapshot -o name -s creation -r " + shellQuote(dataset) + " | tail -n 1)",
		"[ -n \"$snapshot\" ]",
		"clone=" + shellQuote(clone),
		"mountpoint=" + shellQuote(mountpoint),
		"trap 'zfs unmount \"$clone\" >/dev/null 2>&1 || true; zfs destroy -rf \"$clone\" >/dev/null 2>&1 || true; rm -rf \"$mountpoint\"' EXIT",
		"zfs destroy -rf \"$clone\" >/dev/null 2>&1 || true",
		"rm -rf \"$mountpoint\"",
		"mkdir -p \"$mountpoint\"",
		"zfs clone -o mountpoint=\"$mountpoint\" \"$snapshot\" \"$clone\"",
		"zfs mount \"$clone\" >/dev/null 2>&1 || true",
		"cat \"$mountpoint/marker.txt\"",
	}, "\n")
}

func realZFSPreamble(pool string) string {
	return strings.Join([]string{
		"command -v zfs >/dev/null",
		"command -v zpool >/dev/null",
		"zpool list -H " + shellQuote(pool) + " >/dev/null",
	}, "\n")
}

func realZFSMountpoint(dataset string) string {
	replacer := strings.NewReplacer("/", "-", "@", "-")
	return "/var/lib/zfsreplicationcontroller-e2e/" + replacer.Replace(dataset)
}

func realZFSMarkerPath(dataset string) string {
	return realZFSMountpoint(dataset) + "/marker.txt"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func jobFailureMessage(job jobObject) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == "Failed" && condition.Status == "True" {
			if condition.Message != "" {
				return condition.Message
			}
			if condition.Reason != "" {
				return condition.Reason
			}
		}
	}
	return "unknown failure"
}

func realZFSPool() string {
	if pool := os.Getenv("E2E_REAL_ZFS_POOL"); pool != "" {
		return pool
	}
	return "tank"
}

func e2eImageTag() string {
	if image := os.Getenv("E2E_IMAGE_TAG"); image != "" {
		return image
	}
	return "zfsreplicationcontroller:e2e"
}

func uniqueSuffix() string {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	if len(suffix) > 8 {
		return suffix[len(suffix)-8:]
	}
	return suffix
}
