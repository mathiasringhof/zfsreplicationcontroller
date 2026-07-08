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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	e2eNamespace      = "storage"
	e2eSmokeNamespace = "zfsreplication-smoke"

	e2eControllerNamespace      = "zfsreplication-system"
	e2eControllerDeploymentName = "zfsreplication-controller"
	e2eControllerServiceAccount = "system:serviceaccount:zfsreplication-system:zfsreplication-controller"

	e2eSourceNode = "worker-a"
	e2eTargetNode = "worker-b"

	e2eLabelPrefix = "zfsreplication.ringhof.io"

	e2eReceiveTaskPhaseCompleted = "Completed"
	e2eReceiveTaskPhaseFailed    = "Failed"
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
	firstStatus := k.waitForSuccess(first, 4*time.Minute)
	assertSucceededStatus(t, first, firstStatus)
	k.assertReceiveTaskTerminal(first, firstStatus, e2eReceiveTaskPhaseCompleted)
	k.assertRealZFSMarker(first.TargetNode, "zfs-dst-p1-"+suffix, pool, first.TargetDataset, "first-"+suffix)
	k.assertRunEphemeralCleanup(first.Name)

	second := first
	second.Name = name + "-2"
	second.ForceDelete = false
	k.cleanupReplicationOnExit(second.Name)
	k.cleanupReplication(second.Name)
	k.runRealZFS(second.SourceNode, "zfs-mut-"+suffix, realZFSMutateSourceScript(pool, second.SourceDataset, "second-"+suffix))

	k.applyReplication(second)
	secondStatus := k.waitForSuccess(second, 4*time.Minute)
	assertSucceededStatus(t, second, secondStatus)
	k.assertReceiveTaskTerminal(second, secondStatus, e2eReceiveTaskPhaseCompleted)
	k.assertRealZFSMarker(second.TargetNode, "zfs-dst-p2-"+suffix, pool, second.TargetDataset, "second-"+suffix)
	k.assertRealZFSSyncoidSnapshots(second.SourceNode, "zfs-src-sync-snaps-"+suffix, pool, second.SourceDataset, 1)
	k.assertRealZFSSyncoidSnapshots(second.TargetNode, "zfs-dst-sync-snaps-"+suffix, pool, second.TargetDataset, 1)
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
	k.assertReceiveTaskTerminal(sc, status, e2eReceiveTaskPhaseFailed)
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
	k.assertReceiveTaskTerminal(sc, status, e2eReceiveTaskPhaseFailed)
	if !strings.Contains(status.LastError, sc.TargetDataset) {
		t.Fatalf("lastError = %q, want to mention target dataset %q", status.LastError, sc.TargetDataset)
	}
	k.assertRunEphemeralCleanup(sc.Name)
}

func TestE2EDestinationContentionWaits(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	blocker := replicationCase{
		Name:          "e2e-lock-a-" + suffix,
		SourceNode:    "missing-source-" + suffix,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-lock-a-" + suffix,
		TargetDataset: pool + "/dst-lock-" + suffix,
	}
	blocked := replicationCase{
		Name:          "e2e-lock-b-" + suffix,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-lock-b-" + suffix,
		TargetDataset: blocker.TargetDataset,
	}
	k.cleanupReplicationOnExit(blocker.Name)
	k.cleanupReplicationOnExit(blocked.Name)
	k.cleanupReplication(blocker.Name)
	k.cleanupReplication(blocked.Name)

	k.applyReplication(blocker)
	k.patchRunPhase(e2eNamespace, blocker.Name, "Running")
	k.applyReplication(blocked)

	k.waitForDestinationLock(blocked, blocker.Name, blocker.TargetDataset, 90*time.Second)
	k.assertNoSenderJob(blocked.Name)

	k.cleanupReplication(blocker.Name)
	k.waitForRunPastPending(blocked, 2*time.Minute)
}

func TestE2ETerminalCleanupCurrentStateMatrix(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	cases := []struct {
		nameSuffix    string
		phase         string
		objects       []terminalCleanupObject
		wantTaskPhase string
	}{
		{nameSuffix: "task-success", phase: "Succeeded", objects: []terminalCleanupObject{terminalCleanupReceiveTask, terminalCleanupSSHSecret}, wantTaskPhase: e2eReceiveTaskPhaseCompleted},
		{nameSuffix: "task-failed", phase: "Failed", objects: []terminalCleanupObject{terminalCleanupReceiveTask, terminalCleanupSSHSecret}, wantTaskPhase: e2eReceiveTaskPhaseFailed},
		{nameSuffix: "secret", phase: "Succeeded", objects: []terminalCleanupObject{terminalCleanupSSHSecret}},
	}

	k.scaleController(0)
	t.Cleanup(func() { k.scaleController(1) })

	for _, tc := range cases {
		sc := replicationCase{
			Name:          "e2e-clean-" + suffix + "-" + tc.nameSuffix,
			SourceNode:    e2eSourceNode,
			TargetNode:    e2eTargetNode,
			SourceDataset: pool + "/src-clean-" + suffix + "-" + tc.nameSuffix,
			TargetDataset: pool + "/dst-clean-" + suffix + "-" + tc.nameSuffix,
		}
		k.cleanupReplicationOnExit(sc.Name)
		k.cleanupReplication(sc.Name)
		k.applyReplication(sc)
		k.patchRunPhase(e2eNamespace, sc.Name, tc.phase)
		k.seedTerminalCleanupObjects(e2eNamespace, sc, tc.objects)
		k.logSelectedRunObjects(e2eNamespace, sc.Name, "before terminal cleanup")
	}

	k.scaleController(1)
	for _, tc := range cases {
		name := "e2e-clean-" + suffix + "-" + tc.nameSuffix
		k.run(30*time.Second, "annotate", "zfsreplicationrun", name, "-n", e2eNamespace, "e2e-reconcile="+uniqueSuffix(), "--overwrite")
		k.assertRunEphemeralCleanup(name)
		if tc.wantTaskPhase != "" {
			k.assertReceiveTaskPhase(e2eNamespace, runObjectName(name, "receiver"), tc.wantTaskPhase)
		}
		k.logSelectedRunObjects(e2eNamespace, name, "after terminal cleanup")
	}
}

func TestE2EControllerServiceAccountRBAC(t *testing.T) {
	k := newKubectlRunner(t)
	for _, tt := range []struct {
		verb        string
		resource    string
		subresource string
	}{
		{verb: "get", resource: "zfsreplicationruns.zfsreplication.ringhof.io"},
		{verb: "list", resource: "zfsreplicationruns.zfsreplication.ringhof.io"},
		{verb: "watch", resource: "zfsreplicationruns.zfsreplication.ringhof.io"},
		{verb: "create", resource: "zfsreplicationruns.zfsreplication.ringhof.io"},
		{verb: "get", resource: "zfsreplicationruns.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "update", resource: "zfsreplicationruns.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "patch", resource: "zfsreplicationruns.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "get", resource: "zfsreplicationschedules.zfsreplication.ringhof.io"},
		{verb: "list", resource: "zfsreplicationschedules.zfsreplication.ringhof.io"},
		{verb: "watch", resource: "zfsreplicationschedules.zfsreplication.ringhof.io"},
		{verb: "get", resource: "zfsreplicationschedules.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "update", resource: "zfsreplicationschedules.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "patch", resource: "zfsreplicationschedules.zfsreplication.ringhof.io", subresource: "status"},
		{verb: "create", resource: "jobs.batch"},
		{verb: "get", resource: "jobs.batch"},
		{verb: "list", resource: "jobs.batch"},
		{verb: "watch", resource: "jobs.batch"},
		{verb: "update", resource: "jobs.batch"},
		{verb: "patch", resource: "jobs.batch"},
		{verb: "delete", resource: "jobs.batch"},
		{verb: "create", resource: "secrets"},
		{verb: "get", resource: "secrets"},
		{verb: "list", resource: "secrets"},
		{verb: "watch", resource: "secrets"},
		{verb: "update", resource: "secrets"},
		{verb: "patch", resource: "secrets"},
		{verb: "delete", resource: "secrets"},
		{verb: "get", resource: "pods"},
		{verb: "list", resource: "pods"},
		{verb: "watch", resource: "pods"},
		{verb: "delete", resource: "pods"},
		{verb: "get", resource: "pods/log"},
		{verb: "create", resource: "events"},
		{verb: "patch", resource: "events"},
	} {
		k.assertCanIWithSubresource(tt.verb, tt.resource, tt.subresource, e2eNamespace, true)
	}
}

func TestE2ENamespacedDeploymentSmoke(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	smokeName := "e2e-ns-" + suffix
	ignoredName := "e2e-ignore-" + suffix

	k.applyNamespacedControllerProfile()
	t.Cleanup(func() { k.restoreClusterControllerProfile() })
	k.cleanupReplicationOnExitInNamespace(e2eSmokeNamespace, smokeName)
	k.cleanupReplicationOnExitInNamespace(e2eNamespace, ignoredName)
	k.cleanupReplicationInNamespace(e2eSmokeNamespace, smokeName)
	k.cleanupReplication(ignoredName)

	k.assertControllerWatchesNamespace(e2eSmokeNamespace)
	k.assertCanI("create", "jobs.batch", e2eSmokeNamespace, true)
	k.assertCanI("delete", "pods", e2eSmokeNamespace, true)
	k.assertCanI("create", "jobs.batch", e2eNamespace, false)
	k.assertCanI("delete", "pods", e2eNamespace, false)

	smoke := replicationCase{
		Name:          smokeName,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eSourceNode,
		SourceDataset: pool + "/same-ns-" + suffix,
		TargetDataset: pool + "/same-ns-" + suffix,
	}
	k.applyReplicationInNamespace(e2eSmokeNamespace, smoke)
	status := k.waitForFailedInNamespace(e2eSmokeNamespace, smoke, 2*time.Minute)
	assertFailedStatus(t, smoke, status, "source and target must not reference the same dataset on the same node")
	k.assertNoJobsOrSecretsInNamespace(e2eSmokeNamespace, smoke.Name)

	ignored := smoke
	ignored.Name = ignoredName
	ignored.SourceDataset = pool + "/same-ignored-" + suffix
	ignored.TargetDataset = pool + "/same-ignored-" + suffix
	k.applyReplication(ignored)
	k.assertRunUnreconciled(e2eNamespace, ignored.Name, 20*time.Second)
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

func (c replicationCase) manifestJSONInNamespace(t *testing.T, namespace string) []byte {
	t.Helper()
	metadata := map[string]any{
		"name":      c.Name,
		"namespace": namespace,
	}
	if len(c.Annotations) > 0 {
		metadata["annotations"] = c.Annotations
	}
	doc := map[string]any{
		"apiVersion": "zfsreplication.ringhof.io/v1alpha1",
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
	k.assertReceiverDaemonSetAvailable()
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
	k.cleanupReplicationInNamespace(e2eNamespace, name)
}

func (k kubectlRunner) cleanupReplicationInNamespace(namespace, name string) {
	k.t.Helper()
	k.run(75*time.Second, "delete", "zfsreplicationrun", name, "-n", namespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "zfsreceivetasks", "-n", namespace, "-l", e2eLabelPrefix+"/run="+name, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "jobs,pods,secrets", "-n", namespace, "-l", e2eLabelPrefix+"/run="+name, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
}

func (k kubectlRunner) cleanupReplicationOnExit(name string) {
	k.cleanupReplicationOnExitInNamespace(e2eNamespace, name)
}

func (k kubectlRunner) cleanupReplicationOnExitInNamespace(namespace, name string) {
	k.t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		k.t.Logf("leaving e2e resources for %s/%s because E2E_KEEP_RESOURCES=1", namespace, name)
		return
	}
	k.t.Cleanup(func() { k.cleanupReplicationInNamespace(namespace, name) })
}

func (k kubectlRunner) applyReplication(sc replicationCase) {
	k.applyReplicationInNamespace(e2eNamespace, sc)
}

func (k kubectlRunner) applyReplicationInNamespace(namespace string, sc replicationCase) {
	k.t.Helper()
	manifest := sc.manifestJSONInNamespace(k.t, namespace)
	if _, err := k.runInput(30*time.Second, manifest, "apply", "-f", "-"); err != nil {
		k.t.Fatalf("apply replication manifest:\n%s\n%v", manifest, err)
	}
}

func (k kubectlRunner) waitForSuccess(sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForStatusInNamespace(e2eNamespace, sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Succeeded"
	}, "Succeeded")
}

func (k kubectlRunner) waitForFailed(sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForFailedInNamespace(e2eNamespace, sc, timeout)
}

func (k kubectlRunner) waitForFailedInNamespace(namespace string, sc replicationCase, timeout time.Duration) replicationStatus {
	return k.waitForStatusInNamespace(namespace, sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Failed"
	}, "Failed")
}

func (k kubectlRunner) waitForDestinationLock(sc replicationCase, blockerName, targetDataset string, timeout time.Duration) replicationStatus {
	k.t.Helper()
	return k.waitForStatusInNamespace(e2eNamespace, sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Pending" &&
			strings.Contains(st.LastError, "waiting for active run "+blockerName) &&
			strings.Contains(st.LastError, targetDataset)
	}, "Pending destination lock")
}

func (k kubectlRunner) waitForRunPastPending(sc replicationCase, timeout time.Duration) replicationStatus {
	k.t.Helper()
	deadline := time.Now().Add(timeout)
	var last replicationStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := k.getStatusInNamespace(e2eNamespace, sc.Name)
		if err == nil {
			last = status
			if status.Phase != "" && status.Phase != "Pending" {
				return status
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	k.collectDiagnosticsInNamespace(e2eNamespace, sc.Name)
	k.t.Fatalf("timed out waiting for %s to move past Pending; last status=%#v last error=%v", sc.Name, last, lastErr)
	return replicationStatus{}
}

func (k kubectlRunner) waitForStatusInNamespace(namespace string, sc replicationCase, timeout time.Duration, ready func(replicationStatus) bool, want string) replicationStatus {
	k.t.Helper()
	deadline := time.Now().Add(timeout)
	var last replicationStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := k.getStatusInNamespace(namespace, sc.Name)
		if err == nil {
			last = status
			if ready(status) {
				return status
			}
			if status.Phase == "Failed" && !strings.HasPrefix(want, "Failed") {
				k.collectDiagnosticsInNamespace(namespace, sc.Name)
				k.t.Fatalf("%s reached Failed while waiting for %s: %#v", sc.Name, want, status)
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	k.collectDiagnosticsInNamespace(namespace, sc.Name)
	k.t.Fatalf("timed out waiting for %s to reach %s; last status=%#v last error=%v", sc.Name, want, last, lastErr)
	return replicationStatus{}
}

func (k kubectlRunner) getStatusInNamespace(namespace, name string) (replicationStatus, error) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "zfsreplicationrun", name, "-n", namespace, "-o", "json")
	if err != nil {
		return replicationStatus{}, err
	}
	var obj replicationObject
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return replicationStatus{}, fmt.Errorf("parse zfsreplicationrun status: %w\n%s", err, out)
	}
	return obj.Status, nil
}

func (k kubectlRunner) getReceiveTaskStatus(namespace, name string) (receiveTaskStatus, error) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "zfsreceivetask", name, "-n", namespace, "-o", "json")
	if err != nil {
		return receiveTaskStatus{}, err
	}
	var obj receiveTaskObject
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return receiveTaskStatus{}, fmt.Errorf("parse zfsreceivetask status: %w\n%s", err, out)
	}
	return obj.Status, nil
}

func (k kubectlRunner) collectDiagnosticsInNamespace(namespace, name string) {
	for _, args := range [][]string{
		{"get", "zfsreplicationrun", name, "-n", namespace, "-o", "yaml"},
		{"get", "zfsreceivetasks", "-n", namespace, "-o", "yaml"},
		{"get", "pods,jobs,secrets,leases", "-n", namespace, "-o", "wide"},
		{"get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
		{"logs", "-n", e2eControllerNamespace, "deployment/" + e2eControllerDeploymentName},
		{"logs", "-n", e2eControllerNamespace, "daemonset/zfs-receiver"},
	} {
		if out, err := k.runOutput(25*time.Second, args...); err == nil {
			k.t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
		} else {
			k.t.Logf("kubectl %s failed: %v", strings.Join(args, " "), err)
		}
	}

	pods, err := k.podsForReplicationInNamespace(namespace, name)
	if err != nil {
		k.t.Logf("list datamover pods for diagnostics failed: %v", err)
		return
	}
	for _, pod := range pods.Items {
		args := []string{"logs", "-n", namespace, "pod/" + pod.Metadata.Name, "--all-containers=true"}
		if out, err := k.runOutput(20*time.Second, args...); err == nil {
			k.t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
		} else {
			k.t.Logf("kubectl %s failed: %v", strings.Join(args, " "), err)
		}
	}
}

func (k kubectlRunner) podsForReplicationInNamespace(namespace, name string) (podList, error) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name
	out, err := k.runOutput(20*time.Second, "get", "pods", "-n", namespace, "-l", selector, "-o", "json")
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
	k.assertRunEphemeralCleanupInNamespace(e2eNamespace, name)
}

func (k kubectlRunner) assertRunEphemeralCleanupInNamespace(namespace, name string) {
	k.t.Helper()
	k.assertNoLabelledResourcesEventuallyInNamespace(namespace, name, "run secrets", "secrets", "")
}

func (k kubectlRunner) assertNoLabelledResourcesEventuallyInNamespace(namespace, name, description, resource, extraSelector string) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name
	if extraSelector != "" {
		selector += "," + extraSelector
	}
	deadline := time.Now().Add(60 * time.Second)
	var lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := k.runOutput(20*time.Second, "get", resource, "-n", namespace, "-l", selector, "-o", "name")
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
	k.collectDiagnosticsInNamespace(namespace, name)
	k.t.Fatalf("%s still exist for %s/%s after terminal cleanup; selector=%q last output=%q last error=%v", description, namespace, name, selector, lastOut, lastErr)
}

func (k kubectlRunner) assertNoJobsOrSecrets(name string) {
	k.t.Helper()
	k.assertNoJobsOrSecretsInNamespace(e2eNamespace, name)
}

func (k kubectlRunner) assertNoJobsOrSecretsInNamespace(namespace, name string) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "jobs,secrets", "-n", namespace, "-l", e2eLabelPrefix+"/run="+name, "-o", "name")
	if err != nil && !strings.Contains(err.Error(), "No resources found") {
		k.t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		k.t.Fatalf("jobs/secrets exist for %s/%s:\n%s", namespace, name, out)
	}
}

func (k kubectlRunner) assertNoSenderJob(name string) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name + "," + e2eLabelPrefix + "/role=sender"
	out, err := k.runOutput(20*time.Second, "get", "jobs", "-n", e2eNamespace, "-l", selector, "-o", "name")
	if err != nil && !strings.Contains(err.Error(), "No resources found") {
		k.t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		k.collectDiagnosticsInNamespace(e2eNamespace, name)
		k.t.Fatalf("sender job exists for blocked run %s:\n%s", name, out)
	}
}

type terminalCleanupObject string

const (
	terminalCleanupReceiveTask terminalCleanupObject = "receive task"
	terminalCleanupSSHSecret   terminalCleanupObject = "ssh secret"
)

func (k kubectlRunner) scaleController(replicas int) {
	k.t.Helper()
	k.run(90*time.Second, "scale", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "--replicas="+strconv.Itoa(replicas))
	k.run(120*time.Second, "rollout", "status", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "--timeout=120s")
}

func (k kubectlRunner) patchRunPhase(namespace, name, phase string) {
	k.t.Helper()
	payload := fmt.Sprintf(`{"status":{"phase":%q}}`, phase)
	k.run(30*time.Second, "patch", "zfsreplicationrun", name, "-n", namespace, "--subresource=status", "--type=merge", "-p", payload)
}

func (k kubectlRunner) seedTerminalCleanupObjects(namespace string, sc replicationCase, objects []terminalCleanupObject) {
	k.t.Helper()
	for _, obj := range objects {
		var manifest []byte
		switch obj {
		case terminalCleanupReceiveTask:
			manifest = receiveTaskManifest(k.t, namespace, sc)
		case terminalCleanupSSHSecret:
			manifest = runSSHSecretManifest(k.t, namespace, sc.Name)
		default:
			k.t.Fatalf("unknown cleanup object %q", obj)
		}
		if _, err := k.runInput(30*time.Second, manifest, "apply", "-f", "-"); err != nil {
			k.t.Fatalf("seed %s for %s/%s:\n%s\n%v", obj, namespace, sc.Name, manifest, err)
		}
	}
}

func (k kubectlRunner) logSelectedRunObjects(namespace, name, stage string) {
	k.t.Helper()
	for _, selector := range []string{
		e2eLabelPrefix + "/run=" + name,
		e2eLabelPrefix + "/run=" + name + "," + e2eLabelPrefix + "/role=receiver",
	} {
		args := []string{"get", "zfsreceivetasks,pods,jobs,secrets", "-n", namespace, "-l", selector, "-o", "wide", "--show-labels"}
		out, err := k.runOutput(20*time.Second, args...)
		if err != nil && !strings.Contains(err.Error(), "No resources found") {
			k.t.Fatalf("capture %s objects for %s/%s selector %q: %v", stage, namespace, name, selector, err)
		}
		k.t.Logf("%s objects for %s/%s selector %q:\n%s", stage, namespace, name, selector, out)
	}
}

func (k kubectlRunner) assertReceiverDaemonSetAvailable() {
	k.t.Helper()
	k.run(90*time.Second, "rollout", "status", "daemonset/zfs-receiver", "-n", e2eControllerNamespace, "--timeout=90s")
}

func (k kubectlRunner) assertReceiveTaskTerminal(sc replicationCase, runStatus replicationStatus, wantPhase string) {
	k.t.Helper()
	if runStatus.ReceiveTaskName == "" {
		k.t.Fatalf("receiveTaskName is empty for %s: %#v", sc.Name, runStatus)
	}
	st := k.assertReceiveTaskPhase(e2eNamespace, runStatus.ReceiveTaskName, wantPhase)
	if st.Endpoint.Host == "" || st.Endpoint.Port == 0 || st.SSH.HostKey == "" || st.ReceiverPod.Name == "" {
		k.t.Fatalf("receive task status missing daemonset receiver details for %s: %#v", sc.Name, st)
	}
	if wantPhase == e2eReceiveTaskPhaseCompleted && st.Error != "" {
		k.t.Fatalf("receive task error = %q, want empty", st.Error)
	}
	if wantPhase == e2eReceiveTaskPhaseFailed && st.Error == "" {
		k.t.Fatalf("receive task error is empty for failed task %s", runStatus.ReceiveTaskName)
	}
}

func (k kubectlRunner) assertReceiveTaskPhase(namespace, name, wantPhase string) receiveTaskStatus {
	k.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last receiveTaskStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := k.getReceiveTaskStatus(namespace, name)
		if err != nil {
			lastErr = err
		} else {
			last = status
			if status.Phase == wantPhase {
				return status
			}
		}
		time.Sleep(1 * time.Second)
	}
	k.collectDiagnosticsInNamespace(namespace, strings.TrimPrefix(name, "zfsrep-"))
	k.t.Fatalf("timed out waiting for receive task %s/%s to reach %s; last status=%#v last error=%v", namespace, name, wantPhase, last, lastErr)
	return receiveTaskStatus{}
}

func (k kubectlRunner) assertCanI(verb, resource, namespace string, want bool) {
	k.assertCanIWithSubresource(verb, resource, "", namespace, want)
}

func (k kubectlRunner) assertCanIWithSubresource(verb, resource, subresource, namespace string, want bool) {
	k.t.Helper()
	args := []string{"auth", "can-i", verb, resource, "--as=" + e2eControllerServiceAccount}
	if subresource != "" {
		args = append(args, "--subresource="+subresource)
	}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := k.runOutput(20*time.Second, args...)
	trimmed := strings.TrimSpace(out)
	if err != nil && trimmed != "no" {
		k.t.Fatalf("kubectl auth can-i %s %s in %s: %v", verb, resource, namespace, err)
	}
	got := trimmed == "yes"
	if got != want {
		if subresource != "" {
			resource += "/" + subresource
		}
		k.t.Fatalf("controller service account can-i %s %s in %s = %t (%q), want %t", verb, resource, namespace, got, trimmed, want)
	}
}

func (k kubectlRunner) applyNamespacedControllerProfile() {
	k.t.Helper()
	repoRoot := e2eRepoRoot(k.t)
	k.run(60*time.Second, "delete", "clusterrolebinding", e2eControllerDeploymentName, "--ignore-not-found=true")
	k.run(60*time.Second, "delete", "clusterrole", e2eControllerDeploymentName, "--ignore-not-found=true")
	k.run(120*time.Second, "apply", "-k", repoRoot)
	k.useE2EImages()
	k.run(180*time.Second, "rollout", "status", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "--timeout=180s")
	k.run(180*time.Second, "rollout", "status", "daemonset/zfs-receiver", "-n", e2eControllerNamespace, "--timeout=180s")
}

func (k kubectlRunner) restoreClusterControllerProfile() {
	k.t.Helper()
	k.run(120*time.Second, "apply", "-k", filepath.Join(e2eRepoRoot(k.t), "config"))
	k.useE2EImages()
	k.run(180*time.Second, "rollout", "status", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "--timeout=180s")
	k.run(180*time.Second, "rollout", "status", "daemonset/zfs-receiver", "-n", e2eControllerNamespace, "--timeout=180s")
}

func (k kubectlRunner) useE2EImages() {
	k.t.Helper()
	k.run(60*time.Second, "set", "image", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "manager="+e2eImageTag())
	k.run(60*time.Second, "set", "image", "daemonset/zfs-receiver", "-n", e2eControllerNamespace, "receiver="+e2eImageTag())
	k.run(60*time.Second, "set", "env", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "DATA_MOVER_IMAGE="+e2eImageTag())
	controllerPatch := `{"spec":{"template":{"spec":{"containers":[{"name":"manager","imagePullPolicy":"IfNotPresent"}]}}}}`
	k.run(60*time.Second, "patch", "deployment/"+e2eControllerDeploymentName, "-n", e2eControllerNamespace, "--type=strategic", "-p", controllerPatch)
	receiverPatch := `{"spec":{"template":{"spec":{"containers":[{"name":"receiver","imagePullPolicy":"IfNotPresent"}]}}}}`
	k.run(60*time.Second, "patch", "daemonset/zfs-receiver", "-n", e2eControllerNamespace, "--type=strategic", "-p", receiverPatch)
}

func (k kubectlRunner) assertControllerWatchesNamespace(namespace string) {
	k.t.Helper()
	out, err := k.runOutput(20*time.Second, "get", "deployment", e2eControllerDeploymentName, "-n", e2eControllerNamespace, "-o", "json")
	if err != nil {
		k.t.Fatal(err)
	}
	var deployment deploymentObject
	if err := json.Unmarshal([]byte(out), &deployment); err != nil {
		k.t.Fatalf("parse controller deployment: %v\n%s", err, out)
	}
	manager := deployment.managerContainer()
	if manager == nil {
		k.t.Fatalf("controller deployment has no manager container")
	}
	if !contains(manager.Args, "--watch-namespace=$(WATCH_NAMESPACE)") {
		k.t.Fatalf("manager args = %v, missing watch namespace arg", manager.Args)
	}
	if got := deploymentEnvValue(manager.Env, "WATCH_NAMESPACE"); got != namespace {
		k.t.Fatalf("WATCH_NAMESPACE = %q, want %s", got, namespace)
	}
}

func (k kubectlRunner) assertRunUnreconciled(namespace, name string, duration time.Duration) {
	k.t.Helper()
	deadline := time.Now().Add(duration)
	var last replicationStatus
	for time.Now().Before(deadline) {
		status, err := k.getStatusInNamespace(namespace, name)
		if err != nil {
			k.t.Fatalf("get ignored run status: %v", err)
		}
		last = status
		if status.Phase != "" {
			k.collectDiagnosticsInNamespace(namespace, name)
			k.t.Fatalf("%s/%s was reconciled while controller should only watch %s: %#v", namespace, name, e2eSmokeNamespace, status)
		}
		time.Sleep(1 * time.Second)
	}
	k.assertNoJobsOrSecretsInNamespace(namespace, name)
	if last != (replicationStatus{}) {
		k.t.Logf("%s/%s remained unreconciled for %s; last status=%#v", namespace, name, duration, last)
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

func (k kubectlRunner) assertRealZFSSyncoidSnapshots(node, jobName, pool, dataset string, wantCount int) {
	k.t.Helper()
	out := k.runRealZFS(node, jobName, realZFSSyncoidSnapshotsScript(pool, dataset))
	snapshots := nonEmptyOutputLines(out)
	if len(snapshots) != wantCount {
		k.t.Fatalf("syncoid snapshots for %s on %s = %v, want %d", dataset, node, snapshots, wantCount)
	}
	for _, snapshot := range snapshots {
		if !strings.Contains(snapshot, "_zfsrep-sender_") {
			k.t.Fatalf("syncoid snapshot %q does not include stable sender hostname", snapshot)
		}
	}
}

func nonEmptyOutputLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
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
	ReceiveTaskName string `json:"receiveTaskName"`
	ReceiverPodName string `json:"receiverPodName"`
	ReceiverPodIP   string `json:"receiverPodIP"`
	SSHSecretName   string `json:"sshSecretName"`
	StartedAt       string `json:"startedAt"`
	CompletedAt     string `json:"completedAt"`
	LastError       string `json:"lastError"`
}

type receiveTaskObject struct {
	Status receiveTaskStatus `json:"status"`
}

type receiveTaskStatus struct {
	Phase    string `json:"phase"`
	Endpoint struct {
		Host string `json:"host"`
		Port int32  `json:"port"`
	} `json:"endpoint"`
	SSH struct {
		HostKey string `json:"hostKey"`
	} `json:"ssh"`
	ReceiverPod struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"receiverPod"`
	Error string `json:"error"`
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

type deploymentObject struct {
	Spec struct {
		Template struct {
			Spec struct {
				Containers []deploymentContainer `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

type deploymentContainer struct {
	Name string             `json:"name"`
	Args []string           `json:"args"`
	Env  []deploymentEnvVar `json:"env"`
}

type deploymentEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (d deploymentObject) managerContainer() *deploymentContainer {
	for i := range d.Spec.Template.Spec.Containers {
		if d.Spec.Template.Spec.Containers[i].Name == "manager" {
			return &d.Spec.Template.Spec.Containers[i]
		}
	}
	return nil
}

func deploymentEnvValue(env []deploymentEnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}
	return ""
}

func contains(items []string, needle string) bool {
	return slices.Contains(items, needle)
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
	if st.ReceiveTaskName == "" || st.ReceiverPodName == "" || st.ReceiverPodIP == "" || st.SSHSecretName == "" {
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
	if st.ReceiveTaskName == "" || st.ReceiverPodName == "" || st.ReceiverPodIP == "" || st.SSHSecretName == "" {
		t.Fatalf("receiver/ssh status names missing after datamover setup for %s: %#v", sc.Name, st)
	}
}

func receiveTaskManifest(t *testing.T, namespace string, sc replicationCase) []byte {
	t.Helper()
	doc := map[string]any{
		"apiVersion": "zfsreplication.ringhof.io/v1alpha1",
		"kind":       "ZFSReceiveTask",
		"metadata": map[string]any{
			"name":      runObjectName(sc.Name, "receiver"),
			"namespace": namespace,
			"labels":    runLabels(sc.Name, "receiver"),
		},
		"spec": map[string]any{
			"runRef": map[string]any{
				"name": sc.Name,
			},
			"nodeName": sc.TargetNode,
			"destination": map[string]any{
				"dataset": sc.TargetDataset,
			},
			"ssh": map[string]any{
				"authorizedPublicKey": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIE2EReceiveTaskTestKey e2e@example.invalid",
				"expiresAt":           time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339),
			},
			"policy": map[string]any{
				"receiveUnmounted": true,
				"receiveResumable": true,
				"compression":      "none",
			},
		},
	}
	return manifestBytes(t, doc)
}

func runSSHSecretManifest(t *testing.T, namespace, runName string) []byte {
	t.Helper()
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      runObjectName(runName, "ssh"),
			"namespace": namespace,
			"labels":    runLabels(runName, "ssh"),
		},
		"type": "Opaque",
		"stringData": map[string]any{
			"id_rsa":          "test",
			"id_rsa.pub":      "test",
			"authorized_keys": "test",
		},
	}
	return manifestBytes(t, doc)
}

func runLabels(runName, role string) map[string]any {
	return map[string]any{
		e2eLabelPrefix + "/run":  runName,
		e2eLabelPrefix + "/role": role,
	}
}

func runObjectName(runName, suffix string) string {
	return "zfsrep-" + runName + "-" + suffix
}

func manifestBytes(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
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

func realZFSSyncoidSnapshotsScript(pool, dataset string) string {
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"zfs list -H -t snapshot -o name -r " + shellQuote(dataset) + ` | awk -F@ '$2 ~ /^syncoid_/ { print $0 }'`,
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

func e2eRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func uniqueSuffix() string {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	if len(suffix) > 8 {
		return suffix[len(suffix)-8:]
	}
	return suffix
}
