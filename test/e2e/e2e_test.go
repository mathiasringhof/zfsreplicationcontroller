package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	e2eNamespace   = "storage"
	e2eReplication = "e2e-full"
	e2eRunID       = "e2e-full-1"

	e2eSourceNode = "worker-a"
	e2eTargetNode = "worker-b"

	e2eSourceDataset = "tank/src"
	e2eTargetDataset = "tank/dst"
	e2eSnapshotName  = "zsync-e2e-full-1"
)

func TestE2EFullBootstrap(t *testing.T) {
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
	k.run(75*time.Second, "delete", "zfsreplication", e2eReplication, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "jobs,secrets", "-n", e2eNamespace, "-l", "zfsreplication.example.com/name="+e2eReplication, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(75*time.Second, "delete", "lease", "zfsrep-"+e2eReplication, "-n", e2eNamespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s")
	k.run(30*time.Second, "apply", "-f", "./manifests/samples/full-bootstrap.yaml")
	if _, err := k.runOutput(210*time.Second, "wait", "-n", e2eNamespace, "--for=jsonpath={.status.phase}=Succeeded", "zfsreplication/"+e2eReplication, "--timeout=180s"); err != nil {
		k.collectDiagnostics()
		t.Fatal(err)
	}
	assertFullBootstrapZFSEvents(t, k.zfsSimEvents())
}

type kubectlRunner struct {
	t          *testing.T
	path       string
	kubeconfig string
}

func (k kubectlRunner) ensureNamespace(namespace string) {
	if _, err := k.runOutput(20*time.Second, "get", "namespace", namespace); err == nil {
		return
	}
	if _, err := k.runOutput(20*time.Second, "create", "namespace", namespace); err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		k.t.Fatal(err)
	}
}

func (k kubectlRunner) collectDiagnostics() {
	for _, args := range [][]string{
		{"get", "zfsreplication", e2eReplication, "-n", e2eNamespace, "-o", "yaml"},
		{"get", "pods,jobs,secrets,leases", "-n", e2eNamespace, "-o", "wide"},
		{"get", "events", "-n", e2eNamespace, "--sort-by=.lastTimestamp"},
		{"logs", "-n", "zfsreplication-system", "deployment/zfsreplication-controller"},
	} {
		if out, err := k.runOutput(20*time.Second, args...); err == nil {
			k.t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
		} else {
			k.t.Logf("kubectl %s failed: %v", strings.Join(args, " "), err)
		}
	}
}

func (k kubectlRunner) zfsSimEvents() []zfsSimEvent {
	k.t.Helper()

	out, err := k.runOutput(20*time.Second, "get", "pods", "-n", e2eNamespace, "-l", "zfsreplication.example.com/name="+e2eReplication+",zfsreplication.example.com/run-id="+e2eRunID, "-o", "json")
	if err != nil {
		k.t.Fatal(err)
	}
	var pods podList
	if err := json.Unmarshal([]byte(out), &pods); err != nil {
		k.t.Fatalf("parse pod list: %v\n%s", err, out)
	}
	if len(pods.Items) == 0 {
		k.t.Fatalf("no datamover pods found for %s/%s", e2eReplication, e2eRunID)
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
		k.t.Fatalf("no zfs simulator events found in datamover pod logs")
	}
	return events
}

func (k kubectlRunner) run(timeout time.Duration, args ...string) {
	if _, err := k.runOutput(timeout, args...); err != nil {
		k.t.Fatal(err)
	}
}

func (k kubectlRunner) runOutput(timeout time.Duration, args ...string) (string, error) {
	k.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fullArgs := append([]string{"--kubeconfig", k.kubeconfig, "--request-timeout=" + timeout.String()}, args...)
	cmd := exec.CommandContext(ctx, k.path, fullArgs...)
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
		idx := strings.Index(line, "zfs-sim-event ")
		if idx == -1 {
			continue
		}
		raw := line[idx+len("zfs-sim-event "):]
		var event zfsSimEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			t.Fatalf("parse zfs simulator event from %s: %v\nline: %s", podName, err, line)
		}
		events = append(events, event)
	}
	return events
}

func assertFullBootstrapZFSEvents(t *testing.T, events []zfsSimEvent) {
	t.Helper()

	sourceSnap := e2eSourceDataset + "@" + e2eSnapshotName
	targetSnap := e2eTargetDataset + "@" + e2eSnapshotName

	for _, event := range events {
		if event.RunID != e2eRunID {
			t.Fatalf("event runID = %q, want %q: %#v", event.RunID, e2eRunID, event)
		}
		if event.Action == "unsupported" {
			t.Fatalf("unsupported zfs command reached simulator: %#v", event)
		}
		switch event.Role {
		case "sender":
			if event.Node != e2eSourceNode {
				t.Fatalf("sender event ran on node %q, want %q: %#v", event.Node, e2eSourceNode, event)
			}
		case "receiver":
			if event.Node != e2eTargetNode {
				t.Fatalf("receiver event ran on node %q, want %q: %#v", event.Node, e2eTargetNode, event)
			}
		default:
			t.Fatalf("event role = %q, want sender or receiver: %#v", event.Role, event)
		}
	}

	senderList := requireEvent(t, events, "sender", "list-snapshot", sourceSnap)
	if senderList.Detail != "missing" {
		t.Fatalf("initial source snapshot lookup detail = %q, want missing: %#v", senderList.Detail, senderList)
	}
	requireEvent(t, events, "sender", "snapshot", sourceSnap)
	send := requireEvent(t, events, "sender", "send", sourceSnap)
	if send.Command != "zfs send "+sourceSnap {
		t.Fatalf("send command = %q, want %q", send.Command, "zfs send "+sourceSnap)
	}
	if send.Detail != "base=" {
		t.Fatalf("send detail = %q, want base=: %#v", send.Detail, send)
	}
	if send.Bytes <= 0 || send.SHA256 == "" {
		t.Fatalf("send did not record payload bytes and hash: %#v", send)
	}
	requireEvent(t, events, "sender", "get-guid", sourceSnap)

	requireEvent(t, events, "receiver", "get-mounted", e2eTargetDataset)
	requireEvent(t, events, "receiver", "destroy", e2eTargetDataset)
	receive := requireEvent(t, events, "receiver", "receive", targetSnap)
	if receive.Command != "zfs receive -u -s "+e2eTargetDataset {
		t.Fatalf("receive command = %q, want %q", receive.Command, "zfs receive -u -s "+e2eTargetDataset)
	}
	if receive.Detail != "args=-u -s" {
		t.Fatalf("receive detail = %q, want args=-u -s: %#v", receive.Detail, receive)
	}
	targetList := requireEvent(t, events, "receiver", "list-snapshot", targetSnap)
	if targetList.Detail != "exists" {
		t.Fatalf("target snapshot lookup detail = %q, want exists: %#v", targetList.Detail, targetList)
	}
	if receive.Bytes != send.Bytes || receive.SHA256 != send.SHA256 {
		t.Fatalf("receive payload = %d/%s, want send payload %d/%s", receive.Bytes, receive.SHA256, send.Bytes, send.SHA256)
	}

	assertEventOrder(t, events, []eventMatch{
		{role: "sender", action: "list-snapshot", target: sourceSnap},
		{role: "sender", action: "snapshot", target: sourceSnap},
		{role: "sender", action: "send", target: sourceSnap},
		{role: "sender", action: "get-guid", target: sourceSnap},
	})
	assertEventOrder(t, events, []eventMatch{
		{role: "receiver", action: "get-mounted", target: e2eTargetDataset},
		{role: "receiver", action: "destroy", target: e2eTargetDataset},
		{role: "receiver", action: "receive", target: targetSnap},
		{role: "receiver", action: "list-snapshot", target: targetSnap},
	})
}

type eventMatch struct {
	role   string
	action string
	target string
}

func requireEvent(t *testing.T, events []zfsSimEvent, role, action, target string) zfsSimEvent {
	t.Helper()

	var matches []zfsSimEvent
	for _, event := range events {
		if event.Role == role && event.Action == action && event.Target == target {
			matches = append(matches, event)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("found %d events for role=%s action=%s target=%s, want exactly 1\nall events:\n%s", len(matches), role, action, target, formatZFSEvents(events))
	}
	return matches[0]
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
		_, _ = fmt.Fprintf(&b, "%s %s %s %s %q bytes=%d sha=%s detail=%q\n", event.Role, event.Node, event.Action, event.Target, event.Command, event.Bytes, event.SHA256, event.Detail)
	}
	return b.String()
}
