package e2e

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	e2eNamespace   = "storage"
	e2eReplication = "e2e-full"
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
