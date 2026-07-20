package receiverauthorization

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const testPolicyID = "policy-0123456789abcdef0123456789abcdef"

func TestModuleAdmitsAllowedCommand(t *testing.T) {
	dir := t.TempDir()
	writeTestPolicy(t, dir, `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"receiveResumable":true,
		"compression":"none"
	}`)

	if _, err := New(dir).Admit(testReference(t), "zfs receive -u -s tank/dst"); err != nil {
		t.Fatalf("Admit() error = %v, want nil", err)
	}
}

func TestModuleAdmissionLoadsPolicyBeforeCheckingOriginalCommand(t *testing.T) {
	_, err := New(t.TempDir()).Admit(testReference(t), "")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Admit() error = %v, want missing policy error", err)
	}
}

func TestModuleAdmissionRejectsSymlinkedPolicy(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{"targetDataset":"tank/dst","receiveUnmounted":true,"compression":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, testPolicyID+".json")); err != nil {
		t.Fatal(err)
	}

	if _, err := New(dir).Admit(testReference(t), "zfs receive -u tank/dst"); err == nil {
		t.Fatal("Admit() error = nil, want symlink rejection")
	}
}

func TestModuleAdmissionPreservesLegacyMountedReceiveBehavior(t *testing.T) {
	dir := t.TempDir()
	writeTestPolicy(t, dir, `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":false,
		"receiveResumable":true,
		"compression":"none"
	}`)

	if _, err := New(dir).Admit(testReference(t), "zfs receive -s tank/dst"); err != nil {
		t.Fatalf("Admit() error = %v, want legacy mounted receive allowed", err)
	}
}

func TestModuleAdmissionPreservesExplicitMountedReceiveDenial(t *testing.T) {
	dir := t.TempDir()
	writeTestPolicy(t, dir, `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":false,
		"allowMount":false,
		"receiveResumable":true,
		"compression":"none"
	}`)

	if _, err := New(dir).Admit(testReference(t), "zfs receive -s tank/dst"); err == nil {
		t.Fatal("Admit() error = nil, want mounted receive rejected")
	}
}

func TestPlanExecuteSuppliesStandardStreams(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, "#!/bin/sh\ninput=$(cat)\nprintf 'out:%s' \"$input\"\nprintf 'err:%s' \"$input\" >&2\n")
	replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	plan := admitTestPlan(t, "zfs receive -u -s tank/dst", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"receiveResumable":true,
		"compression":"none"
	}`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if err := plan.Execute(context.Background(), strings.NewReader("payload\n"), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "out:payload" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "out:payload")
	}
	if stderr.String() != "err:payload" {
		t.Fatalf("stderr = %q, want %q", stderr.String(), "err:payload")
	}
}

func TestPlanExecuteReturnsWriterErrorUnchanged(t *testing.T) {
	plan := admitTestPlan(t, "echo -n hello", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"compression":"none"
	}`)
	writeErr := &testWriterError{}

	err := plan.Execute(context.Background(), nil, errorWriter{err: writeErr}, nil)
	assertWriterErrorUnchanged(t, err, writeErr)
}

func TestPlanExecuteReturnsLookupWriterErrorUnchanged(t *testing.T) {
	replaceAllowedCommandResolver(t, map[string]string{"mbuffer": "/usr/bin/mbuffer"})
	plan := admitTestPlan(t, "command -v mbuffer", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"compression":"none"
	}`)
	writeErr := &testWriterError{}

	err := plan.Execute(context.Background(), nil, errorWriter{err: writeErr}, nil)
	assertWriterErrorUnchanged(t, err, writeErr)
}

func TestPlanExecuteUsesMinimalChildEnvironment(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "env.txt")
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, fmt.Sprintf("#!/bin/sh\nprintf '%%s|%%s|%%s|%%s\\n' \"$SSH_ORIGINAL_COMMAND\" \"$LD_PRELOAD\" \"$LC_ALL\" \"$LANG\" > %q\n", envPath))
	replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	t.Setenv("SSH_ORIGINAL_COMMAND", "attacker-controlled")
	t.Setenv("LD_PRELOAD", "/tmp/injected.dylib")
	plan := admitTestPlan(t, "zfs get -H name tank/dst", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"compression":"none"
	}`)

	if err := plan.Execute(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != "||C|C" {
		t.Fatalf("child environment = %q, want only explicit locale values", got)
	}
}

func TestPlanExecuteRunsBatchInOrder(t *testing.T) {
	dir := t.TempDir()
	orderPath := filepath.Join(dir, "order.txt")
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\n", orderPath))
	replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	plan := admitTestPlan(t, "zfs destroy tank/dst@snap-a; zfs destroy tank/dst@snap-b", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"allowTargetSnapshotDestroy":true,
		"compression":"none"
	}`)

	if err := plan.Execute(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(orderPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "destroy tank/dst@snap-a\ndestroy tank/dst@snap-b\n" {
		t.Fatalf("execution order = %q", got)
	}
}

func TestPlanExecuteCancelsAndWaitsForRemainingPipelineProcesses(t *testing.T) {
	dir := t.TempDir()
	fail := filepath.Join(dir, "mbuffer")
	pidFile := filepath.Join(dir, "zfs.pid")
	block := filepath.Join(dir, "zfs")
	writeScript(t, fail, "#!/bin/sh\nsleep 0.5\nexit 42\n")
	writeScript(t, block, fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$$\" > %q\nwhile :; do sleep 1; done\n", pidFile))
	replaceAllowedCommandResolver(t, map[string]string{
		"mbuffer": fail,
		"zfs":     block,
	})
	plan := admitTestPlan(t, "mbuffer -q | zfs receive -u -s tank/dst", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"receiveResumable":true,
		"compression":"none"
	}`)

	err := plan.Execute(context.Background(), strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("Execute() error = nil, want pipeline failure")
	}
	waitForFile(t, pidFile)
	pidText, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidText))
	if err != nil {
		t.Fatal(err)
	}
	if processExists(pid) {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill downstream process %d: %v", pid, err)
		}
		t.Fatalf("downstream process %d is still running after Execute returned", pid)
	}
}

func TestPlanExecuteHonorsCallerCancellation(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "zfs.pid")
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$$\" > %q\nwhile :; do sleep 1; done\n", pidFile))
	replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	plan := admitTestPlan(t, "zfs receive -u -s tank/dst", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"receiveResumable":true,
		"compression":"none"
	}`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- plan.Execute(ctx, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	}()
	waitForFile(t, pidFile)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Execute() error = nil, want cancellation failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute() did not return after context cancellation")
	}
	pidText, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidText))
	if err != nil {
		t.Fatal(err)
	}
	if processExists(pid) {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill canceled process %d: %v", pid, err)
		}
		t.Fatalf("canceled process %d is still running after Execute returned", pid)
	}
}

func TestPlanExecutePreservesExitCode(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, "#!/bin/sh\nexit 42\n")
	replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	plan := admitTestPlan(t, "zfs get -H name tank/dst", `{
		"targetDataset":"tank/dst",
		"receiveUnmounted":true,
		"compression":"none"
	}`)

	err := plan.Execute(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{})
	var exitErr interface {
		error
		ExitCode() int
	}
	if !errors.As(err, &exitErr) {
		t.Fatalf("Execute() error = %v, want exit-code error", err)
	}
	if exitErr.ExitCode() != 42 {
		t.Fatalf("exit code = %d, want 42", exitErr.ExitCode())
	}
}

func writeTestPolicy(t *testing.T, dir, policy string) {
	t.Helper()
	path := filepath.Join(dir, testPolicyID+".json")
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
}

func testReference(t *testing.T) Reference {
	t.Helper()
	reference, err := ReferenceFromArgs([]string{"--policy-id", testPolicyID})
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

func admitTestPlan(t *testing.T, command, policy string) Plan {
	t.Helper()
	dir := t.TempDir()
	writeTestPolicy(t, dir, policy)
	plan, err := New(dir).Admit(testReference(t), command)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func writeScript(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}

func replaceAllowedCommandResolver(t *testing.T, paths map[string]string) {
	t.Helper()
	previous := resolveAllowedCommand
	resolveAllowedCommand = func(name string) (string, error) {
		if path, ok := paths[name]; ok {
			return path, nil
		}
		return "", errors.New("unexpected command: " + name)
	}
	t.Cleanup(func() {
		resolveAllowedCommand = previous
	})
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func assertWriterErrorUnchanged(t *testing.T, got error, want *testWriterError) {
	t.Helper()
	var writerErr *testWriterError
	if !errors.As(got, &writerErr) || writerErr != want || errors.Unwrap(got) != nil {
		t.Fatalf("Execute() error = %T %v, want original writer error", got, got)
	}
}

type testWriterError struct{}

func (*testWriterError) Error() string {
	return "write failed"
}
