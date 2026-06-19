package datamover

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type call struct {
	name string
	args []string
}

type fakeRunner struct {
	calls         []call
	snapshots     map[string]bool
	guids         map[string]string
	mounted       string
	failSnapshot  bool
	destroyStderr string
	destroyErr    error
	pipeErr       error
	pipeCloseErr  error
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	if strings.Join(args, " ") == "destroy -r tank/dst" {
		return "", f.destroyStderr, f.destroyErr
	}
	if strings.Join(args, " ") == "get -H -o value mounted tank/dst" {
		return f.mounted, "", nil
	}
	if len(args) >= 4 && args[0] == "list" && args[3] != "" {
		if f.snapshots[args[len(args)-1]] {
			return args[len(args)-1], "", nil
		}
		return "", "not found", errFake
	}
	if args[0] == "snapshot" {
		if f.failSnapshot {
			return "", "snapshot failed", errFake
		}
		f.snapshots[args[1]] = true
		return "", "", nil
	}
	if args[0] == "get" && args[4] == "guid" {
		snap := args[5]
		if !f.snapshots[snap] {
			return "", "not found", errFake
		}
		if guid := f.guids[snap]; guid != "" {
			return guid + "\n", "", nil
		}
		return "123\n", "", nil
	}
	return "", "", nil
}

func (f *fakeRunner) StartPipe(_ context.Context, name string, args ...string) (io.ReadCloser, <-chan error, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	done := make(chan error, 1)
	done <- f.pipeErr
	close(done)
	body := io.ReadCloser(io.NopCloser(strings.NewReader("stream")))
	if f.pipeCloseErr != nil {
		body = closeErrReadCloser{Reader: strings.NewReader("stream"), err: f.pipeCloseErr}
	}
	return body, done, nil
}

func (f *fakeRunner) RunWithStdin(_ context.Context, stdin io.Reader, name string, args ...string) (string, string, error) {
	if _, err := io.Copy(io.Discard, stdin); err != nil {
		return "", "", err
	}
	f.calls = append(f.calls, call{name: name, args: args})
	return "", "", nil
}

type fakeErr struct{}

func (fakeErr) Error() string { return "fake error" }

var errFake error = fakeErr{}

type closeErrReadCloser struct {
	*strings.Reader
	err error
}

func (c closeErrReadCloser) Close() error {
	return c.err
}

func TestSenderUsesIncrementalWhenBaseExists(t *testing.T) {
	tokenFile := writeToken(t)
	var headers http.Header
	var gotURL string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		headers = r.Header.Clone()
		gotURL = r.URL.String()
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     http.Header{},
		}, nil
	})}

	runner := &fakeRunner{snapshots: map[string]bool{
		"tank/src@zsync-run-1": true,
		"tank/src@zsync-run-0": true,
	}}
	guid, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src", BaseSnapshot: "zsync-run-0",
		ReceiverURL: "http://receiver/receive", TokenFile: tokenFile, BootstrapMode: "FailIfNoBase",
	}, runner, client)
	if err != nil {
		t.Fatal(err)
	}
	if guid != "123" {
		t.Fatalf("guid = %q", guid)
	}
	if gotURL != "http://receiver/receive" {
		t.Fatalf("receiver URL = %q", gotURL)
	}
	if !hasCall(runner.calls, "send -i tank/src@zsync-run-0 tank/src@zsync-run-1") {
		t.Fatalf("zfs send -i was not called: %#v", runner.calls)
	}
	if headers.Get("Authorization") != "Bearer secret-token" || headers.Get("X-ZFSRep-Mode") != "incremental" {
		t.Fatalf("missing required headers: %#v", headers)
	}
	if headers.Get("X-ZFSRep-Base-Snapshot") != "zsync-run-0" {
		t.Fatalf("base snapshot header = %q, want zsync-run-0", headers.Get("X-ZFSRep-Base-Snapshot"))
	}
	if headers.Get("X-ZFSRep-Base-GUID") != "123" {
		t.Fatalf("base GUID header = %q, want 123", headers.Get("X-ZFSRep-Base-GUID"))
	}
}

func TestSenderConfigFromEnvDefaultsSnapshotPrefix(t *testing.T) {
	t.Setenv("SNAPSHOT_PREFIX", "")
	cfg := SenderConfigFromEnv()
	if cfg.SnapshotPrefix != "zsync" {
		t.Fatalf("SnapshotPrefix = %q, want zsync", cfg.SnapshotPrefix)
	}
}

func TestReceiverConfigFromEnvDefaults(t *testing.T) {
	for _, key := range []string{"BOOTSTRAP_MODE", "RECEIVE_UNMOUNTED", "RECEIVE_RESUMABLE", "LISTEN_ADDR"} {
		t.Setenv(key, "")
	}
	cfg := ReceiverConfigFromEnv()
	if cfg.BootstrapMode != "FailIfNoBase" {
		t.Fatalf("BootstrapMode = %q, want FailIfNoBase", cfg.BootstrapMode)
	}
	if !cfg.ReceiveUnmounted {
		t.Fatalf("ReceiveUnmounted = false, want true")
	}
	if !cfg.ReceiveResumable {
		t.Fatalf("ReceiveResumable = false, want true")
	}
	if cfg.ListenAddr != ":8080" {
		t.Fatalf("ListenAddr = %q, want :8080", cfg.ListenAddr)
	}
}

func TestExecRunnerMirrorsSuccessfulStderr(t *testing.T) {
	oldStderr := os.Stderr
	readStderr, writeStderr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Stderr = oldStderr
		if err := readStderr.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close stderr pipe reader: %v", err)
		}
	})
	os.Stderr = writeStderr

	stdout, stderr, err := ExecRunner{}.Run(context.Background(), "sh", "-c", "printf stdout; printf stderr >&2")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStderr.Close(); err != nil {
		t.Fatal(err)
	}
	mirrored, err := io.ReadAll(readStderr)
	if err != nil {
		t.Fatal(err)
	}

	if stdout != "stdout" {
		t.Fatalf("stdout = %q", stdout)
	}
	if stderr != "stderr" {
		t.Fatalf("stderr = %q", stderr)
	}
	if string(mirrored) != "stderr" {
		t.Fatalf("mirrored stderr = %q", string(mirrored))
	}
}

func TestConfigFromEnvExplicitValuesOverrideDefaults(t *testing.T) {
	t.Setenv("SNAPSHOT_PREFIX", "nightly")
	t.Setenv("BOOTSTRAP_MODE", BootstrapDestroyTargetAndReceiveFull)
	t.Setenv("RECEIVE_UNMOUNTED", "false")
	t.Setenv("RECEIVE_RESUMABLE", "false")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:9090")

	sender := SenderConfigFromEnv()
	if sender.SnapshotPrefix != "nightly" {
		t.Fatalf("sender SnapshotPrefix = %q, want nightly", sender.SnapshotPrefix)
	}
	receiver := ReceiverConfigFromEnv()
	if receiver.BootstrapMode != BootstrapDestroyTargetAndReceiveFull {
		t.Fatalf("receiver BootstrapMode = %q, want %s", receiver.BootstrapMode, BootstrapDestroyTargetAndReceiveFull)
	}
	if receiver.ReceiveUnmounted {
		t.Fatalf("ReceiveUnmounted = true, want false")
	}
	if receiver.ReceiveResumable {
		t.Fatalf("ReceiveResumable = true, want false")
	}
	if receiver.ListenAddr != "127.0.0.1:9090" {
		t.Fatalf("ListenAddr = %q, want 127.0.0.1:9090", receiver.ListenAddr)
	}
}

func TestSenderExitsBeforeWorkWhenNodeMismatch(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		ExpectedNode: "worker-a", ActualNode: "worker-b", TokenFile: "/does/not/matter",
	}, runner, nil)
	if err == nil || !strings.Contains(err.Error(), "node verification failed") {
		t.Fatalf("error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("zfs calls = %#v", runner.calls)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestSenderRefusesFullWhenBootstrapDisabled(t *testing.T) {
	tokenFile := writeToken(t)
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src",
		ReceiverURL: "http://example.invalid", TokenFile: tokenFile, BootstrapMode: "FailIfNoBase",
	}, runner, nil)
	if err == nil || !strings.Contains(err.Error(), "no base snapshot") {
		t.Fatalf("error = %v", err)
	}
}

func TestSenderHTTPNon2xxIncludesReceiverStatusAndBody(t *testing.T) {
	tokenFile := writeToken(t)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusTeapot,
			Status:     "418 I'm a teapot",
			Body:       io.NopCloser(strings.NewReader("short and stout")),
			Header:     http.Header{},
		}, nil
	})}
	runner := &fakeRunner{snapshots: map[string]bool{
		"tank/src@zsync-run-1": true,
		"tank/src@zsync-run-0": true,
	}}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src", BaseSnapshot: "zsync-run-0",
		ReceiverURL: "http://receiver/receive", TokenFile: tokenFile, BootstrapMode: "FailIfNoBase",
	}, runner, client)
	if err == nil || !strings.Contains(err.Error(), "HTTP stream failed") || !strings.Contains(err.Error(), "418 I'm a teapot") || !strings.Contains(err.Error(), "short and stout") {
		t.Fatalf("error = %v", err)
	}
}

func TestSenderHTTPClientErrorIsWrappedAsStreamFailure(t *testing.T) {
	tokenFile := writeToken(t)
	clientErr := errors.New("connection reset")
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return nil, clientErr
	})}
	runner := &fakeRunner{snapshots: map[string]bool{
		"tank/src@zsync-run-1": true,
		"tank/src@zsync-run-0": true,
	}}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src", BaseSnapshot: "zsync-run-0",
		ReceiverURL: "http://receiver/receive", TokenFile: tokenFile, BootstrapMode: "FailIfNoBase",
	}, runner, client)
	if err == nil || !strings.Contains(err.Error(), "HTTP stream failed") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error = %v", err)
	}
}

func TestSenderSurfacesSendPipeCompletionErrorAfterHTTPRequest(t *testing.T) {
	tokenFile := writeToken(t)
	requested := false
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requested = true
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     http.Header{},
		}, nil
	})}
	runner := &fakeRunner{
		snapshots: map[string]bool{
			"tank/src@zsync-run-1": true,
			"tank/src@zsync-run-0": true,
		},
		pipeErr: errors.New("send exited 1"),
	}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src", BaseSnapshot: "zsync-run-0",
		ReceiverURL: "http://receiver/receive", TokenFile: tokenFile, BootstrapMode: "FailIfNoBase",
	}, runner, client)
	if !requested {
		t.Fatalf("HTTP request was not attempted")
	}
	if err == nil || !strings.Contains(err.Error(), "zfs send failed") || !strings.Contains(err.Error(), "send exited 1") {
		t.Fatalf("error = %v", err)
	}
}

func TestSenderIgnoresAlreadyClosedSendPipe(t *testing.T) {
	tokenFile := writeToken(t)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     http.Header{},
		}, nil
	})}
	runner := &fakeRunner{
		snapshots: map[string]bool{
			"tank/src@zsync-run-1": true,
		},
		pipeCloseErr: os.ErrClosed,
	}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID: "run-1", SnapshotPrefix: "zsync", SrcDataset: "tank/src",
		ReceiverURL: "http://receiver/receive", TokenFile: tokenFile, BootstrapMode: BootstrapDestroyTargetAndReceiveFull,
	}, runner, client)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestReceiverRejectsInvalidToken(t *testing.T) {
	receiver := newTestReceiver(t, &fakeRunner{snapshots: map[string]bool{}, mounted: "no\n"})
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("stream"))
	req.Header.Set("Authorization", "Bearer wrong")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestReceiverRunsReceiveAndVerifiesSnapshot(t *testing.T) {
	runner := &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"}
	receiver := newTestReceiver(t, runner)
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("stream"))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-ZFSRep-Run-ID", "run-1")
	req.Header.Set("X-ZFSRep-Snapshot", "snap-1")
	req.Header.Set("X-ZFSRep-Mode", "incremental")
	req.Header.Set("X-ZFSRep-Base-Snapshot", "base-0")
	req.Header.Set("X-ZFSRep-Base-GUID", "123")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	found := false
	for _, c := range runner.calls {
		if strings.Join(c.args, " ") == "receive -u -s tank/dst" {
			found = true
		}
	}
	if !found {
		t.Fatalf("zfs receive -u -s was not called: %#v", runner.calls)
	}
}

func TestReceiverSignalsCompletionAfterSuccessfulReceive(t *testing.T) {
	runner := &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"}
	receiver := newTestReceiver(t, runner)
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, validReceiveRequest())
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}

	select {
	case <-receiver.completed:
	case <-time.After(time.Second):
		t.Fatalf("receiver did not signal completion after successful receive")
	}
}

func TestReceiverExitsBeforeListeningWhenNodeMismatch(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}, mounted: "no\n"}
	receiver, err := NewReceiver(ReceiverConfig{
		ExpectedNode: "worker-b", ActualNode: "worker-a", TokenFile: "/does/not/matter",
	}, runner)
	if err == nil || !strings.Contains(err.Error(), "node verification failed") {
		t.Fatalf("error = %v receiver=%v", err, receiver)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("zfs calls = %#v", runner.calls)
	}
}

func TestReceiverRequiresRunSnapshotAndModeHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
	}{
		{name: "run ID", header: "X-ZFSRep-Run-ID", value: "wrong"},
		{name: "snapshot", header: "X-ZFSRep-Snapshot", value: "wrong"},
		{name: "mode", header: "X-ZFSRep-Mode", value: "sideways"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver := newTestReceiver(t, &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"})
			req := validReceiveRequest()
			req.Header.Set(tt.header, tt.value)
			rr := httptest.NewRecorder()
			receiver.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestReceiverRejectsMountedTarget(t *testing.T) {
	receiver := newTestReceiver(t, &fakeRunner{snapshots: receiverSnapshots(), mounted: "yes\n"})
	req := validReceiveRequest()
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestReceiverFullReceiveDestroysTargetBeforeReceive(t *testing.T) {
	runner := &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"}
	receiver := newTestReceiverWithConfig(t, runner, ReceiverConfig{
		RunID: "run-1", SnapshotName: "snap-1", DstDataset: "tank/dst", TokenFile: writeToken(t),
		BootstrapMode: BootstrapDestroyTargetAndReceiveFull, ReceiveUnmounted: true, ReceiveResumable: true,
	})
	req := validReceiveRequest()
	req.Header.Set("X-ZFSRep-Mode", "full")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	destroyIndex := callIndex(runner.calls, "destroy -r tank/dst")
	receiveIndex := callIndex(runner.calls, "receive -u -s tank/dst")
	if destroyIndex == -1 || receiveIndex == -1 {
		t.Fatalf("destroy/receive calls missing: %#v", runner.calls)
	}
	if destroyIndex > receiveIndex {
		t.Fatalf("destroy ran after receive: %#v", runner.calls)
	}
}

func TestReceiverFullReceiveContinuesWhenDestroyFindsNoDataset(t *testing.T) {
	runner := &fakeRunner{
		snapshots:     receiverSnapshots(),
		mounted:       "no\n",
		destroyStderr: "cannot open 'tank/dst': dataset does not exist",
		destroyErr:    errFake,
	}
	receiver := newTestReceiverWithConfig(t, runner, ReceiverConfig{
		RunID: "run-1", SnapshotName: "snap-1", DstDataset: "tank/dst", TokenFile: writeToken(t),
		BootstrapMode: BootstrapDestroyTargetAndReceiveFull, ReceiveUnmounted: true, ReceiveResumable: true,
	})
	req := validReceiveRequest()
	req.Header.Set("X-ZFSRep-Mode", "full")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !hasCall(runner.calls, "receive -u -s tank/dst") {
		t.Fatalf("receive was not called: %#v", runner.calls)
	}
}

func TestReceiverRejectsFullReceiveWhenBootstrapDisabled(t *testing.T) {
	runner := &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"}
	receiver := newTestReceiver(t, runner)
	req := validReceiveRequest()
	req.Header.Set("X-ZFSRep-Mode", "full")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if hasCall(runner.calls, "destroy -r tank/dst") || hasCall(runner.calls, "receive -u -s tank/dst") {
		t.Fatalf("unexpected destructive calls: %#v", runner.calls)
	}
}

func TestReceiverAllowsOnlySingleReceiveAttempt(t *testing.T) {
	runner := &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"}
	receiver := newTestReceiver(t, runner)
	first := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(first, validReceiveRequest())
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d body=%s", first.Code, first.Body.String())
	}
	second := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(second, validReceiveRequest())
	if second.Code != http.StatusConflict {
		t.Fatalf("second status = %d body=%s", second.Code, second.Body.String())
	}
	if got := countCalls(runner.calls, "receive -u -s tank/dst"); got != 1 {
		t.Fatalf("receive calls = %d, want 1: %#v", got, runner.calls)
	}
}

func TestReceiverRequiresIncrementalBaseHeaders(t *testing.T) {
	receiver := newTestReceiver(t, &fakeRunner{snapshots: receiverSnapshots(), mounted: "no\n"})
	req := validReceiveRequest()
	req.Header.Del("X-ZFSRep-Base-Snapshot")
	req.Header.Del("X-ZFSRep-Base-GUID")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "missing incremental base") {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestReceiverRejectsIncrementalWhenTargetBaseGUIDDiffers(t *testing.T) {
	runner := &fakeRunner{
		snapshots: receiverSnapshots(),
		guids:     map[string]string{"tank/dst@base-0": "target-guid"},
		mounted:   "no\n",
	}
	receiver := newTestReceiver(t, runner)
	req := validReceiveRequest()
	req.Header.Set("X-ZFSRep-Base-GUID", "source-guid")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "target base snapshot guid mismatch") {
		t.Fatalf("body = %q", rr.Body.String())
	}
	if hasCall(runner.calls, "receive -u -s tank/dst") {
		t.Fatalf("receive should not run after base GUID mismatch: %#v", runner.calls)
	}
}

func newTestReceiver(t *testing.T, runner CommandRunner) *Receiver {
	t.Helper()
	return newTestReceiverWithConfig(t, runner, ReceiverConfig{
		RunID: "run-1", SnapshotName: "snap-1", DstDataset: "tank/dst", TokenFile: writeToken(t),
		BootstrapMode: "FailIfNoBase", ReceiveUnmounted: true, ReceiveResumable: true,
	})
}

func newTestReceiverWithConfig(t *testing.T, runner CommandRunner, cfg ReceiverConfig) *Receiver {
	t.Helper()
	receiver, err := NewReceiver(cfg, runner)
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

func validReceiveRequest() *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("stream"))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-ZFSRep-Run-ID", "run-1")
	req.Header.Set("X-ZFSRep-Snapshot", "snap-1")
	req.Header.Set("X-ZFSRep-Mode", "incremental")
	req.Header.Set("X-ZFSRep-Base-Snapshot", "base-0")
	req.Header.Set("X-ZFSRep-Base-GUID", "123")
	return req
}

func receiverSnapshots() map[string]bool {
	return map[string]bool{
		"tank/dst@base-0": true,
		"tank/dst@snap-1": true,
	}
}

func writeToken(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("secret-token\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func hasCall(calls []call, args string) bool {
	return callIndex(calls, args) != -1
}

func callIndex(calls []call, args string) int {
	for i, c := range calls {
		if c.name == "zfs" && strings.Join(c.args, " ") == args {
			return i
		}
	}
	return -1
}

func countCalls(calls []call, args string) int {
	count := 0
	for _, c := range calls {
		if c.name == "zfs" && strings.Join(c.args, " ") == args {
			count++
		}
	}
	return count
}
