package datamover

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type call struct {
	name string
	args []string
}

type fakeRunner struct {
	calls        []call
	snapshots    map[string]bool
	mounted      string
	failSnapshot bool
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
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
		return "123\n", "", nil
	}
	return "", "", nil
}

func (f *fakeRunner) StartPipe(_ context.Context, name string, args ...string) (io.ReadCloser, <-chan error, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	done := make(chan error, 1)
	done <- nil
	close(done)
	return io.NopCloser(strings.NewReader("stream")), done, nil
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

func TestSenderUsesIncrementalWhenBaseExists(t *testing.T) {
	tokenFile := writeToken(t)
	var headers http.Header
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		headers = r.Header.Clone()
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
	want := "send -i tank/src@zsync-run-0 tank/src@zsync-run-1"
	if got := strings.Join(runner.calls[2].args, " "); got != want {
		t.Fatalf("send args = %q, want %q", got, want)
	}
	if headers.Get("Authorization") != "Bearer secret-token" || headers.Get("X-ZFSRep-Mode") != "incremental" {
		t.Fatalf("missing required headers: %#v", headers)
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
	runner := &fakeRunner{snapshots: map[string]bool{"tank/dst@snap-1": true}, mounted: "no\n"}
	receiver := newTestReceiver(t, runner)
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("stream"))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-ZFSRep-Run-ID", "run-1")
	req.Header.Set("X-ZFSRep-Snapshot", "snap-1")
	req.Header.Set("X-ZFSRep-Mode", "incremental")
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

func TestReceiverRejectsMountedTarget(t *testing.T) {
	receiver := newTestReceiver(t, &fakeRunner{snapshots: map[string]bool{}, mounted: "yes\n"})
	req := httptest.NewRequest(http.MethodPost, "/receive", strings.NewReader("stream"))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-ZFSRep-Run-ID", "run-1")
	req.Header.Set("X-ZFSRep-Snapshot", "snap-1")
	req.Header.Set("X-ZFSRep-Mode", "incremental")
	rr := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d", rr.Code)
	}
}

func newTestReceiver(t *testing.T, runner CommandRunner) *Receiver {
	t.Helper()
	receiver, err := NewReceiver(ReceiverConfig{
		RunID: "run-1", SnapshotName: "snap-1", DstDataset: "tank/dst", TokenFile: writeToken(t),
		BootstrapMode: "FailIfNoBase", ReceiveUnmounted: true, ReceiveResumable: true,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	return receiver
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
