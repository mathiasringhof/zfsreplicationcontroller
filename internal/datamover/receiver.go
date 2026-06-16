package datamover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

type ReceiverConfig struct {
	RunID            string
	SnapshotName     string
	DstDataset       string
	TokenFile        string
	BootstrapMode    string
	ReceiveUnmounted bool
	ReceiveResumable bool
	ListenAddr       string
	ExpectedNode     string
	ActualNode       string
}

type Receiver struct {
	cfg     ReceiverConfig
	runner  CommandRunner
	token   string
	mu      sync.Mutex
	started bool
}

func ReceiverConfigFromEnv() ReceiverConfig {
	return ReceiverConfig{
		RunID:            os.Getenv("RUN_ID"),
		SnapshotName:     os.Getenv("SNAPSHOT_NAME"),
		DstDataset:       os.Getenv("DST_DATASET"),
		TokenFile:        os.Getenv("TOKEN_FILE"),
		BootstrapMode:    getenv("BOOTSTRAP_MODE", "FailIfNoBase"),
		ReceiveUnmounted: getenv("RECEIVE_UNMOUNTED", "true") == "true",
		ReceiveResumable: getenv("RECEIVE_RESUMABLE", "true") == "true",
		ListenAddr:       getenv("LISTEN_ADDR", ":8080"),
		ExpectedNode:     os.Getenv("EXPECTED_NODE_NAME"),
		ActualNode:       os.Getenv("ACTUAL_NODE_NAME"),
	}
}

func NewReceiver(cfg ReceiverConfig, runner CommandRunner) (*Receiver, error) {
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return nil, err
	}
	tokenBytes, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	if cfg.BootstrapMode == "" {
		cfg.BootstrapMode = "FailIfNoBase"
	}
	return &Receiver{cfg: cfg, runner: runner, token: strings.TrimSpace(string(tokenBytes))}, nil
}

func (r *Receiver) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/receive", r.receive)
	return mux
}

func (r *Receiver) Serve(ctx context.Context) error {
	server := &http.Server{Addr: r.cfg.ListenAddr, Handler: r.Handler()}
	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("shutdown receiver server: %v", err)
		}
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (r *Receiver) receive(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if req.Header.Get("Authorization") != "Bearer "+r.token {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	if req.Header.Get("X-ZFSRep-Run-ID") != r.cfg.RunID {
		http.Error(w, "wrong run ID", http.StatusBadRequest)
		return
	}
	if req.Header.Get("X-ZFSRep-Snapshot") != r.cfg.SnapshotName {
		http.Error(w, "wrong snapshot name", http.StatusBadRequest)
		return
	}

	mode := req.Header.Get("X-ZFSRep-Mode")
	if mode != "full" && mode != "incremental" {
		http.Error(w, "invalid receive mode", http.StatusBadRequest)
		return
	}
	if mode == "full" && r.cfg.BootstrapMode != BootstrapDestroyTargetAndReceiveFull {
		http.Error(w, "full receive requires destructive bootstrap", http.StatusConflict)
		return
	}
	mounted, stderr, err := r.runner.Run(req.Context(), "zfs", "get", "-H", "-o", "value", "mounted", r.cfg.DstDataset)
	if err != nil {
		http.Error(w, "target mounted check failed: "+clean(stderr, err), http.StatusInternalServerError)
		return
	}
	if strings.TrimSpace(mounted) == "yes" {
		http.Error(w, "target dataset mounted", http.StatusConflict)
		return
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		http.Error(w, "receive already attempted", http.StatusConflict)
		return
	}
	r.started = true
	r.mu.Unlock()

	if mode == "full" && r.cfg.BootstrapMode == BootstrapDestroyTargetAndReceiveFull {
		_, stderr, err := r.runner.Run(req.Context(), "zfs", "destroy", "-r", r.cfg.DstDataset)
		if err != nil && !strings.Contains(stderr, "dataset does not exist") {
			http.Error(w, "zfs destroy failed: "+clean(stderr, err), http.StatusInternalServerError)
			return
		}
	}

	args := []string{"receive"}
	if r.cfg.ReceiveUnmounted {
		args = append(args, "-u")
	}
	if r.cfg.ReceiveResumable {
		args = append(args, "-s")
	}
	args = append(args, r.cfg.DstDataset)
	if _, stderr, err := r.runner.RunWithStdin(req.Context(), req.Body, "zfs", args...); err != nil {
		http.Error(w, "zfs receive failed: "+clean(stderr, err), http.StatusInternalServerError)
		return
	}
	snap := r.cfg.DstDataset + "@" + r.cfg.SnapshotName
	if _, stderr, err := r.runner.Run(req.Context(), "zfs", "list", "-H", "-t", "snapshot", snap); err != nil {
		http.Error(w, "target snapshot missing after receive: "+clean(stderr, err), http.StatusInternalServerError)
		return
	}
	if _, err := io.WriteString(w, "ok\n"); err != nil {
		return
	}
}
