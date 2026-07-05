package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type receiverConfig struct {
	NodeName           string
	PodName            string
	PodUID             string
	PodIP              string
	WatchNamespace     string
	AuthorizedKeysFile string
	SSHPort            int32
	AllowedPrefixes    []string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) > 1 && os.Args[1] == "exec" {
		if err := runForcedCommandFromArgs(ctx, os.Args[2:]); err != nil {
			var exitErr forcedCommandExitError
			if errors.As(err, &exitErr) {
				if exitErr.Error() != "" {
					fmt.Fprintln(os.Stderr, exitErr)
				}
				os.Exit(exitErr.ExitCode())
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(126)
		}
		return
	}

	if err := run(ctx, configFromEnv()); err != nil {
		log.Fatal(err)
	}
}

func configFromEnv() receiverConfig {
	return receiverConfig{
		NodeName:           os.Getenv("NODE_NAME"),
		PodName:            os.Getenv("POD_NAME"),
		PodUID:             os.Getenv("POD_UID"),
		PodIP:              os.Getenv("POD_IP"),
		WatchNamespace:     os.Getenv("WATCH_NAMESPACE"),
		AuthorizedKeysFile: getenv("SSH_AUTHORIZED_KEYS_FILE", "/run/zfs-receiver/authorized_keys"),
		SSHPort:            int32Env("SSH_LISTEN_PORT", 2222),
		AllowedPrefixes:    listCSVEnv("ZFS_RECEIVER_ALLOWED_DATASET_PREFIXES"),
	}
}

func run(ctx context.Context, cfg receiverConfig) error {
	if cfg.NodeName == "" {
		return fmt.Errorf("NODE_NAME must not be empty")
	}
	if cfg.PodIP == "" {
		return fmt.Errorf("POD_IP must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.AuthorizedKeysFile), 0o700); err != nil {
		return fmt.Errorf("create authorized_keys directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(cfg.AuthorizedKeysFile), 0o700); err != nil {
		return fmt.Errorf("set authorized_keys directory mode: %w", err)
	}
	if err := os.MkdirAll(receiverPolicyDir(cfg), 0o700); err != nil {
		return fmt.Errorf("create receiver policy directory: %w", err)
	}
	if err := os.Chmod(receiverPolicyDir(cfg), 0o700); err != nil {
		return fmt.Errorf("set receiver policy directory mode: %w", err)
	}
	if err := os.MkdirAll("/run/sshd", 0o755); err != nil {
		return fmt.Errorf("create sshd runtime directory: %w", err)
	}
	if err := writeAuthorizedKeys(cfg.AuthorizedKeysFile, nil); err != nil {
		return err
	}
	if err := ensureSSHHostKeys(ctx); err != nil {
		return err
	}
	hostKey, err := readSSHHostKey()
	if err != nil {
		return err
	}
	sshdDone, err := startSSHD(ctx, cfg)
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := zfsv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return err
	}
	kubeClient, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := reconcileReceiveTasks(ctx, kubeClient, cfg, hostKey); err != nil {
			log.Printf("reconcile receive tasks: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-sshdDone:
			if err == nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("sshd exited: %w", err)
		case <-ticker.C:
		}
	}
}

func startSSHD(ctx context.Context, cfg receiverConfig) (<-chan error, error) {
	configPath := filepath.Join(filepath.Dir(cfg.AuthorizedKeysFile), "sshd_config")
	config := renderSSHDConfig(cfg)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return nil, fmt.Errorf("write sshd config: %w", err)
	}

	cmd := exec.CommandContext(ctx, "/usr/sbin/sshd", "-D", "-e", "-f", configPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start sshd: %w", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()
	return done, nil
}

func renderSSHDConfig(cfg receiverConfig) string {
	return fmt.Sprintf(`Port %d
ListenAddress 0.0.0.0
PermitRootLogin prohibit-password
AllowUsers zfs-recv
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
PidFile /run/zfs-receiver/sshd.pid
AuthorizedKeysFile %s
PermitTTY no
X11Forwarding no
AllowAgentForwarding no
AllowTcpForwarding no
PermitTunnel no
PermitUserEnvironment no
LogLevel VERBOSE
Subsystem sftp internal-sftp
`, cfg.SSHPort, cfg.AuthorizedKeysFile)
}

func reconcileReceiveTasks(ctx context.Context, kubeClient client.Client, cfg receiverConfig, hostKey string) error {
	var tasks zfsv1.ZFSReceiveTaskList
	var opts []client.ListOption
	if cfg.WatchNamespace != "" {
		opts = append(opts, client.InNamespace(cfg.WatchNamespace))
	}
	if err := kubeClient.List(ctx, &tasks, opts...); err != nil {
		return err
	}

	now := time.Now()
	activeKeys := map[string]struct{}{}
	activePolicies := map[string]receiverCommandPolicy{}
	var readyTasks []*zfsv1.ZFSReceiveTask
	var reconcileErrs []error
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if task.Spec.NodeName != cfg.NodeName {
			continue
		}
		if task.Status.Phase.Terminal() {
			continue
		}
		if task.Spec.SSH.ExpiresAt.Time.Before(now) {
			if err := patchTaskFailed(ctx, kubeClient, task, "receive task expired"); err != nil {
				reconcileErrs = append(reconcileErrs, err)
			}
			continue
		}
		if !datasetAllowed(task.Spec.Destination.Dataset, cfg.AllowedPrefixes) {
			if err := patchTaskFailed(ctx, kubeClient, task, "destination dataset is not allowed on this receiver"); err != nil {
				reconcileErrs = append(reconcileErrs, err)
			}
			continue
		}
		auth, err := receiveTaskAuthorization(cfg, task)
		if err != nil {
			if patchErr := patchTaskFailed(ctx, kubeClient, task, err.Error()); patchErr != nil {
				reconcileErrs = append(reconcileErrs, patchErr)
			}
			continue
		}
		activeKeys[auth.AuthorizedKey] = struct{}{}
		activePolicies[auth.PolicyID] = auth.Policy
		readyTasks = append(readyTasks, task)
	}

	if err := writeReceiverPolicies(receiverPolicyDir(cfg), activePolicies); err != nil {
		return err
	}
	keys := make([]string, 0, len(activeKeys))
	for key := range activeKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if err := writeAuthorizedKeys(cfg.AuthorizedKeysFile, keys); err != nil {
		return err
	}
	for _, task := range readyTasks {
		if err := patchTaskReady(ctx, kubeClient, task, cfg, hostKey); err != nil {
			reconcileErrs = append(reconcileErrs, err)
		}
	}
	return errors.Join(reconcileErrs...)
}

func patchTaskReady(ctx context.Context, kubeClient client.Client, task *zfsv1.ZFSReceiveTask, cfg receiverConfig, hostKey string) error {
	latest, ok, err := latestNonTerminalTask(ctx, kubeClient, task)
	if err != nil || !ok {
		return err
	}
	task = latest
	if task.Status.Phase == zfsv1.ReceiveTaskPhaseReady &&
		task.Status.Endpoint.Host == cfg.PodIP &&
		task.Status.Endpoint.Port == cfg.SSHPort &&
		task.Status.SSH.HostKey == hostKey &&
		task.Status.ReceiverPod.Name == cfg.PodName &&
		task.Status.ReceiverPod.UID == cfg.PodUID &&
		task.Status.Error == "" {
		return nil
	}
	copy := task.DeepCopy()
	copy.Status.Phase = zfsv1.ReceiveTaskPhaseReady
	copy.Status.Endpoint = zfsv1.ReceiveTaskEndpoint{Host: cfg.PodIP, Port: cfg.SSHPort}
	copy.Status.SSH = zfsv1.ReceiveTaskSSHStatus{HostKey: hostKey}
	copy.Status.ReceiverPod = zfsv1.ReceiveTaskPodStatus{Name: cfg.PodName, UID: cfg.PodUID}
	copy.Status.Error = ""
	return kubeClient.Status().Patch(ctx, copy, client.MergeFrom(task))
}

func patchTaskFailed(ctx context.Context, kubeClient client.Client, task *zfsv1.ZFSReceiveTask, msg string) error {
	latest, ok, err := latestNonTerminalTask(ctx, kubeClient, task)
	if err != nil || !ok {
		return err
	}
	task = latest
	if task.Status.Phase == zfsv1.ReceiveTaskPhaseFailed && task.Status.Error == msg {
		return nil
	}
	copy := task.DeepCopy()
	copy.Status.Phase = zfsv1.ReceiveTaskPhaseFailed
	copy.Status.Error = msg
	return kubeClient.Status().Patch(ctx, copy, client.MergeFrom(task))
}

func latestNonTerminalTask(ctx context.Context, kubeClient client.Client, task *zfsv1.ZFSReceiveTask) (*zfsv1.ZFSReceiveTask, bool, error) {
	var latest zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(task), &latest); err != nil {
		return nil, false, err
	}
	if latest.Status.Phase.Terminal() {
		return &latest, false, nil
	}
	return &latest, true, nil
}

func writeAuthorizedKeys(path string, keys []string) error {
	tmp := path + ".tmp"
	var content strings.Builder
	for _, key := range keys {
		content.WriteString(key)
		content.WriteByte('\n')
	}
	if err := os.WriteFile(tmp, []byte(content.String()), 0o600); err != nil {
		return fmt.Errorf("write authorized_keys: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace authorized_keys: %w", err)
	}
	return nil
}

func ensureSSHHostKeys(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "ssh-keygen", "-A")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("generate ssh host keys: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func readSSHHostKey() (string, error) {
	for _, path := range []string{
		"/etc/ssh/ssh_host_ed25519_key.pub",
		"/etc/ssh/ssh_host_ecdsa_key.pub",
		"/etc/ssh/ssh_host_rsa_key.pub",
	} {
		data, err := os.ReadFile(path)
		if err == nil {
			key := strings.TrimSpace(string(data))
			if key != "" {
				return key, nil
			}
		}
	}
	return "", fmt.Errorf("no ssh host public key found")
}

func datasetAllowed(dataset string, prefixes []string) bool {
	if dataset == "" {
		return false
	}
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if dataset == prefix || strings.HasPrefix(dataset, prefix+"/") {
			return true
		}
	}
	return false
}

func getenv(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func int32Env(key string, def int32) int32 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 {
		return def
	}
	return int32(parsed)
}

func listCSVEnv(key string) []string {
	var out []string
	for _, item := range strings.Split(os.Getenv(key), ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
