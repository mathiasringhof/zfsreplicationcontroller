package receiverauthorization

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOpenSSHControlMasterAdmissionAcrossSnapshots(t *testing.T) {
	sshd := requireOpenSSHCommand(t, "sshd")
	ssh := requireOpenSSHCommand(t, "ssh")
	keygen := requireOpenSSHCommand(t, "ssh-keygen")
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listener unavailable: %v", err)
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	if closeErr := listener.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	account, err := user.Current()
	if err != nil {
		t.Skipf("local SSH account unavailable: %v", err)
	}
	// Keep the Unix control socket below OpenSSH's path length limit, including
	// on macOS, where testing.T.TempDir includes the long test name.
	dir, err := os.MkdirTemp("", "zfsrep-ssh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, name := range []string{"host", "client"} {
		if out, err := exec.CommandContext(ctx, keygen, "-q", "-t", "ed25519", "-N", "", "-f", filepath.Join(dir, name)).CombinedOutput(); err != nil {
			t.Fatalf("generate %s key: %v\n%s", name, err, out)
		}
	}
	publicKey, err := os.ReadFile(filepath.Join(dir, "client.pub"))
	if err != nil {
		t.Fatal(err)
	}
	module := New(filepath.Join(dir, "authorized_keys"))
	candidate := validCandidate(time.Now(), testTaskUID, string(publicKey))
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(dir, "admit")
	writeScript(t, helper, "#!/bin/sh\nexec "+quoteSSHShell(binary)+" -test.run='^TestOpenSSHAdmissionHelper$' -- "+quoteSSHShell(module.manifestPath)+" \"$@\"\n")
	sshManifest := filepath.Join(dir, "sshd_authorized_keys")
	updateManifest := func() {
		t.Helper()
		data, err := os.ReadFile(module.manifestPath)
		if err != nil {
			t.Fatal(err)
		}
		// Use the module's manifest verbatim apart from the executable: the
		// throwaway sshd runs this test binary instead of the installed Receiver.
		data = []byte(strings.ReplaceAll(string(data), receiverCommandPath+" exec", escapeAuthorizedKeysOption(quoteSSHShell(helper))+" exec"))
		if err := os.WriteFile(sshManifest, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	updateManifest()
	config := filepath.Join(dir, "sshd_config")
	configData := fmt.Sprintf(`ListenAddress 127.0.0.1
Port %s
HostKey %s
PidFile %s
AuthorizedKeysFile %s
AllowUsers %s
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
AuthenticationMethods publickey
UsePAM no
PermitRootLogin yes
UseDNS no
PrintMotd no
PrintLastLog no
LogLevel VERBOSE
`, port, strconv.Quote(filepath.Join(dir, "host")), strconv.Quote(filepath.Join(dir, "sshd.pid")), strconv.Quote(sshManifest), account.Username)
	if err := os.WriteFile(config, []byte(configData), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, sshd, "-t", "-f", config).CombinedOutput(); err != nil {
		t.Skipf("throwaway sshd prerequisites unavailable: %v\n%s", err, out)
	}
	serverDone, serverLog := startOpenSSHProcess(t, ctx, dir, "server", sshd, "-D", "-e", "-f", config)
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp4", address, 100*time.Millisecond)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case <-serverDone:
			t.Skipf("throwaway sshd cannot serve in this environment:\n%s", readSSHLog(t, serverLog))
		default:
		}
		if time.Now().After(deadline) {
			t.Skipf("throwaway sshd cannot bind loopback: %v\n%s", err, readSSHLog(t, serverLog))
		}
		time.Sleep(25 * time.Millisecond)
	}
	common := []string{
		"-F", "/dev/null", "-p", port, "-l", account.Username,
		"-i", filepath.Join(dir, "client"), "-S", filepath.Join(dir, "control"),
		"-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
		"-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
		"-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=5", "-o", "LogLevel=ERROR",
	}
	masterArgs := append(append([]string{}, common...), "-M", "-N", "127.0.0.1")
	masterDone, masterLog := startOpenSSHProcess(t, ctx, dir, "master", ssh, masterArgs...)
	checkMaster := func() ([]byte, error) {
		args := append(append([]string{}, common...), "-O", "check", "127.0.0.1")
		return exec.CommandContext(ctx, ssh, args...).CombinedOutput()
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, err := checkMaster(); err == nil {
			break
		}
		select {
		case <-masterDone:
			log := readSSHLog(t, masterLog)
			if strings.Contains(log, "Operation not permitted") {
				t.Skipf("SSH master blocked by sandbox: %s", log)
			}
			t.Fatalf("SSH master exited: %s\nserver: %s", log, readSSHLog(t, serverLog))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("SSH master did not become ready: %s\nserver: %s", readSSHLog(t, masterLog), readSSHLog(t, serverLog))
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, step := range []string{"initial", "unrelated grant added", "own grant revoked"} {
		other := validCandidate(time.Now(), "other-task", otherAuthorizedPublicKey)
		switch step {
		case "unrelated grant added":
			if _, err := module.Replace([]Candidate{candidate, other}); err != nil {
				t.Fatal(err)
			}
		case "own grant revoked":
			if _, err := module.Replace([]Candidate{other}); err != nil {
				t.Fatal(err)
			}
		}
		updateManifest()
		// ProxyCommand=false makes a fallback to a fresh connection fail. All
		// three commands must use the original authenticated ControlMaster.
		args := append(append([]string{}, common...), "-o", "ControlMaster=no", "-o", "ProxyCommand=false", "127.0.0.1", "echo -n admitted")
		out, err := exec.CommandContext(ctx, ssh, args...).CombinedOutput()
		if step == "own grant revoked" {
			if err == nil || !strings.Contains(string(out), "admission rejected") {
				t.Fatalf("%s: want admission rejection, got %v\n%s", step, err, out)
			}
		} else if err != nil || string(out) != "admitted" {
			t.Fatalf("%s: want admitted, got %v\n%s", step, err, out)
		}
		if out, err := checkMaster(); err != nil {
			t.Fatalf("%s: original master stopped: %v\n%s", step, err, out)
		}
	}
}

func TestOpenSSHAdmissionHelper(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "--" || len(os.Args) < i+4 || os.Args[i+2] != "exec" {
			continue
		}
		reference, err := ReferenceFromArgs(os.Args[i+3:])
		if err != nil {
			t.Fatalf("invalid forced command: %v", err)
		}
		plan, err := New(os.Args[i+1]).Admit(reference, os.Getenv("SSH_ORIGINAL_COMMAND"))
		if err != nil {
			t.Fatalf("admission rejected: %v", err)
		}
		if err := plan.Execute(context.Background(), os.Stdin, os.Stdout, os.Stderr); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	t.Skip("only invoked as an OpenSSH forced command helper")
}

func requireOpenSSHCommand(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s unavailable: %v", name, err)
	}
	return path
}

func startOpenSSHProcess(t *testing.T, ctx context.Context, dir, name, path string, args ...string) (<-chan struct{}, string) {
	t.Helper()
	logPath := filepath.Join(dir, name+".log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Stdout, cmd.Stderr = log, log
	err = cmd.Start()
	if closeErr := log.Close(); closeErr != nil {
		t.Error(closeErr)
	}
	if err != nil {
		cancel()
		t.Skipf("cannot start %s: %v", name, err)
	}
	done := make(chan struct{})
	go func() {
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			t.Logf("%s exited: %v", name, err)
		}
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return done, logPath
}

func readSSHLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func quoteSSHShell(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
