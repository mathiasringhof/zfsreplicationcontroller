# VM E2E Environment

This directory contains scripts for a full Kubernetes environment with real
controller, sender, receiver Jobs, pinned upstream `syncoid`, and real ZFS
pools in the Lima workers. The suite exercises the same backend path the
controller uses in production.

## Requirements

- `limactl` 2.x
- `docker` or `podman`, or allow `build-image.sh` to install/use `buildah`
  inside the control-plane VM after `up.sh`
- `kubectl`
- Internet access from the VMs for Ubuntu and k3s installation

The scripts use Lima's `lima:user-v2` network by default, which supports
guest-to-guest k3s traffic without `socket_vmnet`. To use a different network,
set `E2E_LIMA_NETWORK`, for example `E2E_LIMA_NETWORK=lima:shared`.

## One Command Setup

```sh
./test/e2e/doctor.sh
./test/e2e/run.sh
```

This creates three VMs:

- `zrc-e2e-cp`
- `worker-a`
- `worker-b`

It installs k3s, builds the e2e image, imports it into every node, deploys the
controller, and prints the generated kubeconfig path.

```sh
go test ./test/e2e -run TestE2E -count=1 -v
```

The setup installs `zfsutils-linux` on the worker VMs, loads the ZFS kernel
module, and creates a file-backed `tank` pool on each worker under
`/var/lib/zfs-real`. Override the defaults with `E2E_REAL_ZFS_POOL`,
`E2E_REAL_ZFS_ROOT`, or `E2E_REAL_ZFS_SIZE`.

## Individual Steps

```sh
./test/e2e/up.sh
./test/e2e/build-image.sh
./test/e2e/import-image.sh
./test/e2e/deploy.sh
./test/e2e/status.sh
```

The kubeconfig is written to:

```text
test/e2e/.artifacts/kubeconfig
```

## Sample Run

```sh
KUBECONFIG=test/e2e/.artifacts/kubeconfig kubectl create namespace storage
KUBECONFIG=test/e2e/.artifacts/kubeconfig kubectl apply -f test/e2e/manifests/samples/full-bootstrap.yaml
```

Collect Kubernetes and real ZFS state with:

```sh
./test/e2e/collect.sh
```

## Teardown

Delete the VMs:

```sh
./test/e2e/down.sh
```

Or stop them without deleting:

```sh
./test/e2e/down.sh stop
```
