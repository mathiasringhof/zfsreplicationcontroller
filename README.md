# ZFS Replication Controller

MVP1 is a Kubernetes-native ZFS replication controller for one explicit source dataset on one node to one explicit target dataset on another node.

The transport is in-cluster pod-to-pod streaming only. It does not use SSH and has no SSH option.

## What It Does

- Watches `ZFSReplication` objects.
- Creates a per-run bearer token `Secret`.
- Starts a privileged receiver `Job` on the target node.
- Exposes the receiver with a per-run `ClusterIP` `Service`.
- Starts a privileged sender `Job` on the source node after the receiver pod is ready.
- Runs `zfs snapshot`, `zfs send`, and authenticated HTTP streaming to `zfs receive -u -s`.
- Updates basic status and cleans up the temporary `Service` and token `Secret` after success.

## Install

Build and push an image that includes `zfsutils-linux` and the three binaries:

```sh
docker build -t registry.example.com/zfsreplicationcontroller:latest .
docker push registry.example.com/zfsreplicationcontroller:latest
```

Set that image in `config/manager/deployment.yaml`, then install:

```sh
kubectl apply -k config
```

## Example

```yaml
apiVersion: zfsreplication.example.com/v1alpha1
kind: ZFSReplication
metadata:
  name: pg-a-to-b
  namespace: storage
spec:
  runID: manual-0001
  source:
    nodeName: worker-a
    dataset: tank/pvc-source
  target:
    nodeName: worker-b
    dataset: tank/pvc-target
  snapshotPrefix: zsync
  bootstrap:
    mode: FailIfNoBase
  receive:
    receiveUnmounted: true
    resumable: true
```

Trigger a new run by changing `spec.runID`:

```sh
kubectl patch zfsreplication pg-a-to-b -n storage --type merge -p '{"spec":{"runID":"manual-0002"}}'
```

Inspect status:

```sh
kubectl get zfsreplication pg-a-to-b -n storage -o yaml
kubectl get jobs -n storage -l zfsreplication.example.com/name=pg-a-to-b
```

## Operational Warnings

The target dataset must be passive and unmounted. The receiver refuses to run if `zfs get -H -o value mounted <target>` returns `yes`.

`DestroyTargetAndReceiveFull` is destructive. When the sender must perform a full send and this mode is enabled, the receiver may run `zfs destroy -r <target dataset>` before `zfs receive`.

The controller does not discover PVCs, CSI snapshots, ZFS snapshots, or retention state. Dataset names and node names are explicit user input.

## Development

```sh
go fmt ./...
go test ./...
go vet ./...
```
