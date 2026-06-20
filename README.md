# ZFS Replication Controller

MVP1 is a Kubernetes-native ZFS replication controller for one explicit source dataset on one node to one explicit target dataset on another node.

Replication is delegated to pinned upstream `syncoid` 2.3.0 from a privileged per-run sender Job. The target side is a short-lived privileged SSH receiver Job, so `syncoid` can use its normal remote ZFS workflow without relying on node SSH accounts.

## What It Does

- Watches `ZFSReplication` objects.
- Creates a per-run SSH `Secret`.
- Starts a privileged receiver `Job` on the target node that accepts only the per-run key.
- Starts a privileged sender `Job` on the source node after the receiver pod is ready.
- Creates the run snapshot if it does not already exist.
- Runs `syncoid --no-sync-snap` against `root@<receiver-pod-ip>:targetDataset` with include filters for the run snapshot and, on incremental runs, the previous successful snapshot.
- Updates basic status after the sender Job succeeds.

MVP1 does not create a Kubernetes `Service` or use long-lived node SSH credentials. The per-run SSH Secret and receiver Job are removed after the run finishes.

## Install

Build and push an image that includes `zfsutils-linux`, pinned upstream `syncoid` 2.3.0, and the controller binaries:

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

While a run is active, status includes the sender and receiver objects:

```yaml
status:
  phase: Running
  receiverJobName: zfsrep-pg-a-to-b-manual-0002-receiver
  receiverPodName: zfsrep-pg-a-to-b-manual-0002-receiver-k9r5w
  receiverPodIP: 10.42.3.12
  senderJobName: zfsrep-pg-a-to-b-manual-0002-sender
  sshSecretName: zfsrep-pg-a-to-b-manual-0002-ssh
```

## Operational Warnings

The target dataset must be passive and suitable for `syncoid` to receive into.

`DestroyTargetAndReceiveFull` is destructive. When the sender must perform a full replication and this mode is enabled, it passes `--force-delete` to `syncoid`.

The controller does not discover PVCs, CSI snapshots, ZFS snapshots, or retention state. Dataset names and node names are explicit user input.

Sender and receiver Jobs are pinned with `spec.template.spec.nodeName`, not only a node selector. Each container verifies at startup that the actual node from the downward API matches the expected node and exits before running ZFS or SSH commands if it does not.

Jobs use `backoffLimit: 0` and `restartPolicy: Never`. Retry by changing `spec.runID`.

## Development

```sh
go fmt ./...
go test ./...
go vet ./...
```
