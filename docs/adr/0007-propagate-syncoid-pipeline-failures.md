# Propagate all Syncoid transfer-stage failures

A Replication Run reports `Succeeded` only when the patched Syncoid process
exits successfully after every transfer-critical stage in the
controller-generated Replication Source-to-Receiver pipeline reports success.
The release image applies a repository-owned patch to the checksum-pinned
Syncoid archive and runs its constructed transfer command with Bash `pipefail`
semantics. This makes failures from source `zfs send`, local transport helpers,
SSH, and the Receiver pipeline visible through Syncoid's ordinary process exit
status.

The Receiver continues to execute each authorized remote pipeline process and
returns a nonzero SSH result when any Receiver stage fails. The Sender remains
a transparent Syncoid operator: it forwards standard output and standard error
unchanged, preserves the final process exit code, and publishes only the
generic Sender Failure Message. It does not parse Syncoid output, add a result
protocol, or perform post-run ZFS queries.

Warning-level obsolete sync-snapshot pruning, optional target-snapshot cleanup,
and hold-release failures remain nonfatal. A no-op operation remains successful
when Syncoid finds no newer snapshot to transfer. This decision changes neither
the public API and CRD contracts nor the Receiver Authorization policy.

## Consequences

- Bash is an explicit release-image dependency, and patch application fails the
  image build when the pinned Syncoid source no longer matches.
- Full, incremental, bookmark, filtered, and resumed transfers inherit the
  behavior through Syncoid's common transfer-command seam.
- Detailed transfer-stage evidence remains in ordinary Sender logs while Run
  status retains its bounded generic failure surface.
- The local patch remains until a pinned upstream Syncoid release provides
  equivalent all-stage failure propagation and passes the same deterministic
  regression without the patch.
