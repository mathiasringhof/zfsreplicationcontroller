# Mirror Syncoid process outcome in Replication Run phase

A Replication Run reports `Succeeded` when Syncoid exits successfully and `Failed` when Syncoid exits unsuccessfully. Warning-level Syncoid output, including a target snapshot deletion failure that Syncoid treats as nonfatal, remains operator evidence and does not alter the run phase. This preserves Syncoid option semantics and avoids interpreting human-oriented output or independently verifying Syncoid's post-transfer work; operators must inspect sender output for warnings.
