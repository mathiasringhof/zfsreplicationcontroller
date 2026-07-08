package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

func (in *ZFSReplicationRun) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

func (in *ZFSReplicationRunList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationRunList)
	*out = *in
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]ZFSReplicationRun, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *ZFSReceiveTask) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

func (in *ZFSReceiveTaskList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ZFSReceiveTaskList)
	*out = *in
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]ZFSReceiveTask, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *ZFSReplicationSchedule) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

func (in *ZFSReplicationScheduleList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationScheduleList)
	*out = *in
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]ZFSReplicationSchedule, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *ZFSReplicationRun) DeepCopy() *ZFSReplicationRun {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationRun)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = *in.Spec.DeepCopy()
	if in.Status.StartedAt != nil {
		out.Status.StartedAt = in.Status.StartedAt.DeepCopy()
	}
	if in.Status.CompletedAt != nil {
		out.Status.CompletedAt = in.Status.CompletedAt.DeepCopy()
	}
	return out
}

func (in *ZFSReceiveTask) DeepCopy() *ZFSReceiveTask {
	if in == nil {
		return nil
	}
	out := new(ZFSReceiveTask)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = *in.Spec.DeepCopy()
	return out
}

func (in *ZFSReplicationSchedule) DeepCopy() *ZFSReplicationSchedule {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationSchedule)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	if in.Spec.Suspend != nil {
		out.Spec.Suspend = new(bool)
		*out.Spec.Suspend = *in.Spec.Suspend
	}
	if in.Spec.SuccessfulRunsHistoryLimit != nil {
		out.Spec.SuccessfulRunsHistoryLimit = new(int32)
		*out.Spec.SuccessfulRunsHistoryLimit = *in.Spec.SuccessfulRunsHistoryLimit
	}
	if in.Spec.FailedRunsHistoryLimit != nil {
		out.Spec.FailedRunsHistoryLimit = new(int32)
		*out.Spec.FailedRunsHistoryLimit = *in.Spec.FailedRunsHistoryLimit
	}
	out.Spec.RunTemplate = *in.Spec.RunTemplate.DeepCopy()
	if in.Status.LastScheduleTime != nil {
		out.Status.LastScheduleTime = in.Status.LastScheduleTime.DeepCopy()
	}
	return out
}

func (in *ZFSReplicationRunSpec) DeepCopy() *ZFSReplicationRunSpec {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationRunSpec)
	*out = *in
	out.Syncoid = *in.Syncoid.DeepCopy()
	return out
}

func (in *ZFSReceiveTaskSpec) DeepCopy() *ZFSReceiveTaskSpec {
	if in == nil {
		return nil
	}
	out := new(ZFSReceiveTaskSpec)
	*out = *in
	out.SSH.ExpiresAt = *in.SSH.ExpiresAt.DeepCopy()
	return out
}

func (in *SyncoidSpec) DeepCopy() *SyncoidSpec {
	if in == nil {
		return nil
	}
	out := new(SyncoidSpec)
	*out = *in
	if in.NoSyncSnap != nil {
		out.NoSyncSnap = new(bool)
		*out.NoSyncSnap = *in.NoSyncSnap
	}
	if in.NoRollback != nil {
		out.NoRollback = new(bool)
		*out.NoRollback = *in.NoRollback
	}
	if in.ForceDelete != nil {
		out.ForceDelete = new(bool)
		*out.ForceDelete = *in.ForceDelete
	}
	if in.ReceiveUnmounted != nil {
		out.ReceiveUnmounted = new(bool)
		*out.ReceiveUnmounted = *in.ReceiveUnmounted
	}
	if in.ReceiveResumable != nil {
		out.ReceiveResumable = new(bool)
		*out.ReceiveResumable = *in.ReceiveResumable
	}
	if in.IncludeSnaps != nil {
		out.IncludeSnaps = append([]string(nil), in.IncludeSnaps...)
	}
	if in.ExcludeSnaps != nil {
		out.ExcludeSnaps = append([]string(nil), in.ExcludeSnaps...)
	}
	return out
}
