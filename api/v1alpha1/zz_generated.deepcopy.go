package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

func (in *ZFSReplication) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

func (in *ZFSReplicationList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationList)
	*out = *in
	if in.Items != nil {
		out.Items = make([]ZFSReplication, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *ZFSReplication) DeepCopy() *ZFSReplication {
	if in == nil {
		return nil
	}
	out := new(ZFSReplication)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	if in.Status.StartedAt != nil {
		out.Status.StartedAt = in.Status.StartedAt.DeepCopy()
	}
	if in.Status.CompletedAt != nil {
		out.Status.CompletedAt = in.Status.CompletedAt.DeepCopy()
	}
	return out
}
